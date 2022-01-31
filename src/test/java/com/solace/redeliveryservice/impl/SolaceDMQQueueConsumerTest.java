package com.solace.redeliveryservice.impl;

import com.solace.messaging.MessagingService;
import com.solace.messaging.publisher.OutboundMessage;
import com.solace.messaging.publisher.OutboundMessageBuilder;
import com.solace.messaging.publisher.PersistentMessagePublisher;
import com.solace.messaging.receiver.InboundMessage;
import com.solace.messaging.receiver.MessageReceiver;
import com.solace.messaging.receiver.PersistentMessageReceiver;
import com.solace.messaging.resources.Topic;
import com.solace.redeliveryservice.api.IRedeliveryEngine;
import com.solace.redeliveryservice.api.ISolaceMessagingService;
import org.junit.jupiter.api.*;
import org.mockito.*;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.concurrent.Delayed;

import static org.mockito.Mockito.*;


public class SolaceDMQQueueConsumerTest {

    @Mock
    ISolaceMessagingService solaceMessagingService;
    @Mock
    IRedeliveryEngine redeliveryEngine;

    MessagingService messagingService;
    PersistentMessagePublisher messagePublisher;
    PersistentMessageReceiver messageReceiver;

    private final static String REDELIVERY_HEADER_NAME="sol_rx_count";

    AutoCloseable closeable;

    @InjectMocks
    @Spy
    SolaceDMQueueConsumer dmqQueueConsumer;

    @BeforeEach
    void setup(){
        closeable = MockitoAnnotations.openMocks(this);
        ReflectionTestUtils.setField(dmqQueueConsumer,"REDELIVERY_HEADER_NAME",REDELIVERY_HEADER_NAME);
        messagingService = Mockito.mock(MessagingService.class);
        messagePublisher = Mockito.mock(PersistentMessagePublisher.class);
        messageReceiver = Mockito.mock(PersistentMessageReceiver.class);
        OutboundMessageBuilder outboundMessageBuilder = Mockito.mock(OutboundMessageBuilder.class);

        when(redeliveryEngine.canAcceptTask()).thenReturn(true);
        when(solaceMessagingService.getSolaceMessagingService()).thenReturn(messagingService);
        when(solaceMessagingService.getMessageBuilder()).thenReturn(outboundMessageBuilder);
        when(solaceMessagingService.getPublisher()).thenReturn(messagePublisher);
        when(solaceMessagingService.getDmqReceiver()).thenReturn(messageReceiver);

        when(solaceMessagingService.getMessageBuilder().build(any(byte[].class))).thenReturn(Mockito.mock(OutboundMessage.class));

    }

    @AfterEach
    void closeService() throws Exception {
        closeable.close();
    }

    private void injectDelayParams(long REDELIVERY_DELAY, long EXPONENTIAL_BACK_OFF_FACTOR, long MAXIMUM_REDELIVERY_DELAY){
        ReflectionTestUtils.setField(dmqQueueConsumer,"REDELIVERY_DELAY",REDELIVERY_DELAY);
        ReflectionTestUtils.setField(dmqQueueConsumer,"EXPONENTIAL_BACK_OFF_FACTOR",EXPONENTIAL_BACK_OFF_FACTOR);
        ReflectionTestUtils.setField(dmqQueueConsumer,"MAXIMUM_REDELIVERY_DELAY",MAXIMUM_REDELIVERY_DELAY);
    }

    @DisplayName("New Message Test - should not goto the error queue")
    @Test
    public void testNewMessage() throws InterruptedException {
        ReflectionTestUtils.setField(dmqQueueConsumer,"ERROR_QUEUE_NAME","ERROR_QUEUE");
        injectDelayParams(1000L, 5000L, 10000L);
        InboundMessage message = Mockito.mock(InboundMessage.class);
        when(message.getProperty("sol_rx_count")).thenReturn(null);
        when(message.getPayloadAsBytes()).thenReturn("HELLO WORLD".getBytes());
        dmqQueueConsumer.processMessage(message);
        verify(dmqQueueConsumer,times(1)).calculatedDelayTime(0);
        verify(redeliveryEngine, times(1)).submitTask(any(DelayedSolaceMessage.class));
        verify(messagePublisher,times(0)).publishAwaitAcknowledgement(any(OutboundMessage.class),any(Topic.class),anyLong());
    }

    @DisplayName("Expired Message Test with ERROR-QUEUE defined - should publish to the ERROR QUEUE")
    @Test
    public void testExpiredMessageErrorQueue() throws InterruptedException {
        ReflectionTestUtils.setField(dmqQueueConsumer,"ERROR_QUEUE_NAME","ERROR_QUEUE");
        ReflectionTestUtils.setField(dmqQueueConsumer,"ERROR_QUEUE",Topic.of("#P2P/QUE/ERROR_QUEUE"));
        injectDelayParams(1000L, 5000L, 10000L);
        InboundMessage message = Mockito.mock(InboundMessage.class);
        when(message.getProperty("sol_rx_count")).thenReturn("2");
        when(message.getPayloadAsBytes()).thenReturn("HELLO WORLD".getBytes());
        dmqQueueConsumer.processMessage(message);
        verify(dmqQueueConsumer,times(1)).calculatedDelayTime(2);
        verify(redeliveryEngine, times(0)).submitTask(any(Delayed.class));
        verify(messagePublisher,times(1)).publishAwaitAcknowledgement(any(OutboundMessage.class),eq(Topic.of("#P2P/QUE/ERROR_QUEUE")),anyLong());
        verify(messageReceiver,times(1)).ack(any(InboundMessage.class));
    }

    @DisplayName("Expired Message Test with no Error Queue - should goto the ether")
    @Test
    public void testExpiredMessageNoErrorQueue() throws InterruptedException {
        injectDelayParams(1000L, 5000L, 10000L);
        InboundMessage message = Mockito.mock(InboundMessage.class);
        when(message.getProperty("sol_rx_count")).thenReturn("2");
        when(message.getPayloadAsBytes()).thenReturn("HELLO WORLD".getBytes());
        dmqQueueConsumer.processMessage(message);
        verify(dmqQueueConsumer,times(1)).calculatedDelayTime(2);
        verify(redeliveryEngine, times(0)).submitTask(any(Delayed.class));
        verify(messagePublisher,times(0)).publishAwaitAcknowledgement(any(OutboundMessage.class),any(Topic.class),anyLong());
    }

}
