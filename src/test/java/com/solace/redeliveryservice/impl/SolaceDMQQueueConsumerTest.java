package com.solace.redeliveryservice.impl;

import com.solace.messaging.MessagingService;
import com.solace.messaging.publisher.OutboundMessage;
import com.solace.messaging.publisher.OutboundMessageBuilder;
import com.solace.messaging.publisher.PersistentMessagePublisher;
import com.solace.messaging.receiver.InboundMessage;
import com.solace.messaging.receiver.PersistentMessageReceiver;
import com.solace.messaging.resources.Topic;
import com.solace.redeliveryservice.api.IRedeliveryEngine;
import com.solace.redeliveryservice.api.SolaceMessagingService;
import org.junit.jupiter.api.*;
import org.mockito.*;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.concurrent.Delayed;

import static org.mockito.Mockito.*;

public class SolaceDMQQueueConsumerTest {

    @Mock
    SolaceMessagingService solaceMessagingService;
    @Mock
    IRedeliveryEngine redeliveryEngine;

    MessagingService messagingService;
    PersistentMessagePublisher messagePublisher;
    PersistentMessageReceiver messageReceiver;

    private final static String REDELIVERY_HEADER_NAME = "sol_rx_count";

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
        messageReceiver  = Mockito.mock(PersistentMessageReceiver.class);
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

    private void sendTestMessage(String rxCountString) {
        ReflectionTestUtils.setField(dmqQueueConsumer, "REDELIVERY_DELAY", 1000L);
        ReflectionTestUtils.setField(dmqQueueConsumer, "EXPONENTIAL_BACK_OFF_FACTOR", 5000L);
        ReflectionTestUtils.setField(dmqQueueConsumer, "MAXIMUM_REDELIVERY_DELAY", 10000L);

        InboundMessage message = Mockito.mock(InboundMessage.class);
        when(message.getProperty(REDELIVERY_HEADER_NAME)).thenReturn(rxCountString);
        when(message.getPayloadAsBytes()).thenReturn("HELLO WORLD".getBytes());
        dmqQueueConsumer.processMessage(message);
    }

    @DisplayName("New Message Test - should not go to the ERROR_QUEUE")
    @Test
    public void testNewMessage() throws InterruptedException {
        ReflectionTestUtils.setField(dmqQueueConsumer, "ERROR_QUEUE_NAME", "ERROR_QUEUE");

        sendTestMessage(null);

        verify(dmqQueueConsumer,times(1)).getNextDelay(0);
        verify(redeliveryEngine, times(1)).submitTask(any(DelayedSolaceMessage.class));
        verify(messagePublisher, times(0)).publishAwaitAcknowledgement(any(OutboundMessage.class), any(Topic.class), anyLong());
    }

    @DisplayName("Expired Message Test with ERROR_QUEUE defined - should publish to the ERROR_QUEUE")
    @Test
    public void testExpiredMessageErrorQueue() throws InterruptedException {
        final String errorQueueName = "#P2P/QUE/ERROR_QUEUE";

        ReflectionTestUtils.setField(dmqQueueConsumer, "ERROR_QUEUE_NAME", "ERROR_QUEUE");
        ReflectionTestUtils.setField(dmqQueueConsumer, "ERROR_QUEUE", Topic.of(errorQueueName));
        
        sendTestMessage("2");
        
        verify(dmqQueueConsumer, times(1)).getNextDelay(2);
        verify(redeliveryEngine, times(0)).submitTask(any(Delayed.class));
        verify(messagePublisher, times(1)).publishAwaitAcknowledgement(any(OutboundMessage.class), eq(Topic.of(errorQueueName)), anyLong());
        verify(messageReceiver, times(1)).ack(any(InboundMessage.class));
    }

    @DisplayName("Expired Message Test with no Error Queue - should go to the either")
    @Test
    public void testExpiredMessageNoErrorQueue() throws InterruptedException {
        sendTestMessage("2");
        verify(dmqQueueConsumer, times(1)).getNextDelay(2);
        verify(redeliveryEngine, times(0)).submitTask(any(Delayed.class));
        verify(messagePublisher, times(0)).publishAwaitAcknowledgement(any(OutboundMessage.class), any(Topic.class), anyLong());
    }

}