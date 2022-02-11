package com.solace.redeliveryservice.impl;

import com.solace.messaging.MessagingService;
import com.solace.messaging.publisher.OutboundMessage;
import com.solace.messaging.publisher.OutboundMessageBuilder;
import com.solace.messaging.publisher.PersistentMessagePublisher;
import com.solace.messaging.receiver.InboundMessage;
import com.solace.messaging.receiver.PersistentMessageReceiver;
import com.solace.messaging.resources.Topic;
import com.solace.redeliveryservice.api.SolaceMessagingService;
import com.solace.redeliveryservice.api.Queue;

import org.junit.jupiter.api.*;
import org.mockito.*;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.mockito.Mockito.*;

public class SolaceQueueMessageRedeliveryEngineTest {

    @Mock
    SolaceMessagingService solaceMessagingService;

    MessagingService messagingService;
    OutboundMessageBuilder outboundMessageBuilder;
    InboundMessage inboundMessage;

    PersistentMessagePublisher messagePublisher;
    PersistentMessageReceiver dmqReceiver;

    @InjectMocks
    @Spy
    SolaceQueueMessageRedeliveryEngine sqmrde;

    AutoCloseable closeable;

    @BeforeEach
     void setup(){
        closeable = MockitoAnnotations.openMocks(this);
        ReflectionTestUtils.setField(sqmrde, "REDELIVERY_HEADER_NAME", "sol_rx_delivery_count");
        ReflectionTestUtils.setField(sqmrde, "SOURCE_SOLACE_QUEUE_NAME", "SOURCE_QUEUE");
        ReflectionTestUtils.setField(sqmrde, "SOURCE_SOLACE_QUEUE", Queue.of("SOURCE_QUEUE"));

        inboundMessage         = Mockito.mock(InboundMessage.class);
        outboundMessageBuilder = Mockito.mock(OutboundMessageBuilder.class);
        messagePublisher       = Mockito.mock(PersistentMessagePublisher.class);
        messagingService       = Mockito.mock(MessagingService.class);
        dmqReceiver            = Mockito.mock(PersistentMessageReceiver.class);

        when(solaceMessagingService.getSolaceMessagingService()).thenReturn(messagingService);
        when(solaceMessagingService.getPublisher()).thenReturn(messagePublisher);
        when(solaceMessagingService.getDmqReceiver()).thenReturn(dmqReceiver);
        when(inboundMessage.getPayloadAsBytes()).thenReturn("HELLO WORLD".getBytes());
        when(solaceMessagingService.getMessageBuilder()).thenReturn(outboundMessageBuilder);
    }

    @AfterEach
    void closeService() throws Exception {
        closeable.close();
    }

    @DisplayName("Send fresh message for redelivery")
    @Test
    void testMessageBeingSentToQueue() throws InterruptedException {
        DelayedSolaceMessage delayedSolaceMessage  = new DelayedSolaceMessage(inboundMessage,1L);
        Map<String, String> propertyMap = new HashMap<>();
        propertyMap.put("HEADER1", "1");

        when(inboundMessage.getProperties()).thenReturn(propertyMap);
        when(inboundMessage.getPayloadAsBytes()).thenReturn("HELLO_WORLD".getBytes());

        Properties properties = new Properties();
        properties.setProperty("sol_rx_delivery_count", "1");
        properties.setProperty("HEADER1", "1");

        sqmrde.executeTask(delayedSolaceMessage);
        verify(outboundMessageBuilder, times(1)).build(any(byte[].class), eq(properties));
        verify(messagePublisher, times(0)).publishAwaitAcknowledgement(any(OutboundMessage.class), any(Topic.class), anyLong());
    }

}