package com.solace.redeliveryservice.api;

import com.solace.messaging.MessagingService;
import com.solace.messaging.publisher.OutboundMessageBuilder;
import com.solace.messaging.publisher.PersistentMessagePublisher;
import com.solace.messaging.receiver.PersistentMessageReceiver;
import com.solace.messaging.resources.Queue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

/**
 * Convenience class that encapsulates Solace objects for publishing, receiving and building messages
 */
public abstract class ISolaceMessagingService {

    private static final Logger log = LoggerFactory.getLogger(ISolaceMessagingService.class);

    protected MessagingService solaceMessagingService;

    protected PersistentMessageReceiver dmqReceiver;

    protected PersistentMessagePublisher publisher;

    protected OutboundMessageBuilder messageBuilder;

    @Value("${solace.redelivery.dmq.name}")
    private String SOLACE_DMQ_NAME;


    public void init(){
        log.info("Starting consumer on {}",SOLACE_DMQ_NAME);
        dmqReceiver  = solaceMessagingService.createPersistentMessageReceiverBuilder().build(Queue.durableExclusiveQueue(SOLACE_DMQ_NAME)).start();
        publisher = solaceMessagingService.createPersistentMessagePublisherBuilder().build().start();
        messageBuilder = solaceMessagingService.messageBuilder();
    }

    public MessagingService getSolaceMessagingService(){
        return this.getSolaceMessagingService();
    }


    public PersistentMessageReceiver getDmqReceiver() {
        return dmqReceiver;
    }


    public PersistentMessagePublisher getPublisher() {
        return publisher;
    }

    public OutboundMessageBuilder getMessageBuilder() {return messageBuilder;}



}
