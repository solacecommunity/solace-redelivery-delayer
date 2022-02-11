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
 * @author TKTheTechie
 */
public abstract class SolaceMessagingService {

    private static final Logger log = LoggerFactory.getLogger(SolaceMessagingService.class);

    protected MessagingService solaceMessagingService;

    protected PersistentMessageReceiver dmqReceiver;

    protected PersistentMessagePublisher publisher;

    protected OutboundMessageBuilder messageBuilder;

    @Value("${solace.redelivery.dmq.name}")
    private String SOLACE_DMQ_NAME;

    public void init(){
        if (log.isInfoEnabled()) {
            log.info("Start consuming from {}...", SOLACE_DMQ_NAME);
        }

        dmqReceiver    = solaceMessagingService.createPersistentMessageReceiverBuilder().build(Queue.durableExclusiveQueue(SOLACE_DMQ_NAME)).start();
        publisher      = solaceMessagingService.createPersistentMessagePublisherBuilder().build().start();
        messageBuilder = solaceMessagingService.messageBuilder();
    }

    public MessagingService getSolaceMessagingService(){
        return this.getSolaceMessagingService();
    }

    /**
     * Returns DMQ message receiver.
     * 
     * @return - DMQ message receiver.
     */
    public PersistentMessageReceiver getDmqReceiver() {
        return dmqReceiver;
    }

    /**
     * Returns the publisher instance for sending the delay message back to the source queue.
     * 
     * @return - message publisher.
     */
    public PersistentMessagePublisher getPublisher() {
        return publisher;
    }

    /**
     * Returns the message builder instance.
     * 
     * @return - message builder.
     */
    public OutboundMessageBuilder getMessageBuilder() {
        return messageBuilder;
    }

}