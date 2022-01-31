package com.solace.redeliveryservice.impl;

import com.solace.messaging.publisher.OutboundMessage;
import com.solace.messaging.receiver.InboundMessage;
import com.solace.messaging.resources.Topic;
import com.solace.redeliveryservice.api.IRedeliveryEngine;
import com.solace.redeliveryservice.api.ISolaceMessagingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Properties;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

/**
 * This class iterates over a delay queue and pushes expired messages into a SolaceQueue
 * @author TKTheTechie
 */
@Component
public class SolaceQueueMessageRedeliveryEngine implements IRedeliveryEngine<DelayedSolaceMessage> {

    private static final Logger log = LoggerFactory.getLogger(SolaceQueueMessageRedeliveryEngine.class);

    @Value("${solace.redelivery.engine.queue.capacity:1000}")
    private int QUEUE_CAPACITY;

    @Value("${solace.redelivery.custom.redelivery.header:sol_rx_delivery_count}")
    private String REDELIVERY_HEADER_NAME;

    @Value("${solace.redelivery.source.queue.name}")
    private String SOURCE_SOLACE_QUEUE_NAME;

    private Topic SOURCE_SOLACE_QUEUE;

    private DelayQueue<DelayedSolaceMessage> delayQueue = new DelayQueue<DelayedSolaceMessage>();

    @Autowired
    private ISolaceMessagingService solaceMessagingService;

    //Instantiates the publisher and starts the DelayQueue Stream Processor
    @PostConstruct
    public void init() {


        //Using the Queue's Topic here
        SOURCE_SOLACE_QUEUE = Topic.of("#P2P/QUE/" + SOURCE_SOLACE_QUEUE_NAME);
        ExecutorService executorService = Executors.newSingleThreadExecutor();

        executorService.submit(()-> {
            // Code below will start an infinite Java Stream that is constantly iterating over the DelayQueue
            // that releases expired messages in parallel to increase throughput
            Stream.generate(() -> {
                try {
                    return delayQueue.take();
                } catch (InterruptedException e) {
                    log.error("Unable to process delayQueue", e);
                    //If the thread gets interrupted, then it will return an empty DelayedSolaceMessage
                    return DelayedSolaceMessage.createEmptyDelayedSolaceMessage();
                }
            }).parallel().forEach(d -> {
                //If the stream was not interrupted, then execute the task
                if (!d.isEmptyMessage())
                {
                    try {
                    executeTask(d);
                    } catch (Exception ex){
                        log.error("FATAL ERROR PROCESSING A TASK:", ex);
                    }
                }
            });
        });


    }

    /**
     * Function t o add the task to the DelayQueue
     * @param task
     */
    @Override
    public void submitTask(DelayedSolaceMessage task) {
        delayQueue.add(task);
    }


    /**
     * This method needs to be implemented because the DelayQueue is unbounded, this is to protect overflow of memory
     *
     * @return
     */
    @Override
    public boolean canAcceptTask() {
        return delayQueue.size() < QUEUE_CAPACITY;
    }

    /**
     * Gets the expired message and sends it back to the source queue
     * @param task
     */
    public void executeTask(DelayedSolaceMessage task) {
        InboundMessage inboundMessage = task.getMessage();

        Properties messageProperties = new Properties();
        messageProperties.putAll(inboundMessage.getProperties());

        int rxCount = 1;

        //If the message has a redelivery header, then increment it by 1
        if (messageProperties.containsKey(REDELIVERY_HEADER_NAME)) {
            try {
                rxCount = Integer.parseInt(messageProperties.getProperty(REDELIVERY_HEADER_NAME)) + 1;
            } catch (NumberFormatException ex) {
                log.error("Received invalid redelivery count on header {}. Resetting the redelivery counter...", REDELIVERY_HEADER_NAME);
            }
        }

        messageProperties.setProperty(REDELIVERY_HEADER_NAME, String.valueOf(rxCount));
       try {
           OutboundMessage message = solaceMessagingService.getMessageBuilder().build(inboundMessage.getPayloadAsBytes(), messageProperties);
            log.info("Redelivering a message..");
            this.solaceMessagingService.getPublisher().publishAwaitAcknowledgement(message, SOURCE_SOLACE_QUEUE, 20000L);
        } catch (InterruptedException e) {
           log.error("Unable to publish back to the source {} : {}", SOURCE_SOLACE_QUEUE, e);
       }

        //Finally ack the inbound message
        this.solaceMessagingService.getDmqReceiver().ack(inboundMessage);
    }


}
