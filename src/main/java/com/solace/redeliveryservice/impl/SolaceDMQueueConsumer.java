package com.solace.redeliveryservice.impl;

import com.solace.messaging.publisher.OutboundMessage;
import com.solace.messaging.receiver.InboundMessage;
import com.solace.messaging.resources.Topic;
import com.solace.redeliveryservice.api.IRedeliveryEngine;
import com.solace.redeliveryservice.api.SolaceMessagingService;
import com.solace.redeliveryservice.api.WriteableQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Properties;

/**
 * Class which consumes a message from a pre-configured queue
 * @author TKTheTechie
 */
@Component
public class SolaceDMQueueConsumer {

    private static final Logger log = LoggerFactory.getLogger(SolaceDMQueueConsumer.class);

    @Autowired
    private SolaceMessagingService solaceMessagingService;

    @Autowired
    private IRedeliveryEngine redeliveryEngine;

    @Value("${solace.redelivery.delayInMs}")
    private long REDELIVERY_DELAY;

    @Value("${solace.redelivery.maximum.delayInMs}")
    private long MAXIMUM_REDELIVERY_DELAY;
    
    @Value("${solace.redelivery.exponential.backoff.factor}")
    private long EXPONENTIAL_BACK_OFF_FACTOR;

    @Value("${solace.redelivery.custom.redelivery.header:sol_rx_delivery_count}")
    private String REDELIVERY_HEADER_NAME;

    @Value("${solace.redelivery.error.queue:#{null}}")
    private String ERROR_QUEUE_NAME;

    private Topic ERROR_QUEUE;


    /**
     * Tight loop to receive messages from the queue
     */
    @PostConstruct
    public void init() {
        ERROR_QUEUE = WriteableQueue.of(ERROR_QUEUE_NAME);
        solaceMessagingService.getDmqReceiver().receiveAsync(this::processMessage);
    }

    public void processMessage(InboundMessage inboundMessage) {
        int redelivery_count = 0;

        // If the REDELIVERY_HEADER_NAME header exists, then we attempt to increment it
        if (inboundMessage.getProperty(REDELIVERY_HEADER_NAME) != null) {
            try {
                redelivery_count = Integer.parseInt(inboundMessage.getProperty(REDELIVERY_HEADER_NAME));
            } catch (NumberFormatException ex) {
                if (log.isErrorEnabled()) {
                    log.error("Received invalid redelivery count on header {}. Ignoring it.", REDELIVERY_HEADER_NAME);
                }
            }
        }

        // The redelivery engine's internal queue is at capacity... wait until it is free
        // Note: This is a workaround because Java's current implementation of the DelayQueue is that it is unbounded
        while (!redeliveryEngine.canAcceptTask()) {
            if (log.isWarnEnabled()) {
                log.warn("Redelivery engine's internal queue is at capacity. Waiting 1 second...");
            }

            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                if (log.isErrorEnabled()) {
                    log.error("Sleep has been interrupted");
                }
            }
        }

        // The delay time is a product of the configured redelivery day with a multiple of the number of times the
        // message has been redelivered and the back off factor
        long nextDelay = getNextDelay(redelivery_count);

        //If within the threshold, submit for redelivery back to the source queue
        if (nextDelay <= MAXIMUM_REDELIVERY_DELAY) {
            if (log.isInfoEnabled()) {
                log.info("Submitting a message to the redelivery engine with {} ms delay...", String.format("%,d", nextDelay));
            }

            DelayedSolaceMessage delayedSolaceMessage = new DelayedSolaceMessage(inboundMessage, nextDelay);
            redeliveryEngine.submitTask(delayedSolaceMessage);
        } else {
            // If the calculated delay time exceeds the max allowed redelivery, then send to the error queue (if it exists),
            // or to the ether if it doesn't
            if ((ERROR_QUEUE_NAME == null) || (ERROR_QUEUE_NAME.isEmpty())) {
                if (log.isWarnEnabled()) {
                    log.warn("Message has exceeded redelivery thresholds and has disappeared into the ether!");
                }
            } else {
                if (log.isWarnEnabled()) {
                    log.warn("Message exceeded redelivery thresholds - sending to the error queue - {}", ERROR_QUEUE_NAME);
                }

                Properties properties = new Properties();
                properties.putAll(inboundMessage.getProperties());
                OutboundMessage outboundMessage = solaceMessagingService.getMessageBuilder().build(inboundMessage.getPayloadAsBytes());
                try {
                    this.solaceMessagingService.getPublisher().publishAwaitAcknowledgement(outboundMessage, ERROR_QUEUE, 10000L);
                } catch (InterruptedException e) {
                    if (log.isErrorEnabled()) {
                        log.error("Unable to send a message to the error queue");
                    }
                } finally {
                    this.solaceMessagingService.getDmqReceiver().ack(inboundMessage);
                }
            }
        }
    }

    /**
     * Get the next delay.
     * @param redelivery_count - Current redelivery count.
     * @return next delay (in milliseconds).
     */
    long getNextDelay(int redelivery_count) {
        return REDELIVERY_DELAY * ((long) Math.pow(EXPONENTIAL_BACK_OFF_FACTOR, redelivery_count));
    }
    
}