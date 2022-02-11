package com.solace.redeliveryservice.api;

import com.solace.messaging.resources.Topic;

/**
 * <h1>Queue</h1>
 * An interface for queue.  This is an extension of {@link com.solace.messaging.resources.Topic}.
 * <p>
 * <b>Note:</b> The Solace Java API version 1.0.0 used in this codebase does not support publishing (writing) directly into a durable queue.
 * However, each queue has a predefined topic associated with it. This will be fixed in a future release.
 * 
 * @author TheChemerson
 */
public interface Queue extends Topic {

    // prefix of the reserved topic for directly publishing into a durable queue.
    static final String DURABLE_QUEUE_TOPIC_PREFIX = "#P2P/QUE/";

    /**
     * A factory method to create an instance of {@link Queue} for the specified queue name.
     * @param queueName - the durable queue name, this cannot be <code>null</code>.
     * @return An instance of a {@link Queue} implementation.
     */

     public static Topic of (String queueName) {
        return Topic.of(DURABLE_QUEUE_TOPIC_PREFIX + queueName, false);
    }

}