package com.solace.redeliveryservice.api;

import com.solace.messaging.resources.Topic;

/**
 * <h1>WriteableQueue</h1>
 * An interface for writeable queue.  This is an extension of {@link com.solace.messaging.resources.Topic}.
 * <p>
 * <b>Note:</b> The current Solace API does not support publishing (writing) directly into a durable queue.
 * However, each queue has a predefined topic associated with it.
 * 
 * @author TheChemerson
 */
public interface WriteableQueue extends Topic {

    // prefix of the reserved topic for directly publishing into a durable queue.
    static final String DURABLE_QUEUE_TOPIC_PREFIX = "#P2P/QUE/";

    /**
     * A factory method to create an instance of {@link WriteableQueue} for the specified queue name.
     * @param queueName - the durable queue name, this cannot be <code>null</code>.
     * @return An instance of a {@link WriteableQueue} implementation.
     */
    public static Topic of (String queueName) {
        return Topic.of(DURABLE_QUEUE_TOPIC_PREFIX + queueName, false);
    }

}