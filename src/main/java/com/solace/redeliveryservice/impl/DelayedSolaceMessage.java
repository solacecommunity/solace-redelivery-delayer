package com.solace.redeliveryservice.impl;

import com.solace.messaging.receiver.InboundMessage;

import java.util.Objects;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * This class encapsulates an Inbound Solace Message with an appropriate delay to be picked up by the
 * @see SolaceQueueMessageRedeliveryEngine
 * @author TKTheTechie
 */
public class DelayedSolaceMessage implements Delayed {

    private long startTime;

    private InboundMessage message;

    public DelayedSolaceMessage(InboundMessage message, long delayInMillis){
        this.message   = message;
        this.startTime = System.currentTimeMillis() + delayInMillis;
    }

    @Override
    public long getDelay(TimeUnit unit) {
        long diff = this.startTime - System.currentTimeMillis();
        return unit.convert(diff, TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed o) {
         return Math.toIntExact(this.startTime - ((DelayedSolaceMessage) o).startTime);
    }

    public InboundMessage getMessage() {
        return message;
    }

    /**
     * Convenience function for returning an empty object to prevent NPEs
     * @return empty DelayedSolaceMessageObject
     */
    public static DelayedSolaceMessage createEmptyDelayedSolaceMessage(){
        return new DelayedSolaceMessage(null, 0L);
    }

    /**
     * Check whether the DelayedSolaceMessage is empty
     * @return whether the DelayedSolaceMessage Object is empty
     */
    public boolean isEmptyMessage(){
        return this.message == null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DelayedSolaceMessage that = (DelayedSolaceMessage) o;
        return startTime == that.startTime && Objects.equals(message, that.message);
    }

    @Override
    public int hashCode() {
        return Objects.hash(startTime, message);
    }
    
}