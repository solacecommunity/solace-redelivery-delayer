package com.solace.redeliveryservice.api;

import java.util.concurrent.Delayed;

/**
 * An interface for an engine that should redeliver messages after a given delay.
 * @author TKTheTechie
 */
public interface IRedeliveryEngine<T extends Delayed> {

    /**
     * Add a new task for processing.
     * 
     * @param task - task to be added for processing.
     */
    public void submitTask(T task);

    /**
     * Returns whether the engine can accept a task for processing
     * 
     * @return true for can accept a new task.
     */
    public boolean canAcceptTask();

    /**
     * Process the delayed task.
     * 
     * @param task - the task to process.
     */
    public void executeTask(T task);

}