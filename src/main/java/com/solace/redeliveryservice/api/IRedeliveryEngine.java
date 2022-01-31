package com.solace.redeliveryservice.api;

import java.util.concurrent.Delayed;

public interface IRedeliveryEngine<T extends Delayed> {

    public void submitTask(T task);

    /**
     * Whether the engine can accept a task for processing
     * @return
     */
    public boolean canAcceptTask();

    public void executeTask(T task);

}
