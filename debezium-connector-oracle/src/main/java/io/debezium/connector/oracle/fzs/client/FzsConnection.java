/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.fzs.client;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import oracle.streams.LCR;

public abstract class FzsConnection implements Runnable {
    protected BlockingQueue<LCR> outQueue;
    protected String ip;
    protected String port;

    public FzsConnection(String ip, String port, BlockingQueue<LCR> outQueue) {
        this.ip = ip;
        this.port = port;
        this.outQueue = outQueue;

    }
    public abstract void stop();

    public abstract boolean isRunning();
}
