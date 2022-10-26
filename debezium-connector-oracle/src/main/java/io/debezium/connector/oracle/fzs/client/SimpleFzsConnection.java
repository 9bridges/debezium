/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.fzs.client;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import io.debezium.connector.oracle.fzs.client.procuder.DatabaseInfo;
import io.debezium.connector.oracle.fzs.client.procuder.LCRProducer;

import oracle.streams.LCR;

public class SimpleFzsConnection extends FzsConnection {
    private final AtomicBoolean running = new AtomicBoolean(false);

    public SimpleFzsConnection(String ip, String port, BlockingQueue<LCR> outQueue) {
        super(ip, port, outQueue);
    }

    @Override
    public void stop() {
        running.compareAndSet(false, true);
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public void run() {
        if (running.compareAndSet(false, true)) {
            // just test product LCR for FzsAdapter
            DatabaseInfo databaseInfo = new DatabaseInfo("debug", "xstrm", "password", "81.70.133.193:1521/jqdb1", "0");
            LCRProducer xstream = new LCRProducer(databaseInfo, outQueue);
            xstream.start();
        }
    }
}
