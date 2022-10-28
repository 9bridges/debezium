/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.fzs.client;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import io.debezium.connector.oracle.fzs.entry.FzsEntry;
import io.debezium.connector.oracle.fzs.entry.FzsParser;
import io.debezium.connector.oracle.fzs.entry.SimpleFzsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FzsProducer implements Runnable {

    private final Logger logger = LoggerFactory.getLogger(FzsProducer.class);
    private final FzsConnection fzsConnection;
    private final FzsParser fzsParser;
    private final BlockingQueue<FzsEntry> outQueue;
    private final AtomicBoolean started = new AtomicBoolean(false);

    FzsProducer(String ip, String port, BlockingQueue<FzsEntry> outQueue) {
        fzsConnection = new SimpleFzsConnection();
        fzsConnection.setIpAndPort(ip, port);
        fzsParser = new SimpleFzsParser();
        this.outQueue = outQueue;
    }

    private boolean isRunning() {
        return started.get();
    }

    public void stop() {
        started.compareAndSet(true, false);
        logger.info("FzsProducer begin stop.");
    }

    @Override
    public void run() {
        if (started.compareAndSet(false, true)) {
            logger.info("FzsProducer started.");
            fzsConnection.run();
            while (isRunning()) {
                fzsParser.parser(fzsConnection.poll(), outQueue);
            }
            logger.info("FzsProducer stopped.");
        }
    }
}
