/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.fzs.client;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class SimpleFzsConnection implements FzsConnection {
    private String ip;
    private String port;
    private BlockingQueue<byte[]> outQueue;

    @Override
    public void setIpAndPort(String ip, String port) {
        this.ip = ip;
        this.port = port;
        outQueue = new LinkedBlockingQueue<>(2000);
    }

    @Override
    public byte[] poll() {
        return outQueue.poll();
    }

    @Override
    public void run() {
        // recive fzs from source
    }
}
