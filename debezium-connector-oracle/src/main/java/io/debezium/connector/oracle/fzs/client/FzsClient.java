/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.fzs.client;

public class FzsClient {
    private final FzsClientStream stream;

    public FzsClient(String ip, String port) {
        this.stream = new FzsClientStream(ip, port);
    }

    public void start() {
        stream.start();
    }

    public void stop() {
        stream.stop();
    }

    public void setListener(FzsRecordListener fzsRecordListener) {
        stream.setFzsRecordListener(fzsRecordListener);
    }
}
