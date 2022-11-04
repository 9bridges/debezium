/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.fzs.client;

import java.io.File;
import java.io.FileInputStream;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleFzsConnection implements FzsConnection {
    private Logger logger = LoggerFactory.getLogger(SimpleFzsConnection.class);
    private String ip;
    private String port;
    private BlockingQueue<byte[]> outQueue;

    SimpleFzsConnection() {
        outQueue = new LinkedBlockingQueue<>(20000);
    }

    public static byte[] file2byte(String path) {
        try {
            FileInputStream in = new FileInputStream(new File(path));
            byte[] data = new byte[in.available()];
            in.read(data);
            in.close();
            return data;
        }
        catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public void setIpAndPort(String ip, String port) {
        this.ip = ip;
        this.port = port;
        outQueue = new LinkedBlockingQueue<>(2000);
    }

    @Override
    public byte[] poll() {
        logger.info("FzsConnection begin poll");
        byte[] bytes = null;
        try {
            bytes = outQueue.take();
        }
        catch (InterruptedException ignored) {
            // do nothing
        }
        return bytes;
    }

    @Override
    public void run() {
        // recive fzs from source
        while (true) {
            try {
                byte[] bytes = file2byte("D:\\code\\debezium\\debezium-connector-oracle\\src\\test\\resources\\fzs\\qmi.fzs");

                logger.info("FzsConnection put a bytes");
                outQueue.put(bytes);
                Thread.sleep(3000);
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
