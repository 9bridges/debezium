/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.fzs.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class CustomFzsConnection implements FzsConnection {
    private final static Logger log = LoggerFactory.getLogger(CustomFzsConnection.class);
    private String ip;
    private String port;
    private BlockingQueue<byte[]> outQueue;
    private boolean isRunning = false;
    private String serverID;
    private ServerSocket serverSocket;

    private ThreadPoolExecutor executor;

    CustomFzsConnection() {
        outQueue = new LinkedBlockingQueue<>(20000);
    }

    public static byte[] file2byte(String path) {
        try {
            FileInputStream in = new FileInputStream(new File(path));
            byte[] data = new byte[in.available()];
            in.read(data);
            in.close();
            return data;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private static ThreadPoolExecutor createThreadPoolExecutor() {
        int poolCoreSize = 10;
        int maximumSize = 100;
        int poolKeepAlive = 5000;
        int queueCapacity = 10;

        LinkedBlockingQueue<Runnable> blockingQueue = new LinkedBlockingQueue<>(queueCapacity);

        return new ThreadPoolExecutor(poolCoreSize, maximumSize, poolKeepAlive, TimeUnit.MILLISECONDS, blockingQueue);
    }

    @Override
    public void setIpAndPort(String ip, String port) {
        this.ip = ip;
        this.port = port;
        outQueue = new LinkedBlockingQueue<>(2000);
    }

    @Override
    public byte[] poll() {
        log.info("FzsConnection begin poll");
        byte[] bytes = null;
        try {
            bytes = outQueue.take();
        } catch (InterruptedException ignored) {
            // do nothing
        }
        return bytes;
    }

    @Override
    public void run() {
        serverID = "SER-" + UUID.randomUUID().toString().substring(0, 6);
        executor = createThreadPoolExecutor();
        isRunning = true;
        int index = 0;
        while (isRunning) {
            if (serverSocket == null || serverSocket.isClosed()) {
                try {
                    serverSocket = new ServerSocket(Integer.parseInt(port), 10);
                    index = 0;
                    log.info("[{}] create ServerSocket at port={}", serverID, port);
                } catch (IOException e) {
                    log.error("[{}] create ServerSocket failed {} port={}", serverID, e.getMessage(), port);
                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException ignore) {
                    }
                    continue;
                }
            }
            try {
                //System.out.printf("[%s]%s wait for accept socket...\n", tName, serverName);
                log.info("[{}] wait socket connect...", serverID);
                RequestJob requestJob = new RequestJob("JOB-" + index + "-" + UUID.randomUUID().toString().substring(0, 4), serverID, serverSocket.accept(), outQueue);
                executor.execute(requestJob);
                index++;
            } catch (IOException e) {
                log.error("[{}] accept socket failed {}", serverID, e.getMessage());
            }
        }
        //System.out.printf("[%s]%s exit...\n", tName, serverName);
        log.info("[{}] exit...", serverID);
    }
}
