package io.debezium.connector.oracle.fzs.client;

public interface FzsConnection extends Runnable {
    void setIpAndPort(String ip, String port);

    byte[] poll();
}
