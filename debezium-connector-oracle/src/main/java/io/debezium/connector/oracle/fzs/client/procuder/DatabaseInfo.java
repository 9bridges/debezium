/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.fzs.client.procuder;

public class DatabaseInfo {
    public String xstreamService;
    public String userName;
    public String passWord;
    public String url;
    public String startScn;

    public DatabaseInfo(String xstreamService, String userName, String passWord, String url, String startScn) {
        this.xstreamService = xstreamService;
        this.userName = userName;
        this.passWord = passWord;
        this.url = url;
        this.startScn = startScn;
    }

    @Override
    public String toString() {
        return "DatabaseInfo{" +
                "xstreamService='" + xstreamService + '\'' +
                ", userName='" + userName + '\'' +
                ", passWord='" + passWord + '\'' +
                ", url='" + url + '\'' +
                '}';
    }
}
