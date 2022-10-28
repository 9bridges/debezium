/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.fzs.entry;

import oracle.sql.DATE;

public interface FzsEntry {

    void setDatabaseName(String var1);

    void setObjectName(String name);

    void setObjectOwner(String name);

    void setSourceTime(String var1);

    void setScn(long scn);

    void setTransactionId(String var1);

    void setEventType(String var1);

    String getObjectName();

    String getObjectOwner();

    DATE getSourceTime();

    long getScn();

    String getTransactionId();

    String getEventType();

    String getDatabaseName();

}