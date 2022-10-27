package io.debezium.connector.oracle.fzs.entry;

import java.math.BigInteger;

public interface FzsEntry {

    void setDatabaseName(String var1);

    void setObjectName(String name);

    void setObjectOwner(String name);

    void setSourceTime(String var1);

    void setScn(BigInteger scn);

    void setTransactionId(String var1);

    void setEventType(String var1);

    String getObjectName();

    String getObjectOwner();

    String getSourceTime();

    String getScn();

    String getTransactionId();

    String getEventType();

    String getDatabaseName();

}