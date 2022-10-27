package io.debezium.connector.oracle.fzs.entry;

public interface FzsDdlEntry extends FzsEntry {
    void setDDLString(String ddlString);
    void setObjectType(String objectType);
}
