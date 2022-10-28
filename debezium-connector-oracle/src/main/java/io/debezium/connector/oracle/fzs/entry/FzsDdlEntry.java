/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.fzs.entry;

public interface FzsDdlEntry extends FzsEntry {
    void setDDLString(String ddlString);

    void setObjectType(String objectType);

    String getDDLString();

    String getObjectType();
}
