/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.fzs.entry;

public interface FzsDmlEntry extends FzsEntry {

    String INSERT = "INSERT";
    String UPDATE = "UPDATE";
    String DELETE = "DELETE";
    String COMMIT = "COMMIT";
    String BEGIN = "BEGIN";

    void setOldValues(Object[] var1);

    void setNewValues(Object[] var1);

    void setOldColumnName();

    void setNewColumnName();

    Object[] getOldValues();

    Object[] getNewValues();

    String[] getOldColumnName();

    String[] getNewColumnName();
}
