/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.fzs.entry;

public class FzsDmlDrp extends FzsDmlIrp {

    @Override
    protected void setValues(Object[] values, String[] colNames, int[] colTypes) {
        setOldValues(values);
        setOldColumnNames(colNames);
        setOldColumnTypes(colTypes);
    }

    @Override
    public OpCode getEventType() {
        return OpCode.DELETE;
    }
}
