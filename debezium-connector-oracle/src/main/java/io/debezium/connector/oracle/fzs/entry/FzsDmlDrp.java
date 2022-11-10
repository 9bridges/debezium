/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.fzs.entry;

import io.debezium.connector.oracle.OracleValueConverters;

public class FzsDmlDrp extends FzsDmlIrp {

    @Override
    protected void setValues(Object[] values, String[] colNames, int[] colTypes) {
        for (int index = 0; index < colTypes.length; index++) {
            if (isLob(colTypes[index])) {
                values[index] = OracleValueConverters.UNAVAILABLE_VALUE;
            }
        }
        setOldValues(values);
        setOldColumnNames(colNames);
        setOldColumnTypes(colTypes);
    }

    @Override
    public OpCode getEventType() {
        return OpCode.DELETE;
    }
}
