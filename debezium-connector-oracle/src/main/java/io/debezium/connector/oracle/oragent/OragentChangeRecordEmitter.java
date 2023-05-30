/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.oragent;

import static io.debezium.connector.oracle.OracleConnectorConfig.GENERATED_PK_NAME;

import io.debezium.connector.oracle.BaseChangeRecordEmitter;
import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.Table;
import io.debezium.util.Clock;

import net.tbsoft.oragentclient.client.entry.OragentDmlEntry;

public class OragentChangeRecordEmitter extends BaseChangeRecordEmitter<Object> {

    private final OragentDmlEntry oragentDmlEntry;

    public OragentChangeRecordEmitter(OffsetContext offset, OragentDmlEntry lcr,
                                      Table table, Clock clock) {
        super(offset, table, clock);
        this.oragentDmlEntry = lcr;
    }

    @Override
    protected Operation getOperation() {
        switch (oragentDmlEntry.getEventType()) {
            case INSERT:
                return Operation.CREATE;
            case DELETE:
                return Operation.DELETE;
            case UPDATE:
                return Operation.UPDATE;
            default:
                throw new IllegalArgumentException("Received event of unexpected command type: " + oragentDmlEntry);
        }
    }

    @Override
    protected Object[] getOldColumnValues() {
        return getColumnValues(oragentDmlEntry.getOldValues());
    }

    @Override
    protected Object[] getNewColumnValues() {
        return getColumnValues(oragentDmlEntry.getNewValues());
    }

    private Object[] getColumnValues(Object[] columnValues) {
        if (!table.primaryKeyColumnNames().contains(GENERATED_PK_NAME)) {
            return columnValues;
        }
        else {
            int i;
            Object[] values = new Object[table.columns().size()];
            for (i = 0; i < columnValues.length; i++) {
                values[i] = columnValues[i];
            }
            values[i] = oragentDmlEntry.getRowid();
            return values;
        }
    }

}
