/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.fzs;

import io.debezium.connector.oracle.BaseChangeRecordEmitter;
import io.debezium.connector.oracle.fzs.entry.FzsDmlEntry;
import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.Table;
import io.debezium.util.Clock;

public class FzsChangeRecordEmitter extends BaseChangeRecordEmitter<Object> {

    private final FzsDmlEntry fzsDmlEntry;

    public FzsChangeRecordEmitter(Partition partition, OffsetContext offset, FzsDmlEntry lcr,
                                  Table table, Clock clock) {
        super(partition, offset, table, clock);
        this.fzsDmlEntry = lcr;
    }

    @Override
    protected Operation getOperation() {
        switch (fzsDmlEntry.getEventType()) {
            case INSERT:
                return Operation.CREATE;
            case DELETE:
                return Operation.DELETE;
            case UPDATE:
                return Operation.UPDATE;
            default:
                throw new IllegalArgumentException("Received event of unexpected command type: " + fzsDmlEntry);
        }
    }

    @Override
    protected Object[] getOldColumnValues() {
        return fzsDmlEntry.getOldValues();
    }

    @Override
    protected Object[] getNewColumnValues() {
        return fzsDmlEntry.getNewValues();
    }

}
