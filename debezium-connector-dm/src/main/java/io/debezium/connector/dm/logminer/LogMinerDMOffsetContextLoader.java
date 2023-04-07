/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dm.logminer;

import java.util.Collections;
import java.util.Map;

import io.debezium.connector.dm.DMConnectorConfig;
import io.debezium.connector.dm.DMOffsetContext;
import io.debezium.connector.dm.Scn;
import io.debezium.connector.dm.SourceInfo;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;

/**
 * @author Chris Cranford
 */
public class LogMinerDMOffsetContextLoader implements OffsetContext.Loader<DMOffsetContext> {

    private final DMConnectorConfig connectorConfig;

    public LogMinerDMOffsetContextLoader(DMConnectorConfig connectorConfig) {
        this.connectorConfig = connectorConfig;
    }

    @Override
    public Map<String, ?> getPartition() {
        return Collections.singletonMap(DMOffsetContext.SERVER_PARTITION_KEY, connectorConfig.getLogicalName());
    }

    @Override
    public DMOffsetContext load(Map<String, ?> offset) {
        boolean snapshot = Boolean.TRUE.equals(offset.get(SourceInfo.SNAPSHOT_KEY));
        boolean snapshotCompleted = Boolean.TRUE.equals(offset.get(DMOffsetContext.SNAPSHOT_COMPLETED_KEY));

        Scn scn = DMOffsetContext.getScnFromOffsetMapByKey(offset, SourceInfo.SCN_KEY);
        Scn commitScn = DMOffsetContext.getScnFromOffsetMapByKey(offset, SourceInfo.COMMIT_SCN_KEY);
        return new DMOffsetContext(connectorConfig, scn, commitScn, null, snapshot, snapshotCompleted, TransactionContext.load(offset));
    }

}
