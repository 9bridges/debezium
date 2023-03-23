/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dm.logminer;

import io.debezium.config.Configuration;
import io.debezium.connector.dm.AbstractStreamingAdapter;
import io.debezium.connector.dm.DMConnection;
import io.debezium.connector.dm.DMConnectorConfig;
import io.debezium.connector.dm.DMDatabaseSchema;
import io.debezium.connector.dm.DMOffsetContext;
import io.debezium.connector.dm.DMStreamingChangeEventSourceMetrics;
import io.debezium.connector.dm.DMTaskContext;
import io.debezium.document.Document;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.TableId;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.util.Clock;

/**
 * @author Chris Cranford
 */
public class LogMinerAdapter extends AbstractStreamingAdapter {

    private static final String TYPE = "logminer";

    public LogMinerAdapter(DMConnectorConfig connectorConfig) {
        super(connectorConfig);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public HistoryRecordComparator getHistoryRecordComparator() {
        return new HistoryRecordComparator() {
            @Override
            protected boolean isPositionAtOrBefore(Document recorded, Document desired) {
                return resolveScn(recorded).compareTo(resolveScn(desired)) < 1;
            }
        };
    }

    @Override
    public OffsetContext.Loader<DMOffsetContext> getOffsetContextLoader() {
        return new LogMinerDMOffsetContextLoader(connectorConfig);
    }

    @Override
    public StreamingChangeEventSource<DMOffsetContext> getSource(DMConnection connection,
                                                                 EventDispatcher<TableId> dispatcher,
                                                                 ErrorHandler errorHandler,
                                                                 Clock clock,
                                                                 DMDatabaseSchema schema,
                                                                 DMTaskContext taskContext,
                                                                 Configuration jdbcConfig,
                                                                 DMStreamingChangeEventSourceMetrics streamingMetrics) {
        return new LogMinerStreamingChangeEventSource(
                connectorConfig,
                connection,
                dispatcher,
                errorHandler,
                clock,
                schema,
                jdbcConfig,
                streamingMetrics);
    }

}
