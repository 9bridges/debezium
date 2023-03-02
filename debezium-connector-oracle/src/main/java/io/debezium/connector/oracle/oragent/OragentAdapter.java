/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.oragent;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.AbstractStreamingAdapter;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OracleStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.OracleTaskContext;
import io.debezium.document.Document;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.TableId;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.util.Clock;

public class OragentAdapter extends AbstractStreamingAdapter {

    private static final String TYPE = "oragent";

    public OragentAdapter(OracleConnectorConfig connectorConfig) {
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
    public OffsetContext.Loader<OracleOffsetContext> getOffsetContextLoader() {
        return new OragentOracleOffsetContextLoader(connectorConfig);
    }

    @Override
    public StreamingChangeEventSource<OracleOffsetContext> getSource(OracleConnection connection,
                                                                     EventDispatcher<TableId> dispatcher,
                                                                     ErrorHandler errorHandler,
                                                                     Clock clock,
                                                                     OracleDatabaseSchema schema,
                                                                     OracleTaskContext taskContext,
                                                                     Configuration jdbcConfig,
                                                                     OracleStreamingChangeEventSourceMetrics streamingMetrics) {
        return new OragentStreamingChangeEventSource(
                connectorConfig,
                connection,
                dispatcher,
                errorHandler,
                clock,
                schema,
                streamingMetrics);
    }
}
