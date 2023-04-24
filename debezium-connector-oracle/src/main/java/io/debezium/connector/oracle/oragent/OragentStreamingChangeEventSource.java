/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.oragent;

import java.sql.SQLException;
import java.time.ZoneOffset;
import java.util.Map;

import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OracleStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.StreamingAdapter.TableNameCaseSensitivity;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;

import net.tbsoft.oragentclient.OragentClient;

public class OragentStreamingChangeEventSource implements StreamingChangeEventSource<OracleOffsetContext> {
    private final OracleConnectorConfig connectorConfig;
    private final OracleConnection jdbcConnection;
    private final EventDispatcher<TableId> dispatcher;
    private final ErrorHandler errorHandler;
    private final Clock clock;
    private final OracleDatabaseSchema schema;
    private final OracleStreamingChangeEventSourceMetrics streamingMetrics;
    private ZoneOffset zoneOffset;
    public OragentStreamingChangeEventSource(OracleConnectorConfig connectorConfig, OracleConnection jdbcConnection,
                                             EventDispatcher<TableId> dispatcher, ErrorHandler errorHandler,
                                             Clock clock, OracleDatabaseSchema schema,
                                             OracleStreamingChangeEventSourceMetrics streamingMetrics) {
        this.connectorConfig = connectorConfig;
        this.jdbcConnection = jdbcConnection;
        this.dispatcher = dispatcher;
        this.errorHandler = errorHandler;
        this.clock = clock;
        this.schema = schema;
        this.streamingMetrics = streamingMetrics;
    }

    @Override
    public void execute(ChangeEventSourceContext context, OracleOffsetContext offsetContext)
            throws InterruptedException {
        OragentEntryEventHandler eventHandler = new OragentEntryEventHandler(connectorConfig, errorHandler, dispatcher, clock, schema,
                offsetContext,
                TableNameCaseSensitivity.INSENSITIVE.equals(connectorConfig.getAdapter().getTableNameCaseSensitivity(jdbcConnection)),
                streamingMetrics, this);
        OragentClient oragentClient = new OragentClient(connectorConfig.getOragentServerPort());
        oragentClient.setListener(eventHandler::processOragentEntry);

        oragentClient.start();
        while (context.isRunning()) {
            Thread.sleep(5000);
        }
        oragentClient.stop();
    }

    public ZoneOffset getZoneOffset() throws SQLException {
        if (zoneOffset == null) {
            zoneOffset = jdbcConnection.getSystime().getOffset();
        }
        return zoneOffset;
    }

    @Override
    public void commitOffset(Map<String, ?> offset) {
        // do nothing
    }
}
