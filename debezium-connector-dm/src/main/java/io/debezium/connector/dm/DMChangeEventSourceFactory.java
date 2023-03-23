/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dm;

import io.debezium.config.Configuration;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;

public class DMChangeEventSourceFactory implements ChangeEventSourceFactory<DMOffsetContext> {

    private final DMConnectorConfig configuration;
    private final DMConnection jdbcConnection;
    private final ErrorHandler errorHandler;
    private final EventDispatcher<TableId> dispatcher;
    private final Clock clock;
    private final DMDatabaseSchema schema;
    private final Configuration jdbcConfig;
    private final DMTaskContext taskContext;
    private final DMStreamingChangeEventSourceMetrics streamingMetrics;

    public DMChangeEventSourceFactory(DMConnectorConfig configuration, DMConnection jdbcConnection,
                                      ErrorHandler errorHandler, EventDispatcher<TableId> dispatcher, Clock clock, DMDatabaseSchema schema,
                                      Configuration jdbcConfig, DMTaskContext taskContext,
                                      DMStreamingChangeEventSourceMetrics streamingMetrics) {
        this.configuration = configuration;
        this.jdbcConnection = jdbcConnection;
        this.errorHandler = errorHandler;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.schema = schema;
        this.jdbcConfig = jdbcConfig;
        this.taskContext = taskContext;
        this.streamingMetrics = streamingMetrics;
    }

    @Override
    public SnapshotChangeEventSource<DMOffsetContext> getSnapshotChangeEventSource(SnapshotProgressListener snapshotProgressListener) {
        return new DMSnapshotChangeEventSource(configuration, jdbcConnection,
                schema, dispatcher, clock, snapshotProgressListener);
    }

    @Override
    public StreamingChangeEventSource<DMOffsetContext> getStreamingChangeEventSource() {
        return configuration.getAdapter().getSource(
                jdbcConnection,
                dispatcher,
                errorHandler,
                clock,
                schema,
                taskContext,
                jdbcConfig,
                streamingMetrics);
    }
}
