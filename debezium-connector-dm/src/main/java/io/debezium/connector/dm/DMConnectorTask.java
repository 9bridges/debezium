/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dm;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.connector.dm.StreamingAdapter.TableNameCaseSensitivity;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;
import io.debezium.util.Clock;
import io.debezium.util.SchemaNameAdjuster;

public class DMConnectorTask extends BaseSourceTask<DMOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DMConnectorTask.class);
    private static final String CONTEXT_NAME = "dm-connector-task";

    private volatile DMTaskContext taskContext;
    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile DMConnection jdbcConnection;
    private volatile ErrorHandler errorHandler;
    private volatile DMDatabaseSchema schema;

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public ChangeEventSourceCoordinator<DMOffsetContext> start(Configuration config) {
        DMConnectorConfig connectorConfig = new DMConnectorConfig(config);
        TopicSelector<TableId> topicSelector = DMTopicSelector.defaultSelector(connectorConfig);
        SchemaNameAdjuster schemaNameAdjuster = SchemaNameAdjuster.create();

        Configuration jdbcConfig = connectorConfig.getJdbcConfig();
        jdbcConnection = new DMConnection(jdbcConfig, () -> getClass().getClassLoader());

        DMValueConverters valueConverters = new DMValueConverters(connectorConfig, jdbcConnection);
        TableNameCaseSensitivity tableNameCaseSensitivity = connectorConfig.getAdapter().getTableNameCaseSensitivity(jdbcConnection);
        this.schema = new DMDatabaseSchema(connectorConfig, valueConverters, schemaNameAdjuster, topicSelector, tableNameCaseSensitivity);
        this.schema.initializeStorage();
        DMOffsetContext previousOffset = getPreviousOffset(connectorConfig.getAdapter().getOffsetContextLoader());

        if (previousOffset != null) {
            schema.recover(previousOffset);
        }

        taskContext = new DMTaskContext(connectorConfig, schema);

        Clock clock = Clock.system();

        // Set up the task record queue ...
        this.queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .pollInterval(connectorConfig.getPollInterval())
                .maxBatchSize(connectorConfig.getMaxBatchSize())
                .maxQueueSize(connectorConfig.getMaxQueueSize())
                .loggingContextSupplier(() -> taskContext.configureLoggingContext(CONTEXT_NAME))
                .build();

        errorHandler = new DMErrorHandler(connectorConfig.getLogicalName(), queue);

        final DMEventMetadataProvider metadataProvider = new DMEventMetadataProvider();

        EventDispatcher<TableId> dispatcher = new EventDispatcher<>(
                connectorConfig,
                topicSelector,
                schema,
                queue,
                connectorConfig.getTableFilters().dataCollectionFilter(),
                DataChangeEvent::new,
                metadataProvider,
                schemaNameAdjuster);

        final DMStreamingChangeEventSourceMetrics streamingMetrics = new DMStreamingChangeEventSourceMetrics(taskContext, queue, metadataProvider,
                connectorConfig);

        ChangeEventSourceCoordinator<DMOffsetContext> coordinator = new ChangeEventSourceCoordinator<>(
                previousOffset,
                errorHandler,
                DMConnector.class,
                connectorConfig,
                new DMChangeEventSourceFactory(connectorConfig, jdbcConnection, errorHandler, dispatcher, clock, schema, jdbcConfig, taskContext, streamingMetrics),
                new DMChangeEventSourceMetricsFactory(streamingMetrics),
                dispatcher,
                schema);

        coordinator.start(taskContext, this.queue, metadataProvider);

        return coordinator;
    }

    @Override
    public List<SourceRecord> doPoll() throws InterruptedException {
        List<DataChangeEvent> records = queue.poll();

        List<SourceRecord> sourceRecords = records.stream()
                .map(DataChangeEvent::getRecord)
                .collect(Collectors.toList());

        return sourceRecords;
    }

    @Override
    public void doStop() {
        try {
            if (jdbcConnection != null) {
                jdbcConnection.close();
            }
        }
        catch (SQLException e) {
            LOGGER.error("Exception while closing JDBC connection", e);
        }

        schema.close();
    }

    @Override
    protected Iterable<Field> getAllConfigurationFields() {
        return DMConnectorConfig.ALL_FIELDS;
    }
}
