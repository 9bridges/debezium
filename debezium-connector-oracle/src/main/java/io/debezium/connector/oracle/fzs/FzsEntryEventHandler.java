/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.fzs;

import java.sql.SQLException;
import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.OracleSchemaChangeEventEmitter;
import io.debezium.connector.oracle.OracleStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.fzs.entry.FzsDdlEntry;
import io.debezium.connector.oracle.fzs.entry.FzsDmlEntry;
import io.debezium.connector.oracle.fzs.entry.FzsEntry;
import io.debezium.connector.oracle.fzs.entry.OpCode;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;

class FzsEntryEventHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(FzsEntryEventHandler.class);

    private final OracleConnectorConfig connectorConfig;
    private final ErrorHandler errorHandler;
    private final EventDispatcher<TableId> dispatcher;
    private final Clock clock;
    private final OracleDatabaseSchema schema;
    private final OraclePartition partition;
    private final OracleOffsetContext offsetContext;
    private final boolean tablenameCaseInsensitive;
    private final OracleStreamingChangeEventSourceMetrics streamingMetrics;

    public FzsEntryEventHandler(OracleConnectorConfig connectorConfig, ErrorHandler errorHandler, EventDispatcher<TableId> dispatcher, Clock clock,
                                OracleDatabaseSchema schema, OraclePartition partition, OracleOffsetContext offsetContext,
                                boolean tablenameCaseInsensitive, OracleStreamingChangeEventSourceMetrics streamingMetrics) {
        this.connectorConfig = connectorConfig;
        this.errorHandler = errorHandler;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.schema = schema;
        this.partition = partition;
        this.offsetContext = offsetContext;
        this.tablenameCaseInsensitive = tablenameCaseInsensitive;
        this.streamingMetrics = streamingMetrics;
    }

    public void processFzsEntry(FzsEntry fzsEntry) {
        LOGGER.trace("Received LCR {}", fzsEntry.getEventType());

        final Scn fzsEntryScn = Scn.valueOf(fzsEntry.getScn());

        // After a restart it may happen we get the event with the last processed LCR again
        Scn offsetScn = offsetContext.getScn();
        if (fzsEntryScn.compareTo(offsetScn) <= 0) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Ignoring change event with already processed SCN {}, last SCN {}",
                        fzsEntryScn, offsetScn);
            }
            /* return; */
        }

        offsetContext.setScn(Scn.valueOf(fzsEntry.getScn()));
        offsetContext.setTransactionId(fzsEntry.getTransactionId());
        offsetContext.tableEvent(new TableId(fzsEntry.getDatabaseName(), fzsEntry.getObjectOwner(), fzsEntry.getObjectName()),
                fzsEntry.getSourceTime());
        try {
            if (fzsEntry instanceof FzsDmlEntry) {
                LOGGER.info("Received DML LCR {}", fzsEntry);
                processDmlEntry((FzsDmlEntry) fzsEntry);
            }
            else if (fzsEntry instanceof FzsDdlEntry) {
                dispatchSchemaChangeEvent((FzsDdlEntry) fzsEntry);
            }
        }
        catch (InterruptedException e) {
            Thread.interrupted();
            LOGGER.info("Received signal to stop, event loop will halt");
        }
        catch (Exception e) {
            errorHandler.setProducerThrowable(e);
        }
    }

    private void processDmlEntry(FzsDmlEntry fzsDmlEntry) throws InterruptedException {
        LOGGER.trace("Processing DML event {}", fzsDmlEntry.getEventType());
        if (fzsDmlEntry.getEventType() == OpCode.COMMIT) {
            dispatcher.dispatchTransactionCommittedEvent(partition, offsetContext);
            return;
        }

        TableId tableId = getTableId(fzsDmlEntry);

        Table table = schema.tableFor(tableId);
        if (table == null) {
            if (!connectorConfig.getTableFilters().dataCollectionFilter().isIncluded(tableId)) {
                LOGGER.trace("Table {} is new but excluded, schema change skipped.", tableId);
                return;
            }
            LOGGER.info("Table {} is new and will be captured.", tableId);
            dispatcher.dispatchSchemaChangeEvent(
                    tableId,
                    new OracleSchemaChangeEventEmitter(
                            connectorConfig,
                            partition,
                            offsetContext,
                            tableId,
                            tableId.catalog(),
                            tableId.schema(),
                            getTableMetadataDdl(tableId),
                            schema,
                            Instant.now(),
                            streamingMetrics));
        }

        dispatcher.dispatchDataChangeEvent(
                tableId,
                new FzsChangeRecordEmitter(partition, offsetContext, fzsDmlEntry,
                        schema.tableFor(tableId), clock));
    }

    private void dispatchSchemaChangeEvent(FzsDdlEntry ddlLcr) throws InterruptedException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Processing DDL event {}", ddlLcr.getDDLString());
        }

        TableId tableId = getTableId(ddlLcr);

        dispatcher.dispatchSchemaChangeEvent(
                tableId,
                new OracleSchemaChangeEventEmitter(
                        connectorConfig,
                        partition,
                        offsetContext,
                        tableId,
                        ddlLcr.getDatabaseName(),
                        ddlLcr.getObjectOwner(),
                        ddlLcr.getDDLString(),
                        schema,
                        ddlLcr.getSourceTime(),
                        streamingMetrics));
    }

    private TableId getTableId(FzsEntry fzsEntry) {
        if (!this.tablenameCaseInsensitive) {
            return new TableId(fzsEntry.getDatabaseName(), fzsEntry.getObjectOwner(), fzsEntry.getObjectName());
        }
        else {
            return new TableId(fzsEntry.getDatabaseName(), fzsEntry.getObjectOwner(), fzsEntry.getObjectName().toLowerCase());
        }
    }

    private String getTableMetadataDdl(TableId tableId) {
        final String pdbName = connectorConfig.getPdbName();
        // A separate connection must be used for this out-of-bands query while processing the Xstream callback.
        // This should have negligible overhead as this should happen rarely.
        try (OracleConnection connection = new OracleConnection(connectorConfig.getJdbcConfig(), () -> getClass().getClassLoader())) {
            if (pdbName != null) {
                connection.setSessionToPdb(pdbName);
            }
            return connection.getTableMetadataDdl(tableId);
        }
        catch (SQLException e) {
            throw new DebeziumException("Failed to get table DDL metadata for: " + tableId, e);
        }
    }

}
