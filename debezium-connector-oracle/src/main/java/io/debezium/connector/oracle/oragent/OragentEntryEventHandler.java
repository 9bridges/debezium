/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.oragent;

import java.sql.SQLException;
import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OracleSchemaChangeEventEmitter;
import io.debezium.connector.oracle.OracleStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.Scn;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;

import net.tbsoft.oragentclient.client.entry.OpCode;
import net.tbsoft.oragentclient.client.entry.OragentDdlEntry;
import net.tbsoft.oragentclient.client.entry.OragentDmlEntry;
import net.tbsoft.oragentclient.client.entry.OragentEntry;

class OragentEntryEventHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(OragentEntryEventHandler.class);

    private final OracleConnectorConfig connectorConfig;
    private final ErrorHandler errorHandler;
    private final EventDispatcher<TableId> dispatcher;
    private final Clock clock;
    private final OracleDatabaseSchema schema;
    private final OracleOffsetContext offsetContext;
    private final boolean tablenameCaseInsensitive;
    private final OracleStreamingChangeEventSourceMetrics streamingMetrics;

    public OragentEntryEventHandler(OracleConnectorConfig connectorConfig, ErrorHandler errorHandler, EventDispatcher<TableId> dispatcher, Clock clock,
                                    OracleDatabaseSchema schema, OracleOffsetContext offsetContext,
                                    boolean tablenameCaseInsensitive, OracleStreamingChangeEventSourceMetrics streamingMetrics) {
        this.connectorConfig = connectorConfig;
        this.errorHandler = errorHandler;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.schema = schema;
        this.offsetContext = offsetContext;
        this.tablenameCaseInsensitive = tablenameCaseInsensitive;
        this.streamingMetrics = streamingMetrics;
    }

    public void processOragentEntry(OragentEntry oragentEntry) {
        LOGGER.trace("Received LCR {}", oragentEntry.getEventType());

        final Scn oragentEntryScn = Scn.valueOf(oragentEntry.getScn());

        // After a restart it may happen we get the event with the last processed LCR again
        Scn offsetScn = offsetContext.getScn();
        if (oragentEntryScn.compareTo(offsetScn) <= 0) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Ignoring change event with already processed SCN {}, last SCN {}",
                        oragentEntryScn, offsetScn);
            }
            /* return; */
        }

        offsetContext.setScn(Scn.valueOf(oragentEntry.getScn()));
        offsetContext.setTransactionId(oragentEntry.getTransactionId());
        offsetContext.tableEvent(new TableId(oragentEntry.getDatabaseName(), oragentEntry.getObjectOwner(), oragentEntry.getObjectName()),
                oragentEntry.getSourceTime());
        try {
            if (oragentEntry instanceof OragentDmlEntry) {
                LOGGER.trace("Received DML LCR {}", oragentEntry);
                processDmlEntry((OragentDmlEntry) oragentEntry);
            }
            else if (oragentEntry instanceof OragentDdlEntry) {
                dispatchSchemaChangeEvent((OragentDdlEntry) oragentEntry);
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

    private void processDmlEntry(OragentDmlEntry oragentDmlEntry) throws InterruptedException {
        LOGGER.trace("Processing DML event {}", oragentDmlEntry.getEventType());
        if (oragentDmlEntry.getEventType() == OpCode.COMMIT) {
            dispatcher.dispatchTransactionCommittedEvent(offsetContext);
            return;
        }

        TableId tableId = getTableId(oragentDmlEntry);

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
                new OragentChangeRecordEmitter(offsetContext, oragentDmlEntry,
                        schema.tableFor(tableId), clock));
    }

    private void dispatchSchemaChangeEvent(OragentDdlEntry ddlLcr) throws InterruptedException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Processing DDL event {}", ddlLcr.getDDLString());
        }

        TableId tableId = getTableId(ddlLcr);

        dispatcher.dispatchSchemaChangeEvent(
                tableId,
                new OracleSchemaChangeEventEmitter(
                        connectorConfig,
                        offsetContext,
                        tableId,
                        ddlLcr.getDatabaseName(),
                        ddlLcr.getObjectOwner(),
                        ddlLcr.getDDLString(),
                        schema,
                        ddlLcr.getSourceTime(),
                        streamingMetrics));
    }

    private TableId getTableId(OragentEntry oragentEntry) {
        if (!this.tablenameCaseInsensitive) {
            return new TableId(connectorConfig.getCatalogName(), oragentEntry.getObjectOwner(), oragentEntry.getObjectName());
        }
        else {
            return new TableId(connectorConfig.getCatalogName(), oragentEntry.getObjectOwner(), oragentEntry.getObjectName().toLowerCase());
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
