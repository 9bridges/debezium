/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.fzs;

import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleDatabaseVersion;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.OracleStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.SourceInfo;
import io.debezium.connector.oracle.StreamingAdapter.TableNameCaseSensitivity;
import io.debezium.connector.oracle.fzs.client.FzsClient;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.Strings;

import oracle.sql.NUMBER;
import oracle.streams.StreamsException;
import oracle.streams.XStreamOut;
import oracle.streams.XStreamUtility;

public class FzsStreamingChangeEventSource implements StreamingChangeEventSource<OraclePartition, OracleOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FzsStreamingChangeEventSource.class);

    private final OracleConnectorConfig connectorConfig;
    private final OracleConnection jdbcConnection;
    private final EventDispatcher<TableId> dispatcher;
    private final ErrorHandler errorHandler;
    private final Clock clock;
    private final OracleDatabaseSchema schema;
    private final OracleStreamingChangeEventSourceMetrics streamingMetrics;
    private final String xStreamServerName;
    private volatile XStreamOut xsOut;
    private final int posVersion;
    /**
     * A message box between thread that is informed about committed offsets and the XStream thread.
     * When the last offset is committed its value is passed to the XStream thread and a watermark is
     * set to signal which events were safely processed.
     * This is important as setting watermark in a concurrent thread can lead to a deadlock due to an
     * internal Oracle code locking.
     */
    private final AtomicReference<PositionAndScn> lcrMessage = new AtomicReference<>();

    public FzsStreamingChangeEventSource(OracleConnectorConfig connectorConfig, OracleConnection jdbcConnection,
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
        this.xStreamServerName = connectorConfig.getXoutServerName();
        this.posVersion = resolvePosVersion(jdbcConnection, connectorConfig);
    }

    @Override
    public void execute(ChangeEventSourceContext context, OraclePartition partition, OracleOffsetContext offsetContext)
            throws InterruptedException {
        LcrEventHandler eventHandler = new LcrEventHandler(connectorConfig, errorHandler, dispatcher, clock, schema,
                partition, offsetContext,
                TableNameCaseSensitivity.INSENSITIVE.equals(connectorConfig.getAdapter().getTableNameCaseSensitivity(jdbcConnection)),
                this, streamingMetrics);
        FzsClient fzsClient = new FzsClient("127.0.0.1", "8303");
        fzsClient.setListener(lcr -> eventHandler.processLCR(lcr));

        fzsClient.start();
        while (context.isRunning()) {
            Thread.sleep(5000);
        }
        fzsClient.stop();
    }

    @Override
    public void commitOffset(Map<String, ?> offset) {
        if (xsOut != null) {
            LOGGER.debug("Sending message to request recording of offsets to Oracle");
            final LcrPosition lcrPosition = LcrPosition.valueOf((String) offset.get(SourceInfo.LCR_POSITION_KEY));
            final Scn scn = OracleOffsetContext.getScnFromOffsetMapByKey(offset, SourceInfo.SCN_KEY);
            // We can safely overwrite the message even if it was not processed. The watermarked will be set to the highest
            // (last) delivered value in a single step instead of incrementally
            sendPublishedPosition(lcrPosition, scn);
        }
    }

    private byte[] convertScnToPosition(Scn scn) {
        try {
            return XStreamUtility.convertSCNToPosition(new NUMBER(scn.toString(), 0), this.posVersion);
        }
        catch (SQLException | StreamsException e) {
            throw new RuntimeException(e);
        }
    }

    public static class PositionAndScn {
        public final LcrPosition position;
        public final byte[] scn;

        public PositionAndScn(LcrPosition position, byte[] scn) {
            this.position = position;
            this.scn = scn;
        }
    }

    XStreamOut getXsOut() {
        return xsOut;
    }

    private void sendPublishedPosition(final LcrPosition lcrPosition, final Scn scn) {
        lcrMessage.set(new PositionAndScn(lcrPosition, (scn != null) ? convertScnToPosition(scn) : null));
    }

    PositionAndScn receivePublishedPosition() {
        return lcrMessage.getAndSet(null);
    }

    private static int resolvePosVersion(OracleConnection connection, OracleConnectorConfig connectorConfig) {
        // Option 'internal.database.oracle.version' takes precedence
        final String oracleVersion = connectorConfig.getOracleVersion();
        if (!Strings.isNullOrEmpty(oracleVersion)) {
            if ("11".equals(oracleVersion)) {
                return XStreamUtility.POS_VERSION_V1;
            }
            return XStreamUtility.POS_VERSION_V2;
        }

        // As fallback, resolve this based on the OracleDatabaseVersion
        final OracleDatabaseVersion databaseVersion = connection.getOracleVersion();
        if (databaseVersion.getMajor() == 11) {
            return XStreamUtility.POS_VERSION_V1;
        }
        return XStreamUtility.POS_VERSION_V2;
    }
}
