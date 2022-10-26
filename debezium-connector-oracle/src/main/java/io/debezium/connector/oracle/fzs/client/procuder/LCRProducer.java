/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.fzs.client.procuder;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import oracle.sql.NUMBER;
import oracle.streams.ChunkColumnValue;
import oracle.streams.LCR;
import oracle.streams.StreamsException;
import oracle.streams.XStreamLCRCallbackHandler;
import oracle.streams.XStreamOut;
import oracle.streams.XStreamUtility;

public class LCRProducer extends Thread {
    private XStreamOut xsOut;
    private static final Logger logger = LoggerFactory.getLogger(LCRProducer.class);

    private DatabaseInfo databaseInfo;
    private BlockingQueue<LCR> outQueue;

    public LCRProducer(DatabaseInfo databaseInfo, BlockingQueue<LCR> outQueue) {
        this.databaseInfo = databaseInfo;
        this.outQueue = outQueue;
    }

    private String currentScn(Connection connection) throws SQLException {
        Statement st = connection.createStatement();
        String sql = "select current_scn from v$database";
        ResultSet rs = st.executeQuery(sql);
        rs.next();
        String scn = rs.getString(1);
        rs.close();
        st.close();
        logger.info("current scn is: {}", scn);
        return scn;
    }

    @Override
    public void run() {
        String url = "jdbc:oracle:oci:@" + databaseInfo.url;
        logger.info("XStream Plugins Starting, {}", databaseInfo.toString());
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
        }
        catch (ClassNotFoundException e) {
            logger.error("ClassNotFoundException:", e);
            throw new RuntimeException(e);
        }
        Connection connection;
        try {
            connection = DriverManager.getConnection(url, databaseInfo.userName, databaseInfo.passWord);
            byte[] startPosition;
            startPosition = XStreamUtility.convertSCNToPosition(new NUMBER(currentScn(connection), 0));
            xsOut = XStreamOut.attach((oracle.jdbc.OracleConnection) connection, databaseInfo.xstreamService,
                    startPosition, 1, 1, XStreamOut.DEFAULT_MODE);
            // 2. receive events while running
            while (true) {
                xsOut.receiveLCRCallback(new LcrEventHandler(outQueue), XStreamOut.DEFAULT_MODE);
            }
        }
        catch (StreamsException | SQLException e) {
            logger.error("Exception:", e);
            throw new RuntimeException(e);
        }
        finally {
            // 3. disconnect
            if (this.xsOut != null) {
                try {
                    XStreamOut xsOut2 = this.xsOut;
                    this.xsOut = null;
                    xsOut2.detach(XStreamOut.DEFAULT_MODE);
                }
                catch (StreamsException e) {
                    logger.error("StreamsException:", e);
                    e.printStackTrace();
                }
            }
        }
    }

    public static class LcrEventHandler implements XStreamLCRCallbackHandler {
        private BlockingQueue<LCR> outQueue;

        LcrEventHandler(BlockingQueue<LCR> outQueue) {
            this.outQueue = outQueue;
        }

        @Override
        public void processLCR(LCR lcr) {
            try {
                outQueue.put(lcr);
            }
            catch (InterruptedException e) {
                //
            }
        }

        @Override
        public void processChunk(ChunkColumnValue chunkColumnValue) {
            logger.info("processChunk :" + chunkColumnValue);
        }

        @Override
        public LCR createLCR() {
            logger.info("createLCR");
            return null;
        }

        @Override
        public ChunkColumnValue createChunk() {
            logger.info("createChunk");
            return null;
        }
    }

}
