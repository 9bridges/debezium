/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.fzs.entry;

import static io.debezium.connector.oracle.fzs.entry.BytesUtil.getByteOrInt;
import static io.debezium.connector.oracle.fzs.entry.BytesUtil.getByteOrShort;
import static io.debezium.connector.oracle.fzs.entry.BytesUtil.getString;
import static io.debezium.connector.oracle.fzs.entry.BytesUtil.readBytes;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.time.Instant;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class FzsDmlEntryImpl implements FzsDmlEntry {
    private Logger logger = LoggerFactory.getLogger(FzsDmlEntryImpl.class);
    private OpCode eventType = null;
    private Object[] newValues = null;
    private Object[] oldValues = null;
    private String objectOwner = null;
    private String objectName = null;
    private Instant sourceTime;
    private String[] columName;
    private int[] columnType;
    private long scn = -1;
    private short subScn = -1;
    private String transactionId;
    private static final int OP_SIZE_LEN = 4;
    private static final int OP_CODE_LEN = 1;
    private static final int OBJECT_ID_LEN = 4;
    private static final int SCN_LEN = 8;
    private static final int SUB_SCN_LEN = 2;
    private static final int TRANS_ID_LEN = 8;
    private static final int OBJECT_PART_ID_LEN = 4;
    private static final int OBJECT_DATA_ID_LEN = 4;
    private static final int OP_SIZE_OFFSET = 0;
    private static final int OP_CODE_OFFSET = OP_SIZE_OFFSET + OP_SIZE_LEN;
    private static final int OBJECT_ID_OFFSET = OP_CODE_OFFSET + OP_CODE_LEN;
    private static final int SCN_OFFSET = OBJECT_ID_OFFSET + OBJECT_ID_LEN;
    private static final int SUB_SCN_OFFSET = SCN_OFFSET + SCN_LEN;
    private static final int TRANS_ID_OFFSET = SUB_SCN_OFFSET + SUB_SCN_LEN;

    public FzsDmlEntryImpl() {

    }

    public void parse(byte[] data, OpCode opCode) throws IOException {
        ByteBuf byteBuf = Unpooled.wrappedBuffer(data, 0, data.length);
        this.eventType = opCode;
        switch (opCode) {
            case INSERT:
            case DELETE:
                parseInsertAndDelete(byteBuf);
                break;
            case UPDATE:
                parseUpdate(byteBuf);
                break;
            case COMMIT:
                this.transactionId = Long.toString(byteBuf.readerIndex(1).readLong());
            default:
                break; // commit operator only need set opCode
        }
    }

    void parseColumnValues(ByteBuf byteBuf, Object[] values) throws UnsupportedEncodingException {
        int arraylength = values.length;
        this.columName = new String[arraylength];
        this.columnType = new int[arraylength];
        for (int index = 0; index < values.length; index++) {
            int colLen = getByteOrShort(byteBuf);
            values[index] = new String(readBytes(byteBuf, colLen));
            columName[index] = getString(byteBuf);
            columnType[index] = getByteOrShort(byteBuf);
            getByteOrInt(byteBuf); // col_length_max
            byteBuf.readerIndex(byteBuf.readerIndex() + 5); // col_unique + col_csform + col_csid + col_null
        }
    }

    void parseUpdateColumnValues(ByteBuf byteBuf, Object[] values, int skipSize) throws UnsupportedEncodingException {
        int arraylength = values.length;
        this.columName = new String[arraylength];
        this.columnType = new int[arraylength];
        for (int index = 0; index < values.length; index++) {
            getByteOrShort(byteBuf); // columnId
            int colLen = getByteOrShort(byteBuf);
            values[index] = new String(readBytes(byteBuf, colLen));
            columName[index] = getString(byteBuf);
            columnType[index] = getByteOrShort(byteBuf);
            getByteOrInt(byteBuf); // col_length_max
            byteBuf.readerIndex(byteBuf.readerIndex() + skipSize); // col_csform + col_csid + col_null
        }
    }

    private void parseInsertAndDelete(ByteBuf byteBuf) throws IOException {
        this.scn = byteBuf.readerIndex(SCN_OFFSET).readLong();
        this.transactionId = Long.toString(byteBuf.readerIndex(TRANS_ID_OFFSET).readLong());
        byteBuf.readerIndex(64);
        this.objectOwner = getString(byteBuf);
        this.objectName = getString(byteBuf);
        this.sourceTime = Instant.now();
        byteBuf.readerIndex(byteBuf.readerIndex() + 1); // table has pk or uk
        int columnCount = getByteOrShort(byteBuf);
        switch (eventType) {
            case INSERT:
                newValues = new Object[columnCount];
                parseColumnValues(byteBuf, newValues);
                break;
            case DELETE:
                oldValues = new Object[columnCount];
                parseColumnValues(byteBuf, oldValues);
                break;
        }
    }

    private void parseUpdate(ByteBuf byteBuf) throws IOException {
        this.scn = byteBuf.readerIndex(SCN_OFFSET).readLong();
        this.transactionId = Long.toString(byteBuf.readerIndex(TRANS_ID_OFFSET).readLong());
        byteBuf.readerIndex(65);
        this.objectOwner = getString(byteBuf);
        this.objectName = getString(byteBuf);
        this.sourceTime = Instant.now();
        byteBuf.readerIndex(byteBuf.readerIndex() + 1); // table has pk or uk
        int columnCount = getByteOrShort(byteBuf);
        int newColumnCount = getByteOrShort(byteBuf);
        newValues = new Object[newColumnCount];
        parseUpdateColumnValues(byteBuf, newValues, 4);
        if (columnCount > 0) {
            int oldColumnCount = getByteOrShort(byteBuf);
            oldValues = new Object[oldColumnCount];
            parseUpdateColumnValues(byteBuf, oldValues, 3);
        }
    }

    @Override
    public void setOldValues(Object[] var1) {

    }

    @Override
    public void setNewValues(Object[] var1) {

    }

    @Override
    public void setOldColumnName() {

    }

    @Override
    public void setNewColumnName() {

    }

    @Override
    public Object[] getOldValues() {
        return oldValues;
    }

    @Override
    public Object[] getNewValues() {
        return newValues;
    }

    @Override
    public String[] getOldColumnName() {
        return columName;
    }

    @Override
    public String[] getNewColumnName() {
        return columName;
    }

    @Override
    public void setDatabaseName(String var1) {

    }

    @Override
    public void setObjectName(String name) {

    }

    @Override
    public void setObjectOwner(String name) {

    }

    @Override
    public void setSourceTime(String var1) {

    }

    @Override
    public void setScn(long scn) {
        this.scn = scn;
    }

    @Override
    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    @Override
    public void setEventType(OpCode var1) {
        eventType = var1;
    }

    @Override
    public String getObjectName() {
        return objectName;
    }

    @Override
    public String getObjectOwner() {
        return objectOwner;
    }

    @Override
    public Instant getSourceTime() {
        return sourceTime;
    }

    @Override
    public long getScn() {
        return this.scn;
    }

    @Override
    public String getTransactionId() {
        return transactionId;
    }

    @Override
    public OpCode getEventType() {
        return eventType;
    }

    @Override
    public String getDatabaseName() {
        return "fzsAdapter";
    }

    @Override
    public String toString() {
        return "FzsDmlEntryImpl{" +
                "eventType=" + eventType +
                ", newValues=" + Arrays.toString(newValues) +
                ", oldValues=" + Arrays.toString(oldValues) +
                ", objectOwner='" + objectOwner + '\'' +
                ", objectName='" + objectName + '\'' +
                ", sourceTime=" + sourceTime +
                ", columName=" + Arrays.toString(columName) +
                ", columnType=" + Arrays.toString(columnType) +
                ", scn=" + scn +
                ", subScn=" + subScn +
                ", transactionId='" + transactionId + '\'' +
                '}';
    }

}
