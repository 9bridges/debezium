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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class FzsDmlIrp extends FzsDmlEntryImpl {

    void parseColumnValues(ByteBuf byteBuf) throws UnsupportedEncodingException {
        int columnCount = getByteOrShort(byteBuf);
        String[] columNames = new String[columnCount];
        int[] columnTypes = new int[columnCount];
        Object[] values = new Object[columnCount];
        for (int index = 0; index < columnCount; index++) {
            int colLen = getByteOrInt(byteBuf);
            byte[] bytes = null;
            if (colLen > 0) {
                bytes = readBytes(byteBuf, colLen);
            }
            columNames[index] = getString(byteBuf);
            columnTypes[index] = getByteOrShort(byteBuf);
            getByteOrInt(byteBuf); // col_length_max
            setValueByColumnType(values, columnTypes[index], colLen, bytes, index);
            byteBuf.readerIndex(byteBuf.readerIndex() + 5); // col_unique + col_csform + col_csid + col_null
        }
        setValues(values, columNames, columnTypes);

    }

    protected void setValues(Object[] values, String[] colNames, int[] colTypes) {
        setNewValues(values);
        setNewColumnNames(colNames);
        setNewColumnTypes(colTypes);
    }

    @Override
    public void parse(byte[] data) throws IOException {
        ByteBuf byteBuf = Unpooled.wrappedBuffer(data, 0, data.length);
        setScn(byteBuf.readerIndex(SCN_OFFSET).readLong());
        setTransactionId(Long.toString(byteBuf.readerIndex(TRANS_ID_OFFSET).readLong()));
        byteBuf.readerIndex(64);
        setObjectOwner(getString(byteBuf));
        setObjectName(getString(byteBuf));
        setSourceTime(Instant.now());
        byteBuf.readerIndex(byteBuf.readerIndex() + 1); // table has pk or uk
        parseColumnValues(byteBuf);
    }

    @Override
    public OpCode getEventType() {
        return OpCode.INSERT;
    }
}
