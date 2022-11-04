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

public class FzsDmlUrp extends FzsDmlEntryImpl {
    void parseUpdateColumnValues(ByteBuf byteBuf, Object[] values, int skipSize, boolean isBefore) throws UnsupportedEncodingException {
        int columnCount = values.length;
        String[] columName = new String[columnCount];
        int[] columnType = new int[columnCount];
        for (int index = 0; index < columnCount; index++) {
            getByteOrShort(byteBuf); // columnId
            int colLen = getByteOrShort(byteBuf);
            byte[] bytes = null;
            if (colLen > 0) {
                bytes = readBytes(byteBuf, colLen);
            }
            columName[index] = getString(byteBuf);
            columnType[index] = getByteOrShort(byteBuf);
            getByteOrInt(byteBuf); // col_length_max
            setValueByColumnType(values, columnType[index], colLen, bytes, index);
            byteBuf.readerIndex(byteBuf.readerIndex() + skipSize); // col_csform + col_csid + col_null
        }

        if (isBefore) {
            setOldColumnNames(columName);
            setOldValues(values);
            setOldColumnTypes(columnType);
        }
        else {
            setNewColumnNames(columName);
            setNewValues(values);
            setNewColumnTypes(columnType);
        }
    }

    @Override
    public void parse(byte[] data) throws IOException {
        ByteBuf byteBuf = Unpooled.wrappedBuffer(data, 0, data.length);
        setScn(byteBuf.readerIndex(SCN_OFFSET).readLong());
        setTransactionId(Long.toString(byteBuf.readerIndex(TRANS_ID_OFFSET).readLong()));
        byteBuf.readerIndex(65);
        setObjectOwner(getString(byteBuf));
        setObjectName(getString(byteBuf));
        setSourceTime(Instant.now());
        byteBuf.readerIndex(byteBuf.readerIndex() + 1); // table has pk or uk
        int columnCount = getByteOrShort(byteBuf);
        int newColumnCount = getByteOrShort(byteBuf);
        Object[] newValues = new Object[newColumnCount];
        Object[] oldValues = null;
        parseUpdateColumnValues(byteBuf, newValues, 4, false);
        if (columnCount > 0) {
            int oldColumnCount = getByteOrShort(byteBuf);
            oldValues = new Object[oldColumnCount];
            parseUpdateColumnValues(byteBuf, oldValues, 3, true);
        }
    }

    @Override
    public OpCode getEventType() {
        return OpCode.UPDATE;
    }
}
