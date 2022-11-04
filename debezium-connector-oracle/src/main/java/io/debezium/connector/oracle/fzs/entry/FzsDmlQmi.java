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
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class FzsDmlQmi extends FzsDmlEntryImpl {
    private int rowCount;
    private Object[][] rowDatas;

    @Override
    public OpCode getEventType() {
        return OpCode.MULIT_INSERT;
    }

    @Override
    public void parse(byte[] data) throws IOException {
        ByteBuf byteBuf = Unpooled.wrappedBuffer(data, 0, data.length);
        setScn(byteBuf.readerIndex(SCN_OFFSET).readLong());
        setTransactionId(Long.toString(byteBuf.readerIndex(TRANS_ID_OFFSET).readLong()));
        byteBuf.readerIndex(52);
        setObjectOwner(getString(byteBuf));
        setObjectName(getString(byteBuf));
        getString(byteBuf); // part
        setSourceTime(Instant.now());
        byteBuf.readerIndex(byteBuf.readerIndex() + 4); // table has pk or uk, is_full, if_can_dp, queue_id
        parseColumnValues(byteBuf);
    }

    void parseColumnValues(ByteBuf byteBuf) throws IOException {
        rowCount = byteBuf.readShort();
        rowDatas = new Object[rowCount][];
        for (int row = 0; row < rowCount; row++) {
            getByteOrShort(byteBuf); // slt
            byteBuf.readerIndex(byteBuf.readerIndex() + 2); // flag, bit_flag
            int columnCount = getByteOrShort(byteBuf);
            rowDatas[row] = new Object[columnCount];
            String[] colmnNames = new String[columnCount];
            int[] columnTypes = new int[columnCount];
            for (int col = 0; col < columnCount; col++) {
                int colLen = getByteOrShort(byteBuf);
                rowDatas[row][col] = new String(readBytes(byteBuf, colLen));
                if (row == 0) {
                    colmnNames[col] = getString(byteBuf);
                    columnTypes[col] = getByteOrShort(byteBuf);
                    getByteOrInt(byteBuf); // col_len_max
                    byteBuf.readerIndex(byteBuf.readerIndex() + 5); // col_unique, col_dsform, col_csid, col_null

                }
            }
            if (row == 0) {
                setNewColumnNames(colmnNames);
                setNewColumnTypes(columnTypes);
            }
        }
    }

    public int getRowCount() {
        return rowCount;
    }

    public Object[][] getRowDatas() {
        return rowDatas;
    }

    public List<FzsEntry> toList() {
        List<FzsEntry> fzsEntries = new ArrayList<>();
        for (int i = 0; i < rowCount; i++) {
            FzsDmlIrp fzsDmlIrp = new FzsDmlIrp();
            fzsDmlIrp.setDatabaseName(getDatabaseName());
            fzsDmlIrp.setObjectOwner(getObjectOwner());
            fzsDmlIrp.setObjectName(getObjectName());
            fzsDmlIrp.setNewColumnNames(getNewColumnNames());
            fzsDmlIrp.setNewValues(rowDatas[i]);
            fzsDmlIrp.setNewColumnTypes(getNewColumnTypes());
            fzsDmlIrp.setScn(getScn());
            fzsDmlIrp.setTransactionId(getTransactionId());
            fzsDmlIrp.setSourceTime(getSourceTime());
            fzsEntries.add(fzsDmlIrp);
        }
        return fzsEntries;
    }
}
