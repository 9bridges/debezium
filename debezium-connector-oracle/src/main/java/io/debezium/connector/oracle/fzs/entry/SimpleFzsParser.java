/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.fzs.entry;

import static io.debezium.connector.oracle.fzs.entry.BytesUtil.copyBytesByPos;
import static io.debezium.connector.oracle.fzs.entry.BytesUtil.toUnsignedInt;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleFzsParser implements FzsParser {
    private static final int ENTRY_TYPE_OFFSET = 4;
    private final Logger logger = LoggerFactory.getLogger(SimpleFzsParser.class);

    @Override
    public void parser(byte[] bytes, BlockingQueue<FzsEntry> outQueue) {
        int offset = 0;
        while (offset < bytes.length) {
            int dataLength = (int) toUnsignedInt(copyBytesByPos(bytes, offset, 4));
            if (dataLength > 0) {
                byte[] data = copyBytesByPos(bytes, offset, dataLength);
                try {
                    OpCode code = OpCode.from(data[ENTRY_TYPE_OFFSET] & 0xff);
                    logger.info("FzsParser a record: {}", code.name());
                    switch (code) {
                        case INSERT:
                        case UPDATE:
                        case DELETE:
                        case COMMIT:
                            outQueue.put(parseDML(data, code));
                            break;
                        case DDL:
                            outQueue.put(parseDDL(data));
                            break;
                        case UNSUPPORTED:
                            logger.warn("unsupport opcode: {}", data[ENTRY_TYPE_OFFSET] & 0xff);
                    }
                } catch (InterruptedException ignored) {
                    // do nothing
                } catch (IOException ioException) {
                    ioException.printStackTrace();
                    break;
                }
            }
            offset += dataLength + 4;
        }
    }

    FzsEntry parseDML(byte[] bytes, OpCode opCode) throws IOException {
        FzsDmlEntryImpl fzsDmlEntry = new FzsDmlEntryImpl();
        fzsDmlEntry.parse(bytes, opCode);
        return fzsDmlEntry;
    }

    FzsEntry parseDDL(byte[] bytes) {
        FzsDdlEntryImpl fzsDdlEntry = new FzsDdlEntryImpl();
        fzsDdlEntry.parse(bytes);
        return fzsDdlEntry;
    }

}
