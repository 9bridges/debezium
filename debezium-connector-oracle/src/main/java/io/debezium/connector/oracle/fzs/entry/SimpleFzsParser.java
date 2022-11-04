/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.fzs.entry;

import static io.debezium.connector.oracle.fzs.entry.BytesUtil.copyBytesByPos;
import static io.debezium.connector.oracle.fzs.entry.BytesUtil.toUnsignedInt;

import java.io.IOException;
import java.util.List;
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
                FzsEntry fzsEntry = null;
                try {
                    OpCode code = OpCode.from(data[ENTRY_TYPE_OFFSET] & 0xff);
                    switch (code) {
                        case INSERT:
                            fzsEntry = new FzsDmlIrp();
                            break;
                        case DELETE:
                            fzsEntry = new FzsDmlDrp();
                            break;
                        case UPDATE:
                            fzsEntry = new FzsDmlUrp();
                            break;
                        case MULIT_INSERT:
                            fzsEntry = new FzsDmlQmi();
                            break;
                        case MULIT_DELETE:
                            fzsEntry = new FzsDmlQmd();
                            break;
                        case COMMIT:
                            fzsEntry = new FzsTransCommit();
                            break;
                        case DDL:
                            fzsEntry = new FzsDdlEntryImpl();
                            break;
                        case UNSUPPORTED:
                            logger.warn("unsupport opcode: {}", data[ENTRY_TYPE_OFFSET] & 0xff);
                    }
                    if (fzsEntry != null) {
                        fzsEntry.parse(data);
                        switch (code) {
                            case MULIT_DELETE:
                                List<FzsDmlDrp> fzsDmlDrpList = ((FzsDmlQmd) fzsEntry).toDrpList();
                                for (FzsEntry entry : fzsDmlDrpList) {
                                    outQueue.put(entry);
                                }
                                break;
                            case MULIT_INSERT:
                                List<FzsDmlIrp> fzsDmlIrpList = ((FzsDmlQmi) fzsEntry).toIrpList();
                                for (FzsEntry entry : fzsDmlIrpList) {
                                    outQueue.put(entry);
                                }
                                break;
                            default:
                                outQueue.put(fzsEntry);
                                break;
                        }
                    }
                }
                catch (InterruptedException ignored) {
                    // do nothing
                }
                catch (IOException ioException) {
                    ioException.printStackTrace();
                    break;
                }
            }
            offset += dataLength + 4;
        }
    }
}
