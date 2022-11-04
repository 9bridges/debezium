/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.fzs.entry;

import java.io.IOException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class FzsTransCommit extends FzsDmlEntryImpl {

    @Override
    public void parse(byte[] data) throws IOException {
        ByteBuf byteBuf = Unpooled.wrappedBuffer(data, 0, data.length);
        setScn(byteBuf.readerIndex(5).readLong());
    }

    @Override
    public OpCode getEventType() {
        return OpCode.COMMIT;
    }
}
