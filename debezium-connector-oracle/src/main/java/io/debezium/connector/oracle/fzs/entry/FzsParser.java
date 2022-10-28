package io.debezium.connector.oracle.fzs.entry;

import java.util.List;
import java.util.concurrent.BlockingQueue;

public interface FzsParser {
    void parser(byte[] bytes, BlockingQueue<FzsEntry> outQueue);
}
