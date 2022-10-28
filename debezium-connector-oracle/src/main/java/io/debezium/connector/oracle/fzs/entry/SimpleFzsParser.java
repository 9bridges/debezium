package io.debezium.connector.oracle.fzs.entry;

import java.util.List;
import java.util.concurrent.BlockingQueue;

public class SimpleFzsParser implements FzsParser {
    @Override
    public void parser(byte[] bytes, BlockingQueue<FzsEntry> outQueue) {
    }
}
