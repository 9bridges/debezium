/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.fzs.entry;

import java.io.File;
import java.io.FileInputStream;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.Test;

public class SimpleFzsParserTest {
    public static byte[] file2byte(String path) {
        try {
            FileInputStream in = new FileInputStream(new File(path));
            byte[] data = new byte[in.available()];
            in.read(data);
            in.close();
            return data;
        }
        catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @Test
    public void SimpleFzsParserTest() {
        BlockingQueue<FzsEntry> recordQueue = new LinkedBlockingQueue<>(20000);
        byte[] bytes = file2byte("D:\\code\\debezium\\debezium-connector-oracle\\src\\test\\resources\\fzs\\4.fzs");
        SimpleFzsParser simpleFzsParser = new SimpleFzsParser();
        simpleFzsParser.parser(bytes, recordQueue);
        while (!recordQueue.isEmpty()) {
            System.out.println(recordQueue.poll().toString());
        }
    }
}
