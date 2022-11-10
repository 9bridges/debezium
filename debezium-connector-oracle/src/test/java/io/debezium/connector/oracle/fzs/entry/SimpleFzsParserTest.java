/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.fzs.entry;

import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.watch.SimpleWatcher;
import cn.hutool.core.io.watch.WatchMonitor;
import cn.hutool.core.lang.Console;
import org.junit.Test;

public class SimpleFzsParserTest {
    public static byte[] file2byte(String path) {
        try {
            FileInputStream in = new FileInputStream(path);
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

        String filePath = "D:\\fzs\\dmp_test\\run\\fzs_src\\1\\0\\";
        File file = FileUtil.file(filePath);
        SimpleFzsParser simpleFzsParser = new SimpleFzsParser();
        WatchMonitor.createAll(file, new SimpleWatcher() {
            @Override
            public void onCreate(WatchEvent<?> event, Path currentPath) {
                Object obj = event.context();
                Console.log("create {}-> {}", currentPath, obj);
            }

            @Override
            public void onModify(WatchEvent<?> event, Path currentPath) {
                Object obj = event.context();
                Console.log("modify {}-> {}", currentPath, obj);
                byte[] bytes = file2byte(filePath + obj);
                simpleFzsParser.parser(bytes, recordQueue);
                while (!recordQueue.isEmpty()) {
                    System.out.println(recordQueue.poll().toString());
                }
            }

            @Override
            public void onDelete(WatchEvent<?> event, Path currentPath) {
                Object obj = event.context();
                Console.log("delete {}-> {}", currentPath, obj);
            }
        }).start();
        System.out.println("over");
        while (true) {}
    }
}
