/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.fzs.client;

import java.io.*;
import java.util.zip.InflaterInputStream;

public final class BytesUtils {

    public static byte[] decompress(byte[] data) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        InflaterInputStream iis = new InflaterInputStream(bais);
        ByteArrayOutputStream baos = new ByteArrayOutputStream(4096);
        byte[] buf = new byte[4096];
        int readSize = 0;
        while ((readSize = iis.read(buf, 0, 4096)) > 0) {
            baos.write(buf, 0, readSize);
        }
        baos.flush();
        bais.close();
        return baos.toByteArray();
    }

    public static int readBytes(InputStream inputStream, byte[] content, int size) throws IOException {
        int read;
        int readed = 0;
        while (readed < size) {
            read = inputStream.read(content, readed, size - readed);
            readed += read;
        }
        return readed;
    }

    public static int readBytes1(InputStream inputStream, byte[] content, int size) throws IOException {
        int read = 0;
        int readed = 0;
        while (readed < size) {
            read = inputStream.read(content, readed, size - readed);
            readed += read;
        }
        return readed;
    }


    public static void writeBytes(OutputStream outputStream, byte[] content, int offset, int size) throws IOException {
        outputStream.write(content, offset, size);
        outputStream.flush();
    }
}
