package io.debezium.connector.oracle.fzs.client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public final class BytesUtils {
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
