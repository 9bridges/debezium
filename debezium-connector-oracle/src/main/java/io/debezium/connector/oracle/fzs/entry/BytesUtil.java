/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.fzs.entry;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;

import io.netty.buffer.ByteBuf;

/**
 * @author 86138
 */
public final class BytesUtil {
    public static final int INT1024 = 1024;
    public static final int BIT32 = 32;
    public static final int BIT16 = 16;
    public static final int BIT8 = 8;
    public static final int BIT4 = 4;
    public static final int BIT2 = 2;
    public static final int BIT1 = 1;

    public static String size2UnitString(long size) {

        if (size > (long) Math.pow(INT1024, 5)) {
            return (size / (long) Math.pow(1024, 5)) + "PB";
        }
        if (size > (long) Math.pow(INT1024, 4)) {
            return (size / (long) Math.pow(INT1024, 4)) + "TB";
        }
        if (size > (long) Math.pow(INT1024, 3)) {
            return (size / (long) Math.pow(INT1024, 3)) + "GB";
        }
        if (size > (long) Math.pow(INT1024, 2)) {
            return (size / (long) Math.pow(INT1024, 2)) + "MB";
        }
        if (size > (long) Math.pow(INT1024, 1)) {
            return (size / (long) Math.pow(INT1024, 1)) + "KB";
        }
        return size + "B";

    }

    /*
     * short to byte[2]
     */
    public static byte[] getBytes(short data) {
        byte[] bytes = new byte[2];
        bytes[1] = (byte) ((data) & 0xff);
        bytes[0] = (byte) ((data >> 8) & 0xff);
        return bytes;
    }

    /*
     * char to byte[2]
     */
    public static byte[] getBytes(char data) {
        byte[] bytes = new byte[2];
        bytes[1] = (byte) ((data) & 0xff);
        bytes[0] = (byte) ((data >> 8) & 0xff);
        return bytes;
    }

    /*
     * int to byte[4]
     */
    public static byte[] getBytes(int data) {
        byte[] bytes = new byte[4];
        bytes[3] = (byte) ((data) & 0xff);
        bytes[2] = (byte) ((data >> 8) & 0xff);
        bytes[1] = (byte) ((data >> 16) & 0xff);
        bytes[0] = (byte) ((data >> 24) & 0xff);
        return bytes;
    }

    /*
     * long to byte[8]
     */
    public static byte[] getBytes(long data) {
        byte[] bytes = new byte[8];
        bytes[7] = (byte) (data & 0xff);
        bytes[6] = (byte) ((data >> 8) & 0xff);
        bytes[5] = (byte) ((data >> 16) & 0xff);
        bytes[4] = (byte) ((data >> 24) & 0xff);
        bytes[3] = (byte) ((data >> 32) & 0xff);
        bytes[2] = (byte) ((data >> 40) & 0xff);
        bytes[1] = (byte) ((data >> 48) & 0xff);
        bytes[0] = (byte) ((data >> 56) & 0xff);
        return bytes;
    }

    /*
     * float to byte[4]
     */
    public static byte[] getBytes(float data) {
        int intBits = Float.floatToIntBits(data);
        return getBytes(intBits);
    }

    /*
     * double to byte[8]
     */
    public static byte[] getBytes(double data) {
        long intBits = Double.doubleToLongBits(data);
        return getBytes(intBits);
    }

    /*
     * String Charset to byte[]
     */
    public static byte[] getBytes(String data, String charsetName) {
        Charset charset = Charset.forName(charsetName);
        return data.getBytes(charset);
    }

    /*
     * String gbk to byte[]
     */
    public static byte[] getBytes(String data) {
        return getBytes(data, "GBK");
    }

    /*
     * byte[2] to Short
     */
    public static short toShort(byte[] bytes) {
        short ret = 0;
        for (int i = 0; i < 2; i++) {
            byte b = bytes[i];
            ret |= ((short) b & 0xff) << 8 * (1 - i);
        }
        return ret;
    }

    /*
     * byte[2] to int,uShort
     */
    public static int toUnsignedShort(byte[] bytes) {
        return ((int) toShort(bytes)) & 0xffff;
    }

    /*
     * byte[2] to char
     */
    public static char toChar(byte[] bytes) {
        return (char) ((0xff & bytes[1])
                | (0xff00 & (bytes[0] << 8)));
    }

    /*
     * byte[4] to int
     */
    public static int toInt(byte[] bytes) {

        int ret = 0;
        for (int i = 0; i < 4; i++) {
            byte b = bytes[i];
            ret |= ((int) b & 0xff) << 8 * (3 - i);
        }
        return ret;
    }

    /*
     * byte[4] to long,uint
     */
    public static long toUnsignedInt(byte[] bytes) {
        return ((long) toInt(bytes)) & 0xffffffffL;
    }

    /*
     * byte[8] to long
     */
    public static long toLong(byte[] bytes) {
        long ret = 0;
        for (int i = 0; i < 8; i++) {
            byte b = bytes[i];
            ret |= ((long) b & 0xff) << 8 * (7 - i);
        }
        return ret;
    }

    public static float toFloat(byte[] bytes) {
        return Float.intBitsToFloat(toInt(bytes));
    }

    public static double toDouble(byte[] bytes) {
        long l = toLong(bytes);
        return Double.longBitsToDouble(l);
    }

    public static String toString(byte[] bytes, String charsetName) {
        return new String(bytes, Charset.forName(charsetName));
    }

    public static String toString(byte[] bytes) {
        String value = toString(bytes, "GBK");
        if (value.endsWith("\0")) {
            value = value.substring(0, value.length() - 1);
        }
        return value;
    }

    public static String toString(byte[] bytes, int offset, int len) {
        byte[] data = copyBytesByPos(bytes, offset, len);
        return toString(data);
    }

    public static String toHexString(byte[] bytes, int bit) {

        StringBuilder builder = new StringBuilder();
        for (int i = bit; i < bytes.length; i++) {
            if (i % 16 == 0) {
                builder.append('\n');
            }
            int b = (int) bytes[i] & 0xff;
            if (b <= 0xf) {
                builder.append("0");
            }
            builder.append(Integer.toHexString(b)).append(" ");

        }
        return builder.toString();
    }

    public static String showHexBytesAll(byte[] bytes) {

        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < bytes.length; i++) {
            if (i % 16 == 0) {
                builder.append('\n');
            }
            int b = (int) bytes[i] & 0xff;
            if (b <= 0xf) {
                builder.append("0");
            }
            builder.append(Integer.toHexString(b)).append(" ");

        }
        return builder.toString();
    }

    public static String toHexString(byte[] bytes) {

        StringBuilder builder = new StringBuilder();
        final int length = bytes.length;
        for (byte aByte : bytes) {

            int b = (int) aByte & 0xff;
            builder.append(Integer.toHexString(b));

        }
        return builder.toString();
    }

    public static String toHexString(byte[] bytes, int bit, int len) {

        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < Math.min(bytes.length, len); i++) {

            int b = (int) bytes[i] & 0xff;
            if (b <= 0xf) {
                builder.append("0");
            }
            builder.append(Integer.toHexString(b));

        }
        return builder.toString();
    }

    public static byte[] copyBytesByPos(byte[] data, int pos, int size) {
        byte[] bytes = new byte[size];
        System.arraycopy(data, pos, bytes, 0, bytes.length);
        return bytes;
    }

    public static byte[] readBytes(InputStream stream, int size) {
        byte[] bytes = new byte[size];
        try {
            stream.read(bytes, 0, size);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return bytes;

    }

    public static byte[] readBytes(ByteBuf byteBuf, int size) {
        byte[] t = new byte[size];
        System.arraycopy(byteBuf.array(), byteBuf.readerIndex(), t, 0, size);
        byteBuf.readerIndex(byteBuf.readerIndex() + size);
        return t;
    }

    public static byte[] readBytes(InputStream stream, int seek, int size) {
        stream.mark(seek);
        return readBytes(stream, size);
    }

    public static String getString(ByteBuf data) throws UnsupportedEncodingException {
        return getString(data, "UTF-8");
    }

    public static String getString(ByteBuf data, String encoding) throws UnsupportedEncodingException {
        int length = data.readByte();
        String rs = null;
        if (length > 0) {
            rs = new String(data.array(), data.readerIndex(), length - 1, encoding);
        }
        data.readerIndex(data.readerIndex() + length);
        return rs;
    }

    public static int getByteOrShort(ByteBuf byteBuf) {
        int val = byteBuf.readByte() & 0xff;
        if (val == 0xff) {
            val = byteBuf.readShort();
        }
        return val;
    }

    public static int getByteOrInt(ByteBuf byteBuf) {
        int val = byteBuf.readByte() & 0xff;
        if (val == 0xff) {
            val = byteBuf.readInt();
        }
        return val;
    }
}
