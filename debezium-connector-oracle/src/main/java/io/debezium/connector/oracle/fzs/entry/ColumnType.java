/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.fzs.entry;

public enum ColumnType {
    // those not all types, only define byte or lob value in fzs
    FZS_CLOB(112),
    FZS_BLOB(113),
    FZS_RAW(23),
    FZS_LONGRAW(24),
    FZS_RAW2(95),
    FZS_BYTE1(105),
    FZS_BYTE2(106),
    FZS_BYTE3(111),
    FZS_BFILE(114),
    FZS_BYTE5(115),
    UNSUPPORTED(255);

    private static final ColumnType[] types = new ColumnType[256];

    static {
        for (ColumnType option : ColumnType.values()) {
            types[option.getValue()] = option;
        }
    }

    private final int value;

    ColumnType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static ColumnType from(int value) {
        return value < types.length ? types[value] : ColumnType.UNSUPPORTED;
    }

}