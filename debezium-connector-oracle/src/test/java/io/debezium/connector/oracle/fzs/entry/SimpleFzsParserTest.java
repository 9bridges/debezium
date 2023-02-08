/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.fzs.entry;

import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.Before;
import org.junit.Test;

public class SimpleFzsParserTest {
    // test table is create table fzs.test (tint int,tchar char(10),tvarchar2 varchar2(20));

    // insert into fzs.test values(123,'charval','varcharval');
    final byte[] INSERT = new byte[]{0, 0, 0, 55, 82, 0, 5, 0, 16, 0, 0, 4, 72, 0, 0, 0, -44, 0, 0, 0, 0, 0, 19, -67, -48, 0, 0, 0, 0, 0, 19, -67, -44, 0, 0, 67, 63, 85, -84, 67, 63, 85, -78, 0, 0, 0, 1, 0, 0, 0, 26, 0, 0, -7, -93, 0, 0, 0, 55, 0, 0, 0, -100, -78, 0, 1, 104, 55, 0, 0, 0, 0, 0, 19, -67, -48, 0, 1, 0, 5, 0, 16, 0, 0, 4, 72, 0, 1, 104, 55, 0, 1, 104, 55, 0, 0, 0, 0, 0, 19, -67, -48, 1, 0, 2, 35, 0, 0, 0, 0, 0, 0, 0, 0, 0, 44, 0, 1, 0, 0, 0, 0, 26, 4, 70, 90, 83, 0, 5, 84, 69, 83, 84, 0, 0, 0, 0, 0, 0, 99, -29, 24, -78, 3, 3, 49, 50, 51, 5, 84, 73, 78, 84, 0, 2, 51, 0, 0, 0, 0, 1, 10, 99, 104, 97, 114, 118, 97, 108, 32, 32, 32, 6, 84, 67, 72, 65, 82, 0, 96, 11, 0, 1, 3, 105, 0, 10, 118, 97, 114, 99, 104, 97, 114, 118, 97, 108, 10, 84, 86, 65, 82, 67, 72, 65, 82, 50, 0, 1, 21, 0, 1, 3, 105, 0, 0, 0, 0, -100, 0, 0, 0, 19, 84, 0, 0, 0, 0, 0, 19, -67, -44, 0, 0, 67, 63, 85, -78, 0, 0, 0, 19};
    // update fzs.test set tint=456,tchar='newval',tvarchar2='newvarcharval' where tint=123;
    final byte[] UPDATE = new byte[]{0, 0, 0, 55, 82, 0, 4, 0, 2, 0, 0, 3, -121, 0, 0, 1, 10, 0, 0, 0, 0, 0, 19, -67, -38, 0, 0, 0, 0, 0, 19, -67, -36, 0, 0, 67, 63, 85, -57, 67, 63, 85, -57, 0, 0, 0, 1, 0, 0, 0, 26, 0, 0, -7, -89, 0, 0, 0, 55, 0, 0, 0, -25, -75, 0, 1, 104, 55, 0, 0, 0, 0, 0, 19, -67, -38, 0, 1, 0, 4, 0, 2, 0, 0, 3, -121, 0, 1, 104, 55, 0, 1, 104, 55, 0, 0, 0, 0, 0, 19, -67, -38, 1, 0, 2, 35, 0, 0, 0, 0, 0, 0, 0, 0, 0, 44, 0, 0, 0, 0, 0, 0, 26, 4, 70, 90, 83, 0, 5, 84, 69, 83, 84, 0, 0, 0, 0, 0, 0, 99, -29, 24, -57, 3, 3, 1, 3, 52, 53, 54, 5, 84, 73, 78, 84, 0, 2, 51, 0, 0, 0, 1, 2, 10, 110, 101, 119, 118, 97, 108, 32, 32, 32, 32, 6, 84, 67, 72, 65, 82, 0, 96, 11, 1, 3, 105, 0, 3, 13, 110, 101, 119, 118, 97, 114, 99, 104, 97, 114, 118, 97, 108, 10, 84, 86, 65, 82, 67, 72, 65, 82, 50, 0, 1, 21, 1, 3, 105, 0, 44, 3, 1, 3, 49, 50, 51, 5, 84, 73, 78, 84, 0, 2, 51, 0, 0, 0, 2, 10, 99, 104, 97, 114, 118, 97, 108, 32, 32, 32, 6, 84, 67, 72, 65, 82, 0, 96, 11, 1, 3, 105, 3, 10, 118, 97, 114, 99, 104, 97, 114, 118, 97, 108, 10, 84, 86, 65, 82, 67, 72, 65, 82, 50, 0, 1, 21, 1, 3, 105, 0, 0, 0, 0, -25, 0, 0, 0, 19, 84, 0, 0, 0, 0, 0, 19, -67, -36, 0, 0, 67, 63, 85, -57, 0, 0, 0, 19};
    // delete from fzs.test where tint=456;
    final byte[] DELETE = new byte[]{0, 0, 0, 55, 82, 0, 1, 0, 19, 0, 0, 3, 103, 0, 0, 0, -41, 0, 0, 0, 0, 0, 19, -67, -17, 0, 0, 0, 0, 0, 19, -67, -16, 0, 0, 67, 63, 85, -28, 67, 63, 85, -28, 0, 0, 0, 1, 0, 0, 0, 26, 0, 0, -7, -74, 0, 0, 0, 55, 0, 0, 0, -97, -77, 0, 1, 104, 55, 0, 0, 0, 0, 0, 19, -67, -17, 0, 1, 0, 1, 0, 19, 0, 0, 3, 103, 0, 1, 104, 55, 0, 1, 104, 55, 0, 0, 0, 0, 0, 19, -67, -17, 1, 0, 2, 35, 0, 0, 0, 0, 0, 0, 0, 0, 0, 44, 0, 0, 0, 0, 0, 0, 26, 4, 70, 90, 83, 0, 5, 84, 69, 83, 84, 0, 0, 0, 0, 0, 0, 99, -29, 24, -28, 3, 3, 52, 53, 54, 5, 84, 73, 78, 84, 0, 2, 51, 0, 0, 0, 0, 0, 10, 110, 101, 119, 118, 97, 108, 32, 32, 32, 32, 6, 84, 67, 72, 65, 82, 0, 96, 11, 0, 1, 3, 105, 0, 13, 110, 101, 119, 118, 97, 114, 99, 104, 97, 114, 118, 97, 108, 10, 84, 86, 65, 82, 67, 72, 65, 82, 50, 0, 1, 21, 0, 1, 3, 105, 0, 0, 0, 0, -97, 0, 0, 0, 19, 84, 0, 0, 0, 0, 0, 19, -67, -16, 0, 0, 67, 63, 85, -28, 0, 0, 0, 19};
    /* insert into fzs.test values(1,'col1','col2');
       insert into fzs.test values(2,'col1','col2');
       insert into fzs.test values(3,'col1','col2');
       insert into fzs.test values(4,'col1','col2');
       insert into fzs.test values(5,'col1','col2');
     */
    final byte[] MULIT_INSERT = new byte[]{0, 0, 0, 55, 82, 0, 9, 0, 12, 0, 0, 4, 90, 0, 0, 1, 38, 0, 0, 0, 0, 0, 19, -67, -5, 0, 0, 0, 0, 0, 19, -67, -3, 0, 0, 67, 63, 86, 6, 67, 63, 86, 8, 0, 0, 0, 1, 0, 0, 0, 26, 0, 0, -7, -69, 0, 0, 0, 55, 0, 0, 0, -23, -69, 0, 1, 104, 55, 0, 0, 0, 0, 0, 19, -67, -5, 0, 1, 0, 9, 0, 12, 0, 0, 4, 90, 0, 1, 104, 55, 0, 1, 104, 55, 0, 0, 0, 0, 0, 19, -67, -5, 1, 0, 2, 35, 0, 0, 0, 0, 26, 4, 70, 90, 83, 0, 5, 84, 69, 83, 84, 0, 0, 0, 0, 0, 0, 0, 99, -29, 25, 8, 0, 0, 0, 0, 5, 1, 0, 44, 3, 1, 49, 5, 84, 73, 78, 84, 0, 2, 51, 0, 0, 0, 0, 1, 10, 99, 111, 108, 49, 32, 32, 32, 32, 32, 32, 6, 84, 67, 72, 65, 82, 0, 96, 11, 0, 1, 3, 105, 0, 4, 99, 111, 108, 50, 10, 84, 86, 65, 82, 67, 72, 65, 82, 50, 0, 1, 21, 0, 1, 3, 105, 0, 2, 0, 44, 3, 1, 50, 10, 99, 111, 108, 49, 32, 32, 32, 32, 32, 32, 4, 99, 111, 108, 50, 3, 0, 44, 3, 1, 51, 10, 99, 111, 108, 49, 32, 32, 32, 32, 32, 32, 4, 99, 111, 108, 50, 4, 0, 44, 3, 1, 52, 10, 99, 111, 108, 49, 32, 32, 32, 32, 32, 32, 4, 99, 111, 108, 50, 5, 0, 44, 3, 1, 53, 10, 99, 111, 108, 49, 32, 32, 32, 32, 32, 32, 4, 99, 111, 108, 50, 0, 0, 0, -23, 0, 0, 0, 19, 84, 0, 0, 0, 0, 0, 19, -67, -3, 0, 0, 67, 63, 86, 8, 0, 0, 0, 19};
    final int COLUMN_COUNT = 3;
    SimpleFzsParser simpleFzsParser = new SimpleFzsParser();
    BlockingQueue<FzsEntry> recordQueue = new LinkedBlockingQueue<>();
    String objectOwner = "FZS";
    String objectName = "TEST";
    String[] columnNames = new String[]{"TINT", "TCHAR", "TVARCHAR2"};
    String[] beforeValues = new String[]{"123", "charval   ", "varcharval"};
    String[] afterValues = new String[]{"456", "newval    ", "newvarcharval"};
    String[][] mulitValues = new String[5][COLUMN_COUNT];

    @Before
    public void initMulitValues() {
        for (int i = 0; i < mulitValues.length; i++) {
            mulitValues[i] = new String[]{Integer.toString(i + 1), "col1      ", "col2"};
        }
    }

    @Test
    public void UpdateTest() {
        simpleFzsParser.parser(UPDATE, recordQueue);
        assert !recordQueue.isEmpty();
        FzsEntry fzsEntry = recordQueue.poll();
        assert fzsEntry.getEventType().equals(OpCode.UPDATE);
        FzsDmlUrp fzsDmlUrp = (FzsDmlUrp) fzsEntry;
        assert fzsDmlUrp.getObjectOwner().equals(objectOwner);
        assert fzsDmlUrp.getObjectName().equals(objectName);
        String[] parseOldColumnNames = fzsDmlUrp.getOldColumnNames();
        Object[] parseOldValues = fzsDmlUrp.getOldValues();
        String[] parseNewColumnNames = fzsDmlUrp.getNewColumnNames();
        Object[] parseNewValues = fzsDmlUrp.getNewValues();
        assert parseOldColumnNames.length == COLUMN_COUNT;
        assert parseNewColumnNames.length == COLUMN_COUNT;
        for (int i = 0; i < COLUMN_COUNT; i++) {
            assert parseOldColumnNames[i].equals(columnNames[i]);
            assert parseOldValues[i].equals(beforeValues[i]);
            assert parseNewColumnNames[i].equals(columnNames[i]);
            assert parseNewValues[i].equals(afterValues[i]);
        }
    }

    @Test
    public void InsertTest() {
        simpleFzsParser.parser(INSERT, recordQueue);
        assert !recordQueue.isEmpty();
        FzsEntry fzsEntry = recordQueue.poll();
        assert fzsEntry.getEventType().equals(OpCode.INSERT);
        FzsDmlIrp fzsDmlIrp = (FzsDmlIrp) fzsEntry;
        assert fzsDmlIrp.getObjectOwner().equals(objectOwner);
        assert fzsDmlIrp.getObjectName().equals(objectName);
        String[] parseColumnNames = fzsDmlIrp.getNewColumnNames();
        Object[] parseValues = fzsDmlIrp.getNewValues();
        assert parseColumnNames.length == COLUMN_COUNT;
        for (int i = 0; i < COLUMN_COUNT; i++) {
            assert parseColumnNames[i].equals(columnNames[i]);
            assert parseValues[i].equals(beforeValues[i]);
        }
    }

    @Test
    public void DeleteTest() {
        simpleFzsParser.parser(DELETE, recordQueue);
        assert !recordQueue.isEmpty();
        FzsEntry fzsEntry = recordQueue.poll();
        assert fzsEntry.getEventType().equals(OpCode.DELETE);
        FzsDmlDrp fzsDmlDrp = (FzsDmlDrp) fzsEntry;
        assert fzsDmlDrp.getObjectOwner().equals(objectOwner);
        assert fzsDmlDrp.getObjectName().equals(objectName);
        String[] parseColumnNames = fzsDmlDrp.getOldColumnNames();
        Object[] parseValues = fzsDmlDrp.getOldValues();
        assert parseColumnNames.length == COLUMN_COUNT;
        for (int i = 0; i < COLUMN_COUNT; i++) {
            assert parseColumnNames[i].equals(columnNames[i]);
            assert parseValues[i].equals(afterValues[i]);
        }
    }

    @Test
    public void MulitInsertTest() {
        simpleFzsParser.parser(MULIT_INSERT, recordQueue);
        assert recordQueue.size() == 6;
        for (int row = 0; row < 5; row++) {
            FzsEntry fzsEntry = recordQueue.poll();
            assert Objects.requireNonNull(fzsEntry).getEventType().equals(OpCode.INSERT);
            FzsDmlIrp fzsDmlIrp = (FzsDmlIrp) fzsEntry;
            assert fzsDmlIrp.getObjectOwner().equals(objectOwner);
            assert fzsDmlIrp.getObjectName().equals(objectName);
            String[] parseColumnNames = fzsDmlIrp.getNewColumnNames();
            Object[] parseValues = fzsDmlIrp.getNewValues();
            assert parseColumnNames.length == COLUMN_COUNT;
            for (int col = 0; col < COLUMN_COUNT; col++) {
                assert parseColumnNames[col].equals(columnNames[col]);
                assert parseValues[col].equals(mulitValues[row][col]);
            }
        }
    }
}
