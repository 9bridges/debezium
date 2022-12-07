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
    final byte[] INSERT = new byte[]{0, 0, 0, 55, 82, 0, 5, 0, 20, 0, 20, 123, -70, 0, 0, 0, -52, 0, 0, 0, 6, 80, 10, 122, -42, 0, 0, 0, 6, 80, 10, 122, -40, 0, 0, 66, -20, -80, -96, 66, -20, -80, -94, 0, 0, 0, 1, 0, 0, 104, 90, 0, 4, -8, -63, 0, 0, 0, 55, 0, 0, 0, -108, -78, 0, 35, -50, 7, 0, 0, 0, 6, 80, 10, 122, -42, 0, 1, 0, 5, 0, 20, 0, 20, 123, -70, 0, 35, -50, 7, 0, 35, -50, 7, 0, 0, 0, 6, 80, 10, 122, -42, 3, 64, -113, 60, 0, 0, 0, 0, 0, 0, 0, 0, 0, 44, 0, 1, 0, 0, 0, 104, 90, 4, 70, 90, 83, 0, 5, 84, 69, 83, 84, 0, 0, 3, 3, 49, 50, 51, 5, 84, 73, 78, 84, 0, 2, 51, 0, 0, 0, 0, 0, 10, 99, 104, 97, 114, 118, 97, 108, 32, 32, 32, 6, 84, 67, 72, 65, 82, 0, 96, 11, 0, 1, 3, 103, 0, 10, 118, 97, 114, 99, 104, 97, 114, 118, 97, 108, 10, 84, 86, 65, 82, 67, 72, 65, 82, 50, 0, 1, 21, 0, 1, 3, 103, 0, 0, 0, 0, -108, 0, 0, 0, 19, 84, 0, 0, 0, 6, 80, 10, 122, -40, 0, 0, 66, -20, -80, -94, 0, 0, 0, 19};

    // update fzs.test set tint=456,tchar='newval',tvarchar2='newvarcharval' where tint=123;
    final byte[] UPDATE = new byte[]{0, 0, 0, 55, 82, 0, 4, 0, 11, 0, 19, -9, -7, 0, 0, 1, 2, 0, 0, 0, 6, 80, 10, 122, -18, 0, 0, 0, 6, 80, 10, 122, -16, 0, 0, 66, -20, -80, -30, 66, -20, -80, -28, 0, 0, 0, 1, 0, 0, 104, 90, 0, 4, -8, -60, 0, 0, 0, 55, 0, 0, 0, -33, -75, 0, 35, -50, 7, 0, 0, 0, 6, 80, 10, 122, -18, 0, 1, 0, 4, 0, 11, 0, 19, -9, -7, 0, 35, -50, 7, 0, 35, -50, 7, 0, 0, 0, 6, 80, 10, 122, -18, 3, 64, -113, 60, 0, 0, 0, 0, 0, 0, 0, 0, 0, 44, 44, 0, 0, 0, 0, 0, 104, 90, 4, 70, 90, 83, 0, 5, 84, 69, 83, 84, 0, 0, 3, 3, 1, 3, 52, 53, 54, 5, 84, 73, 78, 84, 0, 2, 51, 0, 0, 0, 0, 2, 10, 110, 101, 119, 118, 97, 108, 32, 32, 32, 32, 6, 84, 67, 72, 65, 82, 0, 96, 11, 1, 3, 103, 0, 3, 13, 110, 101, 119, 118, 97, 114, 99, 104, 97, 114, 118, 97, 108, 10, 84, 86, 65, 82, 67, 72, 65, 82, 50, 0, 1, 21, 1, 3, 103, 0, 3, 1, 3, 49, 50, 51, 5, 84, 73, 78, 84, 0, 2, 51, 0, 0, 0, 2, 10, 99, 104, 97, 114, 118, 97, 108, 32, 32, 32, 6, 84, 67, 72, 65, 82, 0, 96, 11, 1, 3, 103, 3, 10, 118, 97, 114, 99, 104, 97, 114, 118, 97, 108, 10, 84, 86, 65, 82, 67, 72, 65, 82, 50, 0, 1, 21, 1, 3, 103, 0, 0, 0, 0, -33, 0, 0, 0, 19, 84, 0, 0, 0, 6, 80, 10, 122, -16, 0, 0, 66, -20, -80, -28, 0, 0, 0, 19};

    // delete from fzs.test where tint=456;
    final byte[] DELETE = new byte[]{0, 0, 0, 55, 82, 0, 6, 0, 8, 0, 14, -21, -38, 0, 0, 0, -49, 0, 0, 0, 6, 80, 10, 123, 1, 0, 0, 0, 6, 80, 10, 123, 3, 0, 0, 66, -20, -79, 21, 66, -20, -79, 22, 0, 0, 0, 1, 0, 0, 104, 90, 0, 4, -8, -56, 0, 0, 0, 55, 0, 0, 0, -105, -77, 0, 35, -50, 7, 0, 0, 0, 6, 80, 10, 123, 1, 0, 1, 0, 6, 0, 8, 0, 14, -21, -38, 0, 35, -50, 7, 0, 35, -50, 7, 0, 0, 0, 6, 80, 10, 123, 1, 3, 64, -113, 60, 0, 0, 0, 0, 0, 0, 0, 0, 0, 44, 0, 0, 0, 0, 0, 104, 90, 4, 70, 90, 83, 0, 5, 84, 69, 83, 84, 0, 0, 3, 3, 52, 53, 54, 5, 84, 73, 78, 84, 0, 2, 51, 0, 0, 0, 0, 0, 10, 110, 101, 119, 118, 97, 108, 32, 32, 32, 32, 6, 84, 67, 72, 65, 82, 0, 96, 11, 0, 1, 3, 103, 0, 13, 110, 101, 119, 118, 97, 114, 99, 104, 97, 114, 118, 97, 108, 10, 84, 86, 65, 82, 67, 72, 65, 82, 50, 0, 1, 21, 0, 1, 3, 103, 0, 0, 0, 0, -105, 0, 0, 0, 19, 84, 0, 0, 0, 6, 80, 10, 123, 3, 0, 0, 66, -20, -79, 22, 0, 0, 0, 19};

    /* insert into fzs.test values(1,'col1','col2');
       insert into fzs.test values(2,'col1','col2');
       insert into fzs.test values(3,'col1','col2');
       insert into fzs.test values(4,'col1','col2');
       insert into fzs.test values(5,'col1','col2');
     */
    final byte[] MULIT_INSERT = new byte[]{0, 0, 0, 55, 82, 0, 1, 0, 27, 0, 14, -24, -72, 0, 0, 1, 30, 0, 0, 0, 6, 80, 10, 124, 50, 0, 0, 0, 6, 80, 10, 124, 52, 0, 0, 66, -20, -79, -52, 66, -20, -79, -51, 0, 0, 0, 1, 0, 0, 104, 90, 0, 4, -6, 90, 0, 0, 0, 55, 0, 0, 0, -31, -69, 0, 35, -50, 7, 0, 0, 0, 6, 80, 10, 124, 50, 0, 1, 0, 1, 0, 27, 0, 14, -24, -72, 0, 35, -50, 7, 0, 35, -50, 7, 0, 0, 0, 6, 80, 10, 124, 50, 3, 64, -113, 60, 0, 0, 0, 104, 90, 4, 70, 90, 83, 0, 5, 84, 69, 83, 84, 0, 0, 0, 0, 0, 0, 0, 5, 1, 0, 44, 3, 1, 49, 5, 84, 73, 78, 84, 0, 2, 51, 0, 0, 0, 0, 0, 10, 99, 111, 108, 49, 32, 32, 32, 32, 32, 32, 6, 84, 67, 72, 65, 82, 0, 96, 11, 0, 1, 3, 103, 0, 4, 99, 111, 108, 50, 10, 84, 86, 65, 82, 67, 72, 65, 82, 50, 0, 1, 21, 0, 1, 3, 103, 0, 2, 0, 44, 3, 1, 50, 10, 99, 111, 108, 49, 32, 32, 32, 32, 32, 32, 4, 99, 111, 108, 50, 3, 0, 44, 3, 1, 51, 10, 99, 111, 108, 49, 32, 32, 32, 32, 32, 32, 4, 99, 111, 108, 50, 4, 0, 44, 3, 1, 52, 10, 99, 111, 108, 49, 32, 32, 32, 32, 32, 32, 4, 99, 111, 108, 50, 5, 0, 44, 3, 1, 53, 10, 99, 111, 108, 49, 32, 32, 32, 32, 32, 32, 4, 99, 111, 108, 50, 0, 0, 0, -31, 0, 0, 0, 19, 84, 0, 0, 0, 6, 80, 10, 124, 52, 0, 0, 66, -20, -79, -51, 0, 0, 0, 19};
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
