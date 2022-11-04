/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.fzs.entry;

import java.time.Instant;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class FzsDmlEntryImpl implements FzsDmlEntry {
    private Logger logger = LoggerFactory.getLogger(FzsDmlEntryImpl.class);
    private Object[] newValues = null;
    private Object[] oldValues = null;
    private String objectOwner = null;
    private String objectName = null;
    private Instant sourceTime;
    private String[] newColumNames;
    private String[] oldColumnNames;

    private int[] newColumnTypes;
    private int[] oldColumntypes;
    private String dataBaseName;
    private long scn = -1;
    private short subScn = -1;
    private String transactionId;
    protected static final int OP_SIZE_LEN = 4;
    protected static final int OP_CODE_LEN = 1;
    protected static final int OBJECT_ID_LEN = 4;
    protected static final int SCN_LEN = 8;
    protected static final int SUB_SCN_LEN = 2;
    protected static final int TRANS_ID_LEN = 8;
    protected static final int OBJECT_PART_ID_LEN = 4;
    protected static final int OBJECT_DATA_ID_LEN = 4;
    protected static final int OP_SIZE_OFFSET = 0;
    protected static final int OP_CODE_OFFSET = OP_SIZE_OFFSET + OP_SIZE_LEN;
    protected static final int OBJECT_ID_OFFSET = OP_CODE_OFFSET + OP_CODE_LEN;
    protected static final int SCN_OFFSET = OBJECT_ID_OFFSET + OBJECT_ID_LEN;
    protected static final int SUB_SCN_OFFSET = SCN_OFFSET + SCN_LEN;
    protected static final int TRANS_ID_OFFSET = SUB_SCN_OFFSET + SUB_SCN_LEN;

    public void setOldValues(Object[] var1) {
        oldValues = var1;
    }

    public void setNewValues(Object[] var1) {
        newValues = var1;
    }

    public void setOldColumnNames(String[] columnNames) {
        oldColumnNames = columnNames;

    }

    public void setNewColumnNames(String[] columNames) {
        newColumNames = columNames;
    }

    @Override
    public Object[] getOldValues() {
        return oldValues;
    }

    @Override
    public Object[] getNewValues() {
        return newValues;
    }

    public String[] getOldColumnNames() {
        return oldColumnNames;
    }

    public String[] getNewColumnNames() {
        return newColumNames;
    }

    public void setDatabaseName(String var1) {
        dataBaseName = var1;
    }

    public void setObjectName(String name) {
        objectName = name;
    }

    public void setObjectOwner(String name) {
        objectOwner = name;
    }

    public void setSourceTime(Instant var1) {
        sourceTime = var1;
    }

    public void setScn(long scn) {
        this.scn = scn;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    @Override
    public String getObjectName() {
        return objectName;
    }

    @Override
    public String getObjectOwner() {
        return objectOwner;
    }

    @Override
    public Instant getSourceTime() {
        return sourceTime;
    }

    @Override
    public long getScn() {
        return this.scn;
    }

    @Override
    public String getTransactionId() {
        return transactionId;
    }

    @Override
    public String getDatabaseName() {
        return "LHR11G";
    }

    public void setNewColumnTypes(int[] newColumnTypes) {
        this.newColumnTypes = newColumnTypes;
    }

    public void setOldColumnTypes(int[] oldColumntypes) {
        this.oldColumntypes = oldColumntypes;
    }

    @Override
    public int[] getNewColumnTypes() {
        return newColumnTypes;
    }

    @Override
    public int[] getOldColumntypes() {
        return oldColumntypes;
    }

    @Override
    public String toString() {
        return "FzsDmlEntryImpl{" +
                "eventType=" + getEventType() +
                ", newValues=" + Arrays.toString(newValues) +
                ", oldValues=" + Arrays.toString(oldValues) +
                ", objectOwner='" + objectOwner + '\'' +
                ", objectName='" + objectName + '\'' +
                ", sourceTime=" + sourceTime +
                ", newColumNames=" + Arrays.toString(newColumNames) +
                ", oldColumnNames=" + Arrays.toString(oldColumnNames) +
                ", newColumnTypes=" + Arrays.toString(newColumnTypes) +
                ", oldColumntypes=" + Arrays.toString(oldColumntypes) +
                ", dataBaseName='" + getDatabaseName() + '\'' +
                ", scn=" + scn +
                ", subScn=" + subScn +
                ", transactionId='" + transactionId + '\'' +
                '}';
    }
}
