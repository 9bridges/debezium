/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.fzs.entry;

import java.time.Instant;

public class FzsDdlEntryImpl implements FzsDdlEntry {
    public FzsDdlEntryImpl() {
    }

    public FzsDdlEntryImpl parse(byte[] bytes) {
        return this;
    }

    @Override
    public void setDDLString(String ddlString) {

    }

    @Override
    public void setObjectType(String objectType) {

    }

    @Override
    public String getDDLString() {
        return null;
    }

    @Override
    public String getObjectType() {
        return null;
    }

    @Override
    public void setDatabaseName(String var1) {

    }

    @Override
    public void setObjectName(String name) {

    }

    @Override
    public void setObjectOwner(String name) {

    }

    @Override
    public void setSourceTime(String var1) {

    }

    @Override
    public void setScn(long scn) {

    }

    @Override
    public void setTransactionId(String var1) {

    }

    @Override
    public void setEventType(OpCode var1) {

    }

    @Override
    public String getObjectName() {
        return null;
    }

    @Override
    public String getObjectOwner() {
        return null;
    }

    @Override
    public Instant getSourceTime() {
        return null;
    }

    @Override
    public long getScn() {
        return 0;
    }

    @Override
    public String getTransactionId() {
        return null;
    }

    @Override
    public OpCode getEventType() {
        return null;
    }

    @Override
    public String getDatabaseName() {
        return null;
    }
}
