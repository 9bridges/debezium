/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dm;

import io.debezium.connector.common.CdcSourceTaskContext;

public class DMTaskContext extends CdcSourceTaskContext {

    public DMTaskContext(DMConnectorConfig config, DMDatabaseSchema schema) {
        super(config.getContextName(), config.getLogicalName(), schema::tableIds);
    }
}
