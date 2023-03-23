/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dm;

import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;

public class DMTopicSelector {

    public static TopicSelector<TableId> defaultSelector(DMConnectorConfig connectorConfig) {
        return TopicSelector.defaultSelector(connectorConfig,
                (tableId, prefix, delimiter) -> String.join(delimiter, prefix, tableId.schema(), tableId.table()));
    }
}
