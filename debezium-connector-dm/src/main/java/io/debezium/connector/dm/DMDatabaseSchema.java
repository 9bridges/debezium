/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.dm.StreamingAdapter.TableNameCaseSensitivity;
import io.debezium.connector.dm.antlr.DMDdlParser;
import io.debezium.relational.HistorizedRelationalDatabaseSchema;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.relational.Tables;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.TopicSelector;
import io.debezium.util.SchemaNameAdjuster;

/**
 * The schema of an DM database.
 *
 * @author Gunnar Morling
 */
public class DMDatabaseSchema extends HistorizedRelationalDatabaseSchema {

    private static final Logger LOGGER = LoggerFactory.getLogger(DMDatabaseSchema.class);

    private final DMDdlParser ddlParser;
    private final DMValueConverters valueConverters;

    public DMDatabaseSchema(DMConnectorConfig connectorConfig, DMValueConverters valueConverters,
                            SchemaNameAdjuster schemaNameAdjuster, TopicSelector<TableId> topicSelector,
                            TableNameCaseSensitivity tableNameCaseSensitivity) {
        super(connectorConfig, topicSelector, connectorConfig.getTableFilters().dataCollectionFilter(),
                connectorConfig.getColumnFilter(),
                new TableSchemaBuilder(
                        valueConverters,
                        schemaNameAdjuster,
                        connectorConfig.customConverterRegistry(),
                        connectorConfig.getSourceInfoStructMaker().schema(),
                        connectorConfig.getSanitizeFieldNames()),
                TableNameCaseSensitivity.INSENSITIVE.equals(tableNameCaseSensitivity),
                connectorConfig.getKeyMapper());

        this.ddlParser = new DMDdlParser(valueConverters, connectorConfig.getTableFilters().dataCollectionFilter());
        this.valueConverters = valueConverters;
    }

    public Tables getTables() {
        return tables();
    }

    public DMValueConverters getValueConverters() {
        return valueConverters;
    }

    @Override
    public DMDdlParser getDdlParser() {
        return ddlParser;
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent schemaChange) {
        LOGGER.debug("Applying schema change event {}", schemaChange);

        switch (schemaChange.getType()) {
            case CREATE:
            case ALTER:
                schemaChange.getTableChanges().forEach(x -> {
                    buildAndRegisterSchema(x.getTable());
                    tables().overwriteTable(x.getTable());
                });
                break;
            case DROP:
                schemaChange.getTableChanges().forEach(x -> removeSchema(x.getId()));
                break;
            default:
        }

        if (schemaChange.getTables().stream().map(Table::id).anyMatch(getTableFilter()::isIncluded)) {
            LOGGER.debug("Recorded DDL statements for database '{}': {}", schemaChange.getDatabase(), schemaChange.getDdl());
            record(schemaChange, schemaChange.getTableChanges());
        }
    }
}
