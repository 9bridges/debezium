/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dm;

import static io.debezium.connector.dm.DMConnectorConfig.GENERATED_PK_NAME;

import java.sql.Clob;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.dm.DMConnectorConfig.ConnectorAdapter;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.Tables.ColumnNameFilter;
import io.debezium.relational.Tables.TableFilter;

import oracle.jdbc.OracleTypes;

public class DMConnection extends JdbcConnection {

    private final static Logger LOGGER = LoggerFactory.getLogger(DMConnection.class);

    /**
     * Returned by column metadata in DM if no scale is set;
     */
    private static final int ORACLE_UNSET_SCALE = -127;

    /**
     * Pattern to identify system generated indices and column names.
     */
    private static final Pattern SYS_NC_PATTERN = Pattern.compile("^SYS_NC(?:_OID|_ROWINFO|[0-9][0-9][0-9][0-9][0-9])\\$$");

    /**
     * A field for the raw jdbc url. This field has no default value.
     */
    private static final Field URL = Field.create("url", "Raw JDBC url");

    /**
     * The database version.
     */
    private final DMDatabaseVersion databaseVersion = null;

    public DMConnection(Configuration config, Supplier<ClassLoader> classLoaderSupplier) {
        super(config, resolveConnectionFactory(config), classLoaderSupplier);

        /*
         * this.databaseVersion = resolveDMDatabaseVersion();
         * LOGGER.info("Database Version: {}", databaseVersion.getBanner());
         */
    }

    public void setSessionToPdb(String pdbName) {
        Statement statement = null;

        try {
            statement = connection().createStatement();
            statement.execute("alter session set container=" + pdbName);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
        finally {
            if (statement != null) {
                try {
                    statement.close();
                }
                catch (SQLException e) {
                    LOGGER.error("Couldn't close statement", e);
                }
            }
        }
    }

    public void resetSessionToCdb() {
        Statement statement = null;

        try {
            statement = connection().createStatement();
            statement.execute("alter session set container=cdb$root");
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
        finally {
            if (statement != null) {
                try {
                    statement.close();
                }
                catch (SQLException e) {
                    LOGGER.error("Couldn't close statement", e);
                }
            }
        }
    }

    public DMDatabaseVersion getDMVersion() {
        return databaseVersion;
    }

    private DMDatabaseVersion resolveDMDatabaseVersion() {
        String versionStr;
        try {
            try {
                // DM 18.1 introduced BANNER_FULL as the new column rather than BANNER
                // This column uses a different format than the legacy BANNER.
                versionStr = queryAndMap("SELECT BANNER_FULL FROM V$VERSION WHERE BANNER_FULL LIKE 'DM Database%'", (rs) -> {
                    if (rs.next()) {
                        return rs.getString(1);
                    }
                    return null;
                });
            }
            catch (SQLException e) {
                // exception ignored
                if (e.getMessage().contains("BANNER_FULL")) {
                    LOGGER.debug("BANNER_FULL column not in V$VERSION, using BANNER column as fallback");
                    versionStr = null;
                }
                else {
                    throw e;
                }
            }

            // For databases prior to 18.1, a SQLException will be thrown due to BANNER_FULL not being a column and
            // this will cause versionStr to remain null, use fallback column BANNER for versions prior to 18.1.
            if (versionStr == null) {
                versionStr = queryAndMap("SELECT BANNER FROM V$VERSION WHERE BANNER LIKE 'DM Database%'", (rs) -> {
                    if (rs.next()) {
                        return rs.getString(1);
                    }
                    return null;
                });
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to resolve DM database version", e);
        }

        if (versionStr == null) {
            throw new RuntimeException("Failed to resolve DM database version");
        }

        return DMDatabaseVersion.parse(versionStr);
    }

    @Override
    public Set<TableId> readTableNames(String databaseCatalog, String schemaNamePattern, String tableNamePattern,
                                       String[] tableTypes)
            throws SQLException {

        Set<TableId> tableIds = super.readTableNames(null, schemaNamePattern, tableNamePattern, tableTypes);

        return tableIds.stream()
                .map(t -> new TableId(databaseCatalog, t.schema(), t.table()))
                .collect(Collectors.toSet());
    }

    /**
     * Retrieves all {@code TableId} in a given database catalog, filtering certain ids that
     * should be omitted from the returned set such as special spatial tables and index-organized
     * tables.
     *
     * @param catalogName the catalog/database name
     * @return set of all table ids for existing table objects
     * @throws SQLException if a database exception occurred
     */
    protected Set<TableId> getAllTableIds(String catalogName) throws SQLException {
        final String query = "select owner, table_name from all_tables " +
        // filter special spatial tables
                "where table_name NOT LIKE 'MDRT_%' " +
                "and table_name NOT LIKE 'MDRS_%' " +
                "and table_name NOT LIKE 'MDXT_%' ";

        Set<TableId> tableIds = new LinkedHashSet<>();
        query(query, (rs) -> {
            while (rs.next()) {
                tableIds.add(new TableId(catalogName, rs.getString(1), rs.getString(2)));
            }
            LOGGER.trace("TableIds are: {}", tableIds);
        });

        return tableIds;
    }

    // todo replace metadata with something like this
    private ResultSet getTableColumnsInfo(String schemaNamePattern, String tableName) throws SQLException {
        String columnQuery = "select column_name, data_type, data_length, data_precision, data_scale, default_length, density, char_length from " +
                "all_tab_columns where owner like '" + schemaNamePattern + "' and table_name='" + tableName + "'";

        PreparedStatement statement = connection().prepareStatement(columnQuery);
        return statement.executeQuery();
    }

    // this is much faster, we will use it until full replacement of the metadata usage TODO
    public void readSchemaForCapturedTables(Tables tables, String databaseCatalog, String schemaNamePattern,
                                            ColumnNameFilter columnFilter, boolean removeTablesNotFoundInJdbc, Set<TableId> capturedTables)
            throws SQLException {

        Set<TableId> tableIdsBefore = new HashSet<>(tables.tableIds());

        DatabaseMetaData metadata = connection().getMetaData();
        Map<TableId, List<Column>> columnsByTable = new HashMap<>();

        for (TableId tableId : capturedTables) {
            try (ResultSet columnMetadata = metadata.getColumns(databaseCatalog, schemaNamePattern, tableId.table(), null)) {
                while (columnMetadata.next()) {
                    // add all whitelisted columns
                    readTableColumn(columnMetadata, tableId, columnFilter).ifPresent(column -> {
                        columnsByTable.computeIfAbsent(tableId, t -> new ArrayList<>())
                                .add(column.create());
                    });
                }
            }
        }

        // Read the metadata for the primary keys ...
        for (Map.Entry<TableId, List<Column>> tableEntry : columnsByTable.entrySet()) {
            // First get the primary key information, which must be done for *each* table ...
            List<String> pkColumnNames = readPrimaryKeyNames(metadata, tableEntry.getKey());

            // Then define the table ...
            List<Column> columns = tableEntry.getValue();
            Collections.sort(columns);
            tables.overwriteTable(tableEntry.getKey(), columns, pkColumnNames, null);
        }

        if (removeTablesNotFoundInJdbc) {
            // Remove any definitions for tables that were not found in the database metadata ...
            tableIdsBefore.removeAll(columnsByTable.keySet());
            tableIdsBefore.forEach(tables::removeTable);
        }

        for (TableId tableId : capturedTables) {
            overrideDMSpecificColumnTypes(tables, tableId, tableId);
        }

    }

    @Override
    public void readSchema(Tables tables, String databaseCatalog, String schemaNamePattern, TableFilter tableFilter,
                           ColumnNameFilter columnFilter, boolean removeTablesNotFoundInJdbc)
            throws SQLException {

        super.readSchema(tables, null, schemaNamePattern, tableFilter, columnFilter, removeTablesNotFoundInJdbc);

        Set<TableId> tableIds = tables.tableIds().stream().filter(x -> schemaNamePattern.equals(x.schema())).collect(Collectors.toSet());

        for (TableId tableId : tableIds) {
            // super.readSchema() populates ids without the catalog; hence we apply the filtering only
            // here and if a table is included, overwrite it with a new id including the catalog
            TableId tableIdWithCatalog = new TableId(databaseCatalog, tableId.schema(), tableId.table());

            if (tableFilter.isIncluded(tableIdWithCatalog)) {
                overrideDMSpecificColumnTypes(tables, tableId, tableIdWithCatalog);
            }

            tables.removeTable(tableId);
        }
    }

    @Override
    public List<String> readTableUniqueIndices(DatabaseMetaData metadata, TableId id) throws SQLException {
        return super.readTableUniqueIndices(metadata, id.toDoubleQuoted());
    }

    @Override
    protected boolean isTableUniqueIndexIncluded(String indexName, String columnName) {
        if (columnName != null) {
            return !SYS_NC_PATTERN.matcher(columnName).matches();
        }
        return false;
    }

    protected Column createRowidColumn(int position) throws SQLException {

        final String columnName = GENERATED_PK_NAME;
        final ColumnEditor column = Column.editor().name(columnName);
        column.type("ROWID");
        column.length(19);
        column.optional(true);
        column.position(position);
        column.autoIncremented(true);
        column.generated(false);
        column.nativeType(-1);
        column.jdbcType(Types.VARCHAR);
        return column.create();
    }

    private void overrideDMSpecificColumnTypes(Tables tables, TableId tableId, TableId tableIdWithCatalog) {
        TableEditor editor = tables.editTable(tableId);
        editor.tableId(tableIdWithCatalog);

        List<String> columnNames = new ArrayList<>(editor.columnNames());
        for (String columnName : columnNames) {
            Column column = editor.columnWithName(columnName);
            if (column.jdbcType() == Types.TIMESTAMP) {
                editor.addColumn(
                        column.edit()
                                .length(column.scale().orElse(Column.UNSET_INT_VALUE))
                                .scale(null)
                                .create());
            }
            else if (column.jdbcType() == Types.DATE) {
                editor.addColumn(
                        column.edit()
                                .jdbcType(Types.TIMESTAMP)
                                .create());
            }
            else if (column.jdbcType() == Types.NUMERIC && column.scale().isPresent() && column.scale().get() == 0) {
                editor.addColumn(
                        column.edit()
                                .scale(null)
                                .create());
            }
            // NUMBER columns without scale value have it set to -127 instead of null;
            // let's rectify that
            else if (column.jdbcType() == OracleTypes.NUMBER) {
                column.scale()
                        .filter(s -> s == ORACLE_UNSET_SCALE)
                        .ifPresent(s -> {
                            editor.addColumn(
                                    column.edit()
                                            .scale(null)
                                            .create());
                        });
            }
        }
        try {
            List<String> primaryKeyColumns = new ArrayList<>(editor.primaryKeyColumnNames());
            int generatedColumnId = columnNames.size() + 1;
            editor.addColumn(createRowidColumn(generatedColumnId));
            primaryKeyColumns.add(GENERATED_PK_NAME);
            editor.setPrimaryKeyNames(primaryKeyColumns);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
        tables.overwriteTable(editor.create());
    }

    /**
     * Get the current, most recent system change number.
     *
     * @return the current system change number
     * @throws SQLException if an exception occurred
     * @throws IllegalStateException if the query does not return at least one row
     */
    public Scn getCurrentScn() throws SQLException {
        return queryAndMap("SELECT CUR_LSN FROM V$RLOG", (rs) -> {
            if (rs.next()) {
                return Scn.valueOf(rs.getString(1));
            }
            throw new IllegalStateException("Could not get SCN");
        });
    }

    /**
     * Get the maximum system change number in the archive logs.
     *
     * @param archiveLogDestinationName the archive log destination name to be queried, can be {@code null}.
     * @return the maximum system change number in the archive logs
     * @throws SQLException if a database exception occurred
     * @throws DebeziumException if the maximum archive log system change number could not be found
     */
    public Scn getMaxArchiveLogScn(String archiveLogDestinationName) throws SQLException {
        String query = "SELECT MAX(NEXT_CHANGE#) FROM V$ARCHIVED_LOG " +
                "WHERE NAME IS NOT NULL " +
                "AND ARCHIVED = 'YES' " +
                "AND STATUS = 'A' ";
        return queryAndMap(query, (rs) -> {
            if (rs.next()) {
                return Scn.valueOf(rs.getString(1));
            }
            throw new DebeziumException("Could not obtain maximum archive log scn.");
        });
    }

    /**
     * Generate a given table's DDL metadata.
     *
     * @param tableId table identifier, should never be {@code null}
     * @return generated DDL
     * @throws SQLException if an exception occurred obtaining the DDL metadata
     */
    public String getTableMetadataDdl(TableId tableId) throws SQLException {
        // The storage and segment attributes aren't necessary
        executeWithoutCommitting("begin dbms_metadata.set_transform_param(DBMS_METADATA.SESSION_TRANSFORM, 'STORAGE', false); end;");
        /*
         * executeWithoutCommitting("begin dbms_metadata.set_transform_param(DBMS_METADATA.SESSION_TRANSFORM, 'SEGMENT_ATTRIBUTES', false); end;");
         * // In case DDL is returned as multiple DDL statements, this allows the parser to parse each separately.
         * // This is only critical during streaming as during snapshot the table structure is built from JDBC driver queries.
         * executeWithoutCommitting("begin dbms_metadata.set_transform_param(DBMS_METADATA.SESSION_TRANSFORM, 'SQLTERMINATOR', true); end;");
         */
        return queryAndMap("SELECT dbms_metadata.get_ddl('TABLE','" + tableId.table() + "','" + tableId.schema() + "') FROM DUAL", rs -> {
            if (!rs.next()) {
                throw new DebeziumException("Could not get DDL metadata for table: " + tableId);
            }

            Object res = rs.getObject(1);
            return ((Clob) res).getSubString(1, (int) ((Clob) res).length()).replace("NOT CLUSTER", " ");
        });
    }

    public static String connectionString(Configuration config) {
        return config.getString(URL) != null ? config.getString(URL)
                : ConnectorAdapter.parse(config.getString("connection.adapter")).getConnectionUrl();
    }

    private static ConnectionFactory resolveConnectionFactory(Configuration config) {
        return JdbcConnection.patternBasedFactory(connectionString(config), "dm.jdbc.driver.DmDriver", null);
    }
}
