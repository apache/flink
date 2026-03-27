/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.jdbc;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.StatementResult;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

import javax.annotation.Nullable;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static org.apache.flink.table.jdbc.utils.DatabaseMetaDataUtils.createCatalogsResultSet;
import static org.apache.flink.table.jdbc.utils.DatabaseMetaDataUtils.createColumnsResultSet;
import static org.apache.flink.table.jdbc.utils.DatabaseMetaDataUtils.createPrimaryKeysResultSet;
import static org.apache.flink.table.jdbc.utils.DatabaseMetaDataUtils.createSchemasResultSet;
import static org.apache.flink.table.jdbc.utils.DatabaseMetaDataUtils.createTableTypesResultSet;
import static org.apache.flink.table.jdbc.utils.DatabaseMetaDataUtils.createTablesResultSet;

/** Implementation of {@link java.sql.DatabaseMetaData} for flink jdbc driver. */
public class FlinkDatabaseMetaData extends BaseDatabaseMetaData {
    private final String url;
    private final FlinkConnection connection;
    private final Statement statement;
    private final Executor executor;

    @VisibleForTesting
    protected FlinkDatabaseMetaData(String url, FlinkConnection connection, Statement statement) {
        this.url = url;
        this.connection = connection;
        this.statement = statement;
        this.executor = connection.getExecutor();
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        try (StatementResult result = catalogs()) {
            return createCatalogsResultSet(statement, result);
        } catch (Exception e) {
            throw new SQLException("Get catalogs fail", e);
        }
    }

    private StatementResult catalogs() {
        return executor.executeStatement("SHOW CATALOGS");
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        return getSchemas(null, null);
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        try {
            List<String> catalogList = getCatalogList(catalog);

            Map<String, List<String>> catalogSchemaList = new HashMap<>();
            for (String cat : catalogList) {
                catalogSchemaList.put(cat, getSchemaList(cat, schemaPattern));
            }

            return createSchemasResultSet(statement, catalogList, catalogSchemaList);
        } catch (Exception e) {
            throw new SQLException("Get schemas fail", e);
        }
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return false;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return false;
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getTables(
            String catalog, String schemaPattern, String tableNamePattern, String[] types)
            throws SQLException {
        try {
            Set<String> typeFilter = null;
            if (types != null) {
                typeFilter = new HashSet<>(Arrays.asList(types));
            }

            List<String> catalogList = getCatalogList(catalog);

            List<RowData> tableRows = new ArrayList<>();
            for (String cat : catalogList) {
                List<String> schemaList = getSchemaList(cat, schemaPattern);

                for (String schema : schemaList) {
                    String qualifiedPath = String.format("`%s`.`%s`", cat, schema);
                    collectTables(
                            tableRows,
                            cat,
                            schema,
                            tableNamePattern,
                            "TABLE",
                            typeFilter,
                            qualifiedPath);
                    collectTables(
                            tableRows,
                            cat,
                            schema,
                            tableNamePattern,
                            "VIEW",
                            typeFilter,
                            qualifiedPath);
                }
            }

            return createTablesResultSet(statement, tableRows);
        } catch (Exception e) {
            throw new SQLException("Get tables fail", e);
        }
    }

    private void collectTables(
            List<RowData> tableRows,
            String catalog,
            String schema,
            @Nullable String tableNamePattern,
            String tableType,
            @Nullable Set<String> typeFilter,
            String qualifiedPath) {
        if (typeFilter != null && !typeFilter.contains(tableType)) {
            return;
        }
        String command = "VIEW".equals(tableType) ? "SHOW VIEWS" : "SHOW TABLES";
        String sql = String.format("%s FROM %s", command, qualifiedPath);
        try (StatementResult result = executor.executeStatement(sql)) {
            while (result.hasNext()) {
                String tableName = result.next().getString(0).toString();
                if (matchesPattern(tableName, tableNamePattern)) {
                    tableRows.add(
                            GenericRowData.of(
                                    StringData.fromString(catalog),
                                    StringData.fromString(schema),
                                    StringData.fromString(tableName),
                                    StringData.fromString(tableType),
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null));
                }
            }
        }
    }

    private static boolean matchesPattern(String value, @Nullable String pattern) {
        if (pattern == null) {
            return true;
        }
        String regex = Pattern.quote(pattern).replace("%", "\\E.*\\Q").replace("_", "\\E.\\Q");
        return value.matches(regex);
    }

    @Override
    public ResultSet getColumns(
            String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
            throws SQLException {
        try {
            List<RowData> columnRows = new ArrayList<>();
            for (String cat : getCatalogList(catalog)) {
                for (String schema : getSchemaList(cat, schemaPattern)) {
                    for (String table : getTableList(cat, schema, tableNamePattern)) {
                        columnRows.addAll(collectColumns(cat, schema, table, columnNamePattern));
                    }
                }
            }
            return createColumnsResultSet(statement, columnRows);
        } catch (Exception e) {
            throw new SQLException("Get columns fail", e);
        }
    }

    private List<RowData> collectColumns(
            String catalog, String schema, String table, @Nullable String columnNamePattern) {
        List<RowData> columnRows = new ArrayList<>();
        String qualifiedTable = String.format("`%s`.`%s`.`%s`", catalog, schema, table);
        try (StatementResult result =
                executor.executeStatement(
                        String.format("SELECT * FROM %s LIMIT 0", qualifiedTable))) {
            List<Column> columns = result.getResultSchema().getColumns();
            int ordinal = 1;
            for (Column col : columns) {
                if (matchesPattern(col.getName(), columnNamePattern)) {
                    columnRows.add(toColumnRow(catalog, schema, table, col, ordinal));
                    ordinal++;
                }
            }
        } catch (Exception e) {
            // skip tables that cannot be described
        }
        return columnRows;
    }

    private static RowData toColumnRow(
            String catalog, String schema, String table, Column col, int ordinal) {
        String typeName = col.getDataType().getLogicalType().asSummaryString();
        boolean nullable = col.getDataType().getLogicalType().isNullable();
        return GenericRowData.of(
                StringData.fromString(catalog),
                StringData.fromString(schema),
                StringData.fromString(table),
                StringData.fromString(col.getName()),
                getJdbcType(typeName),
                StringData.fromString(typeName),
                null,
                null,
                null,
                null,
                nullable
                        ? java.sql.DatabaseMetaData.columnNullable
                        : java.sql.DatabaseMetaData.columnNoNulls,
                null,
                null,
                null,
                null,
                null,
                ordinal,
                StringData.fromString(nullable ? "YES" : "NO"),
                null,
                null,
                null,
                null,
                StringData.fromString("NO"));
    }

    private static int getJdbcType(String flinkType) {
        String upper = flinkType.toUpperCase();
        if (upper.startsWith("BOOLEAN")) {
            return java.sql.Types.BOOLEAN;
        } else if (upper.startsWith("TINYINT")) {
            return java.sql.Types.TINYINT;
        } else if (upper.startsWith("SMALLINT")) {
            return java.sql.Types.SMALLINT;
        } else if (upper.startsWith("INT")) {
            return java.sql.Types.INTEGER;
        } else if (upper.startsWith("BIGINT")) {
            return java.sql.Types.BIGINT;
        } else if (upper.startsWith("FLOAT")) {
            return java.sql.Types.FLOAT;
        } else if (upper.startsWith("DOUBLE")) {
            return java.sql.Types.DOUBLE;
        } else if (upper.startsWith("DECIMAL") || upper.startsWith("NUMERIC")) {
            return java.sql.Types.DECIMAL;
        } else if (upper.startsWith("VARCHAR") || upper.startsWith("STRING")) {
            return java.sql.Types.VARCHAR;
        } else if (upper.startsWith("CHAR")) {
            return java.sql.Types.CHAR;
        } else if (upper.startsWith("DATE")) {
            return java.sql.Types.DATE;
        } else if (upper.startsWith("TIMESTAMP")) {
            return java.sql.Types.TIMESTAMP;
        } else if (upper.startsWith("TIME")) {
            return java.sql.Types.TIME;
        } else if (upper.startsWith("BINARY")
                || upper.startsWith("VARBINARY")
                || upper.startsWith("BYTES")) {
            return java.sql.Types.BINARY;
        } else if (upper.startsWith("ARRAY")) {
            return java.sql.Types.ARRAY;
        } else if (upper.startsWith("MAP") || upper.startsWith("ROW")) {
            return java.sql.Types.STRUCT;
        }
        return java.sql.Types.OTHER;
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table)
            throws SQLException {
        // tableSchema.getPrimaryKey() is not working
        try {
            List<RowData> pkRows = new ArrayList<>();
            String qualifiedTable = String.format("`%s`.`%s`.`%s`", catalog, schema, table);
            try (StatementResult result =
                    executor.executeStatement(
                            String.format("SELECT * FROM %s LIMIT 0", qualifiedTable))) {
                ResolvedSchema tableSchema = result.getResultSchema();
                tableSchema
                        .getPrimaryKey()
                        .ifPresent(
                                pk -> {
                                    List<String> columns = pk.getColumns();
                                    for (int i = 0; i < columns.size(); i++) {
                                        pkRows.add(
                                                GenericRowData.of(
                                                        StringData.fromString(catalog),
                                                        StringData.fromString(schema),
                                                        StringData.fromString(table),
                                                        StringData.fromString(columns.get(i)),
                                                        i + 1,
                                                        StringData.fromString(pk.getName())));
                                    }
                                });
            }
            return createPrimaryKeysResultSet(statement, pkRows);
        } catch (Exception e) {
            throw new SQLException("Get primary keys fail", e);
        }
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        List<RowData> typeRows = new ArrayList<>();
        typeRows.add(GenericRowData.of(StringData.fromString("TABLE")));
        typeRows.add(GenericRowData.of(StringData.fromString("VIEW")));
        return createTableTypesResultSet(statement, typeRows);
    }

    private List<String> getCatalogList(@Nullable String catalog) {
        if (catalog != null) {
            return Collections.singletonList(catalog);
        }
        List<String> catalogList = new ArrayList<>();
        try (StatementResult result = catalogs()) {
            while (result.hasNext()) {
                catalogList.add(result.next().getString(0).toString());
            }
        }
        return catalogList;
    }

    private List<String> getSchemaList(String catalog, @Nullable String schemaPattern) {
        List<String> schemas = new ArrayList<>();
        try (StatementResult result =
                executor.executeStatement(String.format("SHOW DATABASES IN `%s`", catalog))) {
            while (result.hasNext()) {
                String schema = result.next().getString(0).toString();
                if (matchesPattern(schema, schemaPattern)) {
                    schemas.add(schema);
                }
            }
        }
        return schemas;
    }

    private List<String> getTableList(
            String catalog, String schema, @Nullable String tableNamePattern) {
        List<String> tables = new ArrayList<>();
        String qualifiedPath = String.format("`%s`.`%s`", catalog, schema);
        try (StatementResult result =
                executor.executeStatement(String.format("SHOW TABLES FROM %s", qualifiedPath))) {
            while (result.hasNext()) {
                String tableName = result.next().getString(0).toString();
                if (matchesPattern(tableName, tableNamePattern)) {
                    tables.add(tableName);
                }
            }
        }
        return tables;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public boolean supportsSavepoints() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        return false;
    }

    @Override
    public boolean allProceduresAreCallable() throws SQLException {
        return false;
    }

    @Override
    public boolean allTablesAreSelectable() throws SQLException {
        return true;
    }

    @Override
    public String getURL() throws SQLException {
        return url;
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return true;
    }

    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        return false;
    }

    /** In flink null value will be used as low value for sort. */
    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        return true;
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        return false;
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        return DriverInfo.DRIVER_NAME;
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        return DriverInfo.DRIVER_VERSION;
    }

    @Override
    public String getDriverName() throws SQLException {
        return FlinkDriver.class.getName();
    }

    @Override
    public String getDriverVersion() throws SQLException {
        return DriverInfo.DRIVER_VERSION;
    }

    @Override
    public int getDriverMajorVersion() {
        return DriverInfo.DRIVER_VERSION_MAJOR;
    }

    @Override
    public int getDriverMinorVersion() {
        return DriverInfo.DRIVER_VERSION_MINOR;
    }

    @Override
    public boolean supportsResultSetType(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        return false;
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean updatesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean deletesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean insertsAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        return false;
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        return DriverInfo.DRIVER_VERSION_MAJOR;
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        return DriverInfo.DRIVER_VERSION_MINOR;
    }

    @Override
    public int getJDBCMajorVersion() throws SQLException {
        return DriverInfo.DRIVER_VERSION_MAJOR;
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        return DriverInfo.DRIVER_VERSION_MINOR;
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException {
        return false;
    }

    @Override
    public boolean usesLocalFiles() throws SQLException {
        return false;
    }

    @Override
    public boolean usesLocalFilePerTable() throws SQLException {
        return false;
    }

    /** Flink sql is mixed case as sensitive. */
    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return false;
    }

    /** Flink sql is mixed case as sensitive. */
    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return true;
    }

    /** Flink sql is mixed case as sensitive. */
    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    /** Flink sql is mixed case as sensitive. */
    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public String getIdentifierQuoteString() throws SQLException {
        return "`";
    }

    @Override
    public String getExtraNameCharacters() throws SQLException {
        return "";
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsColumnAliasing() throws SQLException {
        return true;
    }

    /** Null value plus non-null in flink will be null result. */
    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsConvert() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTableCorrelationNames() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsOrderByUnrelated() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupBy() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupByUnrelated() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsLikeEscapeClause() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMultipleResultSets() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNonNullableColumns() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOuterJoins() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsFullOuterJoins() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
        return true;
    }

    @Override
    public String getSchemaTerm() throws SQLException {
        return "database";
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        return "catalog";
    }

    /** Catalog name appears at the start of full name. */
    @Override
    public boolean isCatalogAtStart() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedDelete() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSelectForUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStoredProcedures() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInExists() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInIns() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsUnion() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsUnionAll() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
}
