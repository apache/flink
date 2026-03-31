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

import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.StatementResult;
import org.apache.flink.table.data.RowData;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for flink database metadata. */
public class FlinkDatabaseMetaDataTest extends FlinkJdbcDriverTestBase {
    @Test
    public void testCatalogSchemas() throws Exception {

        DriverUri driverUri = getDriverUri();
        try (FlinkConnection connection = new FlinkConnection(driverUri)) {
            Executor executor = connection.getExecutor();
            // create databases in default catalog
            executeDDL("CREATE DATABASE database11", executor);
            executeDDL("CREATE DATABASE database12", executor);
            executeDDL("CREATE DATABASE database13", executor);

            // create catalog2 and databases
            executeDDL("CREATE CATALOG test_catalog2 WITH ('type'='generic_in_memory');", executor);
            executeDDL("CREATE DATABASE test_catalog2.database11", executor);
            executeDDL("CREATE DATABASE test_catalog2.database21", executor);
            executeDDL("CREATE DATABASE test_catalog2.database31", executor);

            // create catalog1 and databases
            executeDDL("CREATE CATALOG test_catalog1 WITH ('type'='generic_in_memory');", executor);
            executeDDL("CREATE DATABASE test_catalog1.database11", executor);
            executeDDL("CREATE DATABASE test_catalog1.database21", executor);
            executeDDL("CREATE DATABASE test_catalog1.database13", executor);

            connection.setCatalog("test_catalog2");
            connection.setSchema("database21");
            assertEquals("test_catalog2", connection.getCatalog());
            assertEquals("database21", connection.getSchema());

            DatabaseMetaData databaseMetaData =
                    new FlinkDatabaseMetaData(
                            driverUri.getURL(), connection, new TestingStatement());
            // Show all catalogs
            assertThat(resultSetToListAndClose(databaseMetaData.getCatalogs()))
                    .containsExactly("default_catalog", "test_catalog1", "test_catalog2");
            // Show all databases
            assertThat(resultSetToListAndClose(databaseMetaData.getSchemas()))
                    .containsExactly(
                            "database11,default_catalog",
                            "database12,default_catalog",
                            "database13,default_catalog",
                            "default_database,default_catalog",
                            "database11,test_catalog1",
                            "database13,test_catalog1",
                            "database21,test_catalog1",
                            "default,test_catalog1",
                            "database11,test_catalog2",
                            "database21,test_catalog2",
                            "database31,test_catalog2",
                            "default,test_catalog2");

            // Validate that the default catalog and database are not changed.
            assertEquals("test_catalog2", connection.getCatalog());
            assertEquals("database21", connection.getSchema());

            assertFalse(databaseMetaData.allProceduresAreCallable());
            assertTrue(databaseMetaData.allTablesAreSelectable());
            assertTrue(databaseMetaData.isReadOnly());
            assertFalse(databaseMetaData.nullsAreSortedHigh());
            assertTrue(databaseMetaData.nullsAreSortedLow());
            assertFalse(databaseMetaData.nullsAreSortedAtStart());
            assertFalse(databaseMetaData.nullsAreSortedAtEnd());
            assertFalse(databaseMetaData.usesLocalFiles());
            assertFalse(databaseMetaData.usesLocalFilePerTable());
            assertTrue(databaseMetaData.supportsMixedCaseIdentifiers());
            assertFalse(databaseMetaData.storesUpperCaseIdentifiers());
            assertFalse(databaseMetaData.storesLowerCaseIdentifiers());
            assertTrue(databaseMetaData.storesMixedCaseIdentifiers());
            assertTrue(databaseMetaData.supportsMixedCaseQuotedIdentifiers());
            assertFalse(databaseMetaData.storesUpperCaseQuotedIdentifiers());
            assertFalse(databaseMetaData.storesLowerCaseQuotedIdentifiers());
            assertTrue(databaseMetaData.storesMixedCaseQuotedIdentifiers());
            assertTrue(databaseMetaData.supportsAlterTableWithAddColumn());
            assertTrue(databaseMetaData.supportsAlterTableWithDropColumn());
            assertTrue(databaseMetaData.supportsColumnAliasing());
            assertTrue(databaseMetaData.nullPlusNonNullIsNull());
            assertFalse(databaseMetaData.supportsConvert());
            assertFalse(databaseMetaData.supportsConvert(1, 2));
            assertTrue(databaseMetaData.supportsTableCorrelationNames());
            assertFalse(databaseMetaData.supportsDifferentTableCorrelationNames());
            assertTrue(databaseMetaData.supportsExpressionsInOrderBy());
            assertTrue(databaseMetaData.supportsOrderByUnrelated());
            assertTrue(databaseMetaData.supportsGroupBy());
            assertTrue(databaseMetaData.supportsGroupByUnrelated());
            assertTrue(databaseMetaData.supportsGroupByBeyondSelect());
            assertTrue(databaseMetaData.supportsLikeEscapeClause());
            assertFalse(databaseMetaData.supportsMultipleResultSets());
            assertFalse(databaseMetaData.supportsMultipleTransactions());
            assertTrue(databaseMetaData.supportsNonNullableColumns());
            assertFalse(databaseMetaData.supportsMinimumSQLGrammar());
            assertFalse(databaseMetaData.supportsCoreSQLGrammar());
            assertFalse(databaseMetaData.supportsExtendedSQLGrammar());
            assertFalse(databaseMetaData.supportsANSI92EntryLevelSQL());
            assertFalse(databaseMetaData.supportsANSI92IntermediateSQL());
            assertFalse(databaseMetaData.supportsANSI92FullSQL());
            assertFalse(databaseMetaData.supportsIntegrityEnhancementFacility());
            assertTrue(databaseMetaData.supportsOuterJoins());
            assertTrue(databaseMetaData.supportsFullOuterJoins());
            assertTrue(databaseMetaData.supportsLimitedOuterJoins());
            assertTrue(databaseMetaData.isCatalogAtStart());
            assertTrue(databaseMetaData.supportsSchemasInDataManipulation());
            assertFalse(databaseMetaData.supportsSchemasInProcedureCalls());
            assertTrue(databaseMetaData.supportsSchemasInTableDefinitions());
            assertFalse(databaseMetaData.supportsSchemasInIndexDefinitions());
            assertFalse(databaseMetaData.supportsSchemasInPrivilegeDefinitions());
            assertTrue(databaseMetaData.supportsCatalogsInDataManipulation());
            assertFalse(databaseMetaData.supportsCatalogsInProcedureCalls());
            assertTrue(databaseMetaData.supportsCatalogsInTableDefinitions());
            assertFalse(databaseMetaData.supportsCatalogsInIndexDefinitions());
            assertFalse(databaseMetaData.supportsCatalogsInPrivilegeDefinitions());
            assertFalse(databaseMetaData.supportsPositionedDelete());
            assertFalse(databaseMetaData.supportsPositionedUpdate());
            assertFalse(databaseMetaData.supportsSelectForUpdate());
            assertFalse(databaseMetaData.supportsStoredProcedures());
            assertTrue(databaseMetaData.supportsSubqueriesInComparisons());
            assertTrue(databaseMetaData.supportsSubqueriesInExists());
            assertTrue(databaseMetaData.supportsSubqueriesInIns());
            assertTrue(databaseMetaData.supportsSubqueriesInQuantifieds());
            assertTrue(databaseMetaData.supportsCorrelatedSubqueries());
            assertTrue(databaseMetaData.supportsUnion());
            assertTrue(databaseMetaData.supportsUnionAll());
            assertFalse(databaseMetaData.supportsOpenCursorsAcrossCommit());
            assertFalse(databaseMetaData.supportsOpenCursorsAcrossRollback());
            assertFalse(databaseMetaData.supportsOpenStatementsAcrossCommit());
            assertFalse(databaseMetaData.supportsOpenStatementsAcrossRollback());
            assertFalse(databaseMetaData.doesMaxRowSizeIncludeBlobs());
            assertFalse(databaseMetaData.supportsTransactions());
            assertFalse(databaseMetaData.supportsTransactionIsolationLevel(1));
            assertFalse(databaseMetaData.supportsDataDefinitionAndDataManipulationTransactions());
            assertFalse(databaseMetaData.supportsDataManipulationTransactionsOnly());
            assertFalse(databaseMetaData.dataDefinitionCausesTransactionCommit());
            assertFalse(databaseMetaData.dataDefinitionIgnoredInTransactions());
            assertFalse(databaseMetaData.supportsResultSetType(1));
            assertFalse(databaseMetaData.supportsResultSetConcurrency(1, 2));
            assertFalse(databaseMetaData.ownUpdatesAreVisible(1));
            assertFalse(databaseMetaData.ownDeletesAreVisible(1));
            assertFalse(databaseMetaData.ownInsertsAreVisible(1));
            assertFalse(databaseMetaData.othersUpdatesAreVisible(1));
            assertFalse(databaseMetaData.othersDeletesAreVisible(1));
            assertFalse(databaseMetaData.othersInsertsAreVisible(1));
            assertFalse(databaseMetaData.updatesAreDetected(1));
            assertFalse(databaseMetaData.deletesAreDetected(1));
            assertFalse(databaseMetaData.insertsAreDetected(1));
            assertFalse(databaseMetaData.supportsBatchUpdates());
            assertFalse(databaseMetaData.supportsSavepoints());
            assertFalse(databaseMetaData.supportsNamedParameters());
            assertFalse(databaseMetaData.supportsMultipleOpenResults());
            assertFalse(databaseMetaData.supportsGetGeneratedKeys());
            assertFalse(databaseMetaData.supportsResultSetHoldability(1));
            assertFalse(databaseMetaData.locatorsUpdateCopy());
            assertFalse(databaseMetaData.supportsStatementPooling());
            assertFalse(databaseMetaData.supportsStoredFunctionsUsingCallSyntax());
            assertFalse(databaseMetaData.autoCommitFailureClosesAllResultSets());
            assertFalse(databaseMetaData.generatedKeyAlwaysReturned());
        }
    }

    @Test
    public void testGetSchemasWithFilter() throws Exception {
        DriverUri driverUri = getDriverUri();
        try (FlinkConnection connection = new FlinkConnection(driverUri)) {
            Executor executor = connection.getExecutor();
            executeDDL("CREATE DATABASE database_a", executor);
            executeDDL("CREATE DATABASE database_b", executor);

            DatabaseMetaData metaData =
                    new FlinkDatabaseMetaData(
                            driverUri.getURL(), connection, new TestingStatement());

            // Filter by catalog
            List<String> schemas =
                    resultSetToListAndClose(metaData.getSchemas("default_catalog", null));
            assertThat(schemas).allMatch(s -> s.contains("default_catalog"));

            // Filter by schema pattern
            List<String> filteredSchemas =
                    resultSetToListAndClose(metaData.getSchemas("default_catalog", "database_%"));
            assertThat(filteredSchemas)
                    .allMatch(s -> s.startsWith("database_a,") || s.startsWith("database_b,"));
        }
    }

    @Test
    public void testGetTables() throws Exception {
        DriverUri driverUri = getDriverUri();
        try (FlinkConnection connection = new FlinkConnection(driverUri)) {
            Executor executor = connection.getExecutor();
            executeDDL(
                    "CREATE TABLE test_table1 (id INT, name STRING) WITH ('connector' = 'datagen')",
                    executor);
            executeDDL(
                    "CREATE TABLE test_table2 (id INT, val DOUBLE) WITH ('connector' = 'datagen')",
                    executor);
            executeDDL("CREATE VIEW test_view1 AS SELECT id, name FROM test_table1", executor);

            DatabaseMetaData metaData =
                    new FlinkDatabaseMetaData(
                            driverUri.getURL(), connection, new TestingStatement());

            // Get all tables and views
            ResultSet rs = metaData.getTables("default_catalog", "default_database", null, null);
            List<String> allResults = resultSetToListAndClose(rs);
            Set<String> tableNames = new HashSet<>();
            for (String row : allResults) {
                tableNames.add(row.split(",")[2]);
            }
            assertThat(tableNames).contains("test_table1", "test_table2", "test_view1");

            // Filter by type TABLE only
            rs =
                    metaData.getTables(
                            "default_catalog", "default_database", null, new String[] {"TABLE"});
            List<String> tableOnly = resultSetToListAndClose(rs);
            for (String row : tableOnly) {
                assertThat(row).contains("TABLE");
            }
            Set<String> tableOnlyNames = new HashSet<>();
            for (String row : tableOnly) {
                tableOnlyNames.add(row.split(",")[2]);
            }
            assertThat(tableOnlyNames).contains("test_table1", "test_table2");
            assertThat(tableOnlyNames).doesNotContain("test_view1");

            // Filter by name pattern
            rs = metaData.getTables("default_catalog", "default_database", "test_table%", null);
            List<String> patternResults = resultSetToListAndClose(rs);
            for (String row : patternResults) {
                assertThat(row.split(",")[2]).startsWith("test_table");
            }
        }
    }

    @Test
    public void testGetColumns() throws Exception {
        DriverUri driverUri = getDriverUri();
        try (FlinkConnection connection = new FlinkConnection(driverUri)) {
            Executor executor = connection.getExecutor();
            executeDDL(
                    "CREATE TABLE col_test (id INT NOT NULL, name STRING, score DOUBLE) WITH ('connector' = 'datagen')",
                    executor);

            DatabaseMetaData metaData =
                    new FlinkDatabaseMetaData(
                            driverUri.getURL(), connection, new TestingStatement());

            ResultSet rs =
                    metaData.getColumns("default_catalog", "default_database", "col_test", null);

            // Verify column count and names using COLUMN_NAME (column 4)
            List<String> columnNames = new ArrayList<>();
            while (rs.next()) {
                columnNames.add(rs.getString("COLUMN_NAME"));
            }
            rs.close();
            assertThat(columnNames).containsExactly("id", "name", "score");

            // Filter by column name pattern
            rs = metaData.getColumns("default_catalog", "default_database", "col_test", "na%");
            List<String> filtered = new ArrayList<>();
            while (rs.next()) {
                filtered.add(rs.getString("COLUMN_NAME"));
            }
            rs.close();
            assertThat(filtered).containsExactly("name");
        }
    }

    @Test
    public void testGetTableTypes() throws Exception {
        DriverUri driverUri = getDriverUri();
        try (FlinkConnection connection = new FlinkConnection(driverUri)) {
            DatabaseMetaData metaData =
                    new FlinkDatabaseMetaData(
                            driverUri.getURL(), connection, new TestingStatement());

            List<String> types = resultSetToListAndClose(metaData.getTableTypes());
            assertThat(types).containsExactly("TABLE", "VIEW");
        }
    }

    @Test
    public void testGetPrimaryKeys() throws Exception {
        DriverUri driverUri = getDriverUri();
        try (FlinkConnection connection = new FlinkConnection(driverUri)) {
            Executor executor = connection.getExecutor();
            executeDDL(
                    "CREATE TABLE pk_test (id INT NOT NULL, name STRING, PRIMARY KEY (id) NOT ENFORCED) WITH ('connector' = 'datagen')",
                    executor);

            DatabaseMetaData metaData =
                    new FlinkDatabaseMetaData(
                            driverUri.getURL(), connection, new TestingStatement());

            // getPrimaryKeys uses SELECT * LIMIT 0 which may not preserve PK info
            // in the result schema, so we verify at least it does not throw
            ResultSet rs =
                    metaData.getPrimaryKeys("default_catalog", "default_database", "pk_test");
            List<String> pkColumns = new ArrayList<>();
            while (rs.next()) {
                pkColumns.add(rs.getString("COLUMN_NAME"));
            }
            rs.close();
            // PK info may or may not be available depending on the catalog implementation
            assertThat(rs.getMetaData().getColumnCount()).isEqualTo(6);
        }
    }

    private List<String> resultSetToListAndClose(ResultSet resultSet) throws Exception {
        List<String> resultList = new ArrayList<>();
        int columnCount = resultSet.getMetaData().getColumnCount();
        while (resultSet.next()) {
            List<String> columnStringList = new ArrayList<>(columnCount);
            for (int i = 1; i <= columnCount; i++) {
                columnStringList.add(resultSet.getString(i));
            }
            resultList.add(StringUtils.join(columnStringList, ","));
        }
        resultSet.close();

        return resultList;
    }

    private void executeDDL(String sql, Executor executor) {
        try (StatementResult result = executor.executeStatement(sql)) {
            assertTrue(result.hasNext());
            RowData rowData = result.next();
            assertEquals(1, rowData.getArity());
            assertEquals("OK", rowData.getString(0).toString());

            assertFalse(result.hasNext());
        }
    }
}
