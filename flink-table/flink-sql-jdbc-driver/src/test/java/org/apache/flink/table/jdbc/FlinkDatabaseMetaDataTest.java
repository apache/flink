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
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for flink database metadata. */
public class FlinkDatabaseMetaDataTest extends FlinkJdbcDriverTestBase {
    @Test
    void testCatalogSchemas() throws Exception {

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
            assertThat(connection.getCatalog()).isEqualTo("test_catalog2");
            assertThat(connection.getSchema()).isEqualTo("database21");

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
            assertThat(connection.getCatalog()).isEqualTo("test_catalog2");
            assertThat(connection.getSchema()).isEqualTo("database21");

            assertThat(databaseMetaData.allProceduresAreCallable()).isFalse();
            assertThat(databaseMetaData.allTablesAreSelectable()).isTrue();
            assertThat(databaseMetaData.isReadOnly()).isTrue();
            assertThat(databaseMetaData.nullsAreSortedHigh()).isFalse();
            assertThat(databaseMetaData.nullsAreSortedLow()).isTrue();
            assertThat(databaseMetaData.nullsAreSortedAtStart()).isFalse();
            assertThat(databaseMetaData.nullsAreSortedAtEnd()).isFalse();
            assertThat(databaseMetaData.usesLocalFiles()).isFalse();
            assertThat(databaseMetaData.usesLocalFilePerTable()).isFalse();
            assertThat(databaseMetaData.supportsMixedCaseIdentifiers()).isTrue();
            assertThat(databaseMetaData.storesUpperCaseIdentifiers()).isFalse();
            assertThat(databaseMetaData.storesLowerCaseIdentifiers()).isFalse();
            assertThat(databaseMetaData.storesMixedCaseIdentifiers()).isTrue();
            assertThat(databaseMetaData.supportsMixedCaseQuotedIdentifiers()).isTrue();
            assertThat(databaseMetaData.storesUpperCaseQuotedIdentifiers()).isFalse();
            assertThat(databaseMetaData.storesLowerCaseQuotedIdentifiers()).isFalse();
            assertThat(databaseMetaData.storesMixedCaseQuotedIdentifiers()).isTrue();
            assertThat(databaseMetaData.supportsAlterTableWithAddColumn()).isTrue();
            assertThat(databaseMetaData.supportsAlterTableWithDropColumn()).isTrue();
            assertThat(databaseMetaData.supportsColumnAliasing()).isTrue();
            assertThat(databaseMetaData.nullPlusNonNullIsNull()).isTrue();
            assertThat(databaseMetaData.supportsConvert()).isFalse();
            assertThat(databaseMetaData.supportsConvert(1, 2)).isFalse();
            assertThat(databaseMetaData.supportsTableCorrelationNames()).isTrue();
            assertThat(databaseMetaData.supportsDifferentTableCorrelationNames()).isFalse();
            assertThat(databaseMetaData.supportsExpressionsInOrderBy()).isTrue();
            assertThat(databaseMetaData.supportsOrderByUnrelated()).isTrue();
            assertThat(databaseMetaData.supportsGroupBy()).isTrue();
            assertThat(databaseMetaData.supportsGroupByUnrelated()).isTrue();
            assertThat(databaseMetaData.supportsGroupByBeyondSelect()).isTrue();
            assertThat(databaseMetaData.supportsLikeEscapeClause()).isTrue();
            assertThat(databaseMetaData.supportsMultipleResultSets()).isFalse();
            assertThat(databaseMetaData.supportsMultipleTransactions()).isFalse();
            assertThat(databaseMetaData.supportsNonNullableColumns()).isTrue();
            assertThat(databaseMetaData.supportsMinimumSQLGrammar()).isFalse();
            assertThat(databaseMetaData.supportsCoreSQLGrammar()).isFalse();
            assertThat(databaseMetaData.supportsExtendedSQLGrammar()).isFalse();
            assertThat(databaseMetaData.supportsANSI92EntryLevelSQL()).isFalse();
            assertThat(databaseMetaData.supportsANSI92IntermediateSQL()).isFalse();
            assertThat(databaseMetaData.supportsANSI92FullSQL()).isFalse();
            assertThat(databaseMetaData.supportsIntegrityEnhancementFacility()).isFalse();
            assertThat(databaseMetaData.supportsOuterJoins()).isTrue();
            assertThat(databaseMetaData.supportsFullOuterJoins()).isTrue();
            assertThat(databaseMetaData.supportsLimitedOuterJoins()).isTrue();
            assertThat(databaseMetaData.isCatalogAtStart()).isTrue();
            assertThat(databaseMetaData.supportsSchemasInDataManipulation()).isTrue();
            assertThat(databaseMetaData.supportsSchemasInProcedureCalls()).isFalse();
            assertThat(databaseMetaData.supportsSchemasInTableDefinitions()).isTrue();
            assertThat(databaseMetaData.supportsSchemasInIndexDefinitions()).isFalse();
            assertThat(databaseMetaData.supportsSchemasInPrivilegeDefinitions()).isFalse();
            assertThat(databaseMetaData.supportsCatalogsInDataManipulation()).isTrue();
            assertThat(databaseMetaData.supportsCatalogsInProcedureCalls()).isFalse();
            assertThat(databaseMetaData.supportsCatalogsInTableDefinitions()).isTrue();
            assertThat(databaseMetaData.supportsCatalogsInIndexDefinitions()).isFalse();
            assertThat(databaseMetaData.supportsCatalogsInPrivilegeDefinitions()).isFalse();
            assertThat(databaseMetaData.supportsPositionedDelete()).isFalse();
            assertThat(databaseMetaData.supportsPositionedUpdate()).isFalse();
            assertThat(databaseMetaData.supportsSelectForUpdate()).isFalse();
            assertThat(databaseMetaData.supportsStoredProcedures()).isFalse();
            assertThat(databaseMetaData.supportsSubqueriesInComparisons()).isTrue();
            assertThat(databaseMetaData.supportsSubqueriesInExists()).isTrue();
            assertThat(databaseMetaData.supportsSubqueriesInIns()).isTrue();
            assertThat(databaseMetaData.supportsSubqueriesInQuantifieds()).isTrue();
            assertThat(databaseMetaData.supportsCorrelatedSubqueries()).isTrue();
            assertThat(databaseMetaData.supportsUnion()).isTrue();
            assertThat(databaseMetaData.supportsUnionAll()).isTrue();
            assertThat(databaseMetaData.supportsOpenCursorsAcrossCommit()).isFalse();
            assertThat(databaseMetaData.supportsOpenCursorsAcrossRollback()).isFalse();
            assertThat(databaseMetaData.supportsOpenStatementsAcrossCommit()).isFalse();
            assertThat(databaseMetaData.supportsOpenStatementsAcrossRollback()).isFalse();
            assertThat(databaseMetaData.doesMaxRowSizeIncludeBlobs()).isFalse();
            assertThat(databaseMetaData.supportsTransactions()).isFalse();
            assertThat(databaseMetaData.supportsTransactionIsolationLevel(1)).isFalse();
            assertThat(databaseMetaData.supportsDataDefinitionAndDataManipulationTransactions())
                    .isFalse();
            assertThat(databaseMetaData.supportsDataManipulationTransactionsOnly()).isFalse();
            assertThat(databaseMetaData.dataDefinitionCausesTransactionCommit()).isFalse();
            assertThat(databaseMetaData.dataDefinitionIgnoredInTransactions()).isFalse();
            assertThat(databaseMetaData.supportsResultSetType(1)).isFalse();
            assertThat(databaseMetaData.supportsResultSetConcurrency(1, 2)).isFalse();
            assertThat(databaseMetaData.ownUpdatesAreVisible(1)).isFalse();
            assertThat(databaseMetaData.ownDeletesAreVisible(1)).isFalse();
            assertThat(databaseMetaData.ownInsertsAreVisible(1)).isFalse();
            assertThat(databaseMetaData.othersUpdatesAreVisible(1)).isFalse();
            assertThat(databaseMetaData.othersDeletesAreVisible(1)).isFalse();
            assertThat(databaseMetaData.othersInsertsAreVisible(1)).isFalse();
            assertThat(databaseMetaData.updatesAreDetected(1)).isFalse();
            assertThat(databaseMetaData.deletesAreDetected(1)).isFalse();
            assertThat(databaseMetaData.insertsAreDetected(1)).isFalse();
            assertThat(databaseMetaData.supportsBatchUpdates()).isFalse();
            assertThat(databaseMetaData.supportsSavepoints()).isFalse();
            assertThat(databaseMetaData.supportsNamedParameters()).isFalse();
            assertThat(databaseMetaData.supportsMultipleOpenResults()).isFalse();
            assertThat(databaseMetaData.supportsGetGeneratedKeys()).isFalse();
            assertThat(databaseMetaData.supportsResultSetHoldability(1)).isFalse();
            assertThat(databaseMetaData.locatorsUpdateCopy()).isFalse();
            assertThat(databaseMetaData.supportsStatementPooling()).isFalse();
            assertThat(databaseMetaData.supportsStoredFunctionsUsingCallSyntax()).isFalse();
            assertThat(databaseMetaData.autoCommitFailureClosesAllResultSets()).isFalse();
            assertThat(databaseMetaData.generatedKeyAlwaysReturned()).isFalse();
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
            assertThat(result.hasNext()).isTrue();
            RowData rowData = result.next();
            assertThat(rowData.getArity()).isEqualTo(1);
            assertThat(rowData.getString(0).toString()).isEqualTo("OK");

            assertThat(result.hasNext()).isFalse();
        }
    }
}
