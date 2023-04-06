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

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

/** Base {@link DatabaseMetaData} for flink driver with not supported features. */
public abstract class BaseDatabaseMetaData implements DatabaseMetaData {
    @Override
    public String getUserName() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#getUserName is not supported");
    }

    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#nullsAreSortedHigh is not supported");
    }

    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#nullsAreSortedLow is not supported");
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#nullsAreSortedAtStart is not supported");
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#nullsAreSortedAtEnd is not supported");
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#supportsMixedCaseIdentifiers is not supported");
    }

    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#storesUpperCaseIdentifiers is not supported");
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#storesLowerCaseIdentifiers is not supported");
    }

    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#storesMixedCaseIdentifiers is not supported");
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#supportsMixedCaseQuotedIdentifiers is not supported");
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#storesUpperCaseQuotedIdentifiers is not supported");
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#storesLowerCaseQuotedIdentifiers is not supported");
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#storesMixedCaseQuotedIdentifiers is not supported");
    }

    @Override
    public String getSQLKeywords() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#getSQLKeywords is not supported");
    }

    @Override
    public String getNumericFunctions() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#getNumericFunctions is not supported");
    }

    @Override
    public String getStringFunctions() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#getStringFunctions is not supported");
    }

    @Override
    public String getSystemFunctions() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#getSystemFunctions is not supported");
    }

    @Override
    public String getTimeDateFunctions() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#getTimeDateFunctions is not supported");
    }

    @Override
    public String getSearchStringEscape() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#getSearchStringEscape is not supported");
    }

    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#nullPlusNonNullIsNull is not supported");
    }

    @Override
    public boolean supportsConvert() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#supportsConvert is not supported");
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#supportsConvert is not supported");
    }

    @Override
    public boolean supportsMultipleResultSets() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#supportsMultipleResultSets is not supported");
    }

    @Override
    public boolean supportsMultipleTransactions() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#supportsMultipleTransactions is not supported");
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#supportsMinimumSQLGrammar is not supported");
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#supportsCoreSQLGrammar is not supported");
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#supportsExtendedSQLGrammar is not supported");
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#supportsANSI92EntryLevelSQL is not supported");
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#supportsANSI92IntermediateSQL is not supported");
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#supportsANSI92FullSQL is not supported");
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#supportsIntegrityEnhancementFacility is not supported");
    }

    @Override
    public String getProcedureTerm() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#getProcedureTerm is not supported");
    }

    @Override
    public boolean isCatalogAtStart() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#isCatalogAtStart is not supported");
    }

    @Override
    public String getCatalogSeparator() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#getCatalogSeparator is not supported");
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#supportsSchemasInProcedureCalls is not supported");
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#supportsSchemasInIndexDefinitions is not supported");
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#supportsSchemasInPrivilegeDefinitions is not supported");
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#supportsCatalogsInProcedureCalls is not supported");
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#supportsCatalogsInIndexDefinitions is not supported");
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#supportsCatalogsInPrivilegeDefinitions is not supported");
    }

    @Override
    public boolean supportsPositionedDelete() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#supportsPositionedDelete is not supported");
    }

    @Override
    public boolean supportsPositionedUpdate() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#supportsPositionedUpdate is not supported");
    }

    @Override
    public boolean supportsSelectForUpdate() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#supportsSelectForUpdate is not supported");
    }

    @Override
    public boolean supportsStoredProcedures() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#supportsStoredProcedures is not supported");
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#supportsOpenCursorsAcrossCommit is not supported");
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#supportsOpenCursorsAcrossRollback is not supported");
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#supportsOpenStatementsAcrossCommit is not supported");
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#supportsOpenStatementsAcrossRollback is not supported");
    }

    @Override
    public int getMaxBinaryLiteralLength() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#getMaxBinaryLiteralLength is not supported");
    }

    @Override
    public int getMaxCharLiteralLength() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#getMaxCharLiteralLength is not supported");
    }

    @Override
    public int getMaxColumnNameLength() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#getMaxColumnNameLength is not supported");
    }

    @Override
    public int getMaxColumnsInGroupBy() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#getMaxColumnsInGroupBy is not supported");
    }

    @Override
    public int getMaxColumnsInIndex() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#getMaxColumnsInIndex is not supported");
    }

    @Override
    public int getMaxColumnsInOrderBy() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#getMaxColumnsInOrderBy is not supported");
    }

    @Override
    public int getMaxColumnsInSelect() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#getMaxColumnsInSelect is not supported");
    }

    @Override
    public int getMaxColumnsInTable() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#getMaxColumnsInTable is not supported");
    }

    @Override
    public int getMaxConnections() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#getMaxConnections is not supported");
    }

    @Override
    public int getMaxCursorNameLength() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#getMaxCursorNameLength is not supported");
    }

    @Override
    public int getMaxIndexLength() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#getMaxIndexLength is not supported");
    }

    @Override
    public int getMaxSchemaNameLength() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#getMaxSchemaNameLength is not supported");
    }

    @Override
    public int getMaxProcedureNameLength() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#getMaxProcedureNameLength is not supported");
    }

    @Override
    public int getMaxCatalogNameLength() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#getMaxCatalogNameLength is not supported");
    }

    @Override
    public int getMaxRowSize() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#getMaxRowSize is not supported");
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#doesMaxRowSizeIncludeBlobs is not supported");
    }

    @Override
    public int getMaxStatementLength() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#getMaxStatementLength is not supported");
    }

    @Override
    public int getMaxStatements() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#getMaxStatements is not supported");
    }

    @Override
    public int getMaxTableNameLength() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#getMaxTableNameLength is not supported");
    }

    @Override
    public int getMaxTablesInSelect() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#getMaxTablesInSelect is not supported");
    }

    @Override
    public int getMaxUserNameLength() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#getMaxUserNameLength is not supported");
    }

    @Override
    public int getDefaultTransactionIsolation() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#getDefaultTransactionIsolation is not supported");
    }

    @Override
    public boolean supportsTransactions() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#supportsTransactions is not supported");
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#supportsTransactionIsolationLevel is not supported");
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#supportsDataDefinitionAndDataManipulationTransactions is not supported");
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#supportsDataManipulationTransactionsOnly is not supported");
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#dataDefinitionCausesTransactionCommit is not supported");
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#dataDefinitionIgnoredInTransactions is not supported");
    }

    @Override
    public ResultSet getProcedures(
            String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#getProcedures is not supported");
    }

    @Override
    public ResultSet getProcedureColumns(
            String catalog,
            String schemaPattern,
            String procedureNamePattern,
            String columnNamePattern)
            throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#getProcedureColumns is not supported");
    }

    @Override
    public ResultSet getColumnPrivileges(
            String catalog, String schema, String table, String columnNamePattern)
            throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#getColumnPrivileges is not supported");
    }

    @Override
    public ResultSet getTablePrivileges(
            String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#getTablePrivileges is not supported");
    }

    @Override
    public ResultSet getBestRowIdentifier(
            String catalog, String schema, String table, int scope, boolean nullable)
            throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#getBestRowIdentifier is not supported");
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table)
            throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#getVersionColumns is not supported");
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table)
            throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#getImportedKeys is not supported");
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table)
            throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#getExportedKeys is not supported");
    }

    @Override
    public ResultSet getCrossReference(
            String parentCatalog,
            String parentSchema,
            String parentTable,
            String foreignCatalog,
            String foreignSchema,
            String foreignTable)
            throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#getCrossReference is not supported");
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#getTypeInfo is not supported");
    }

    @Override
    public ResultSet getIndexInfo(
            String catalog, String schema, String table, boolean unique, boolean approximate)
            throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#getIndexInfo is not supported");
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#supportsResultSetConcurrency is not supported");
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#ownUpdatesAreVisible is not supported");
    }

    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#ownDeletesAreVisible is not supported");
    }

    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#ownInsertsAreVisible is not supported");
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#othersUpdatesAreVisible is not supported");
    }

    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#othersDeletesAreVisible is not supported");
    }

    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#othersInsertsAreVisible is not supported");
    }

    @Override
    public boolean updatesAreDetected(int type) throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#updatesAreDetected is not supported");
    }

    @Override
    public boolean deletesAreDetected(int type) throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#deletesAreDetected is not supported");
    }

    @Override
    public boolean insertsAreDetected(int type) throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#insertsAreDetected is not supported");
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#supportsBatchUpdates is not supported");
    }

    @Override
    public ResultSet getUDTs(
            String catalog, String schemaPattern, String typeNamePattern, int[] types)
            throws SQLException {
        throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#getUDTs is not supported");
    }

    @Override
    public boolean supportsSavepoints() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#supportsSavepoints is not supported");
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#supportsNamedParameters is not supported");
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#supportsMultipleOpenResults is not supported");
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#supportsGetGeneratedKeys is not supported");
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern)
            throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#getSuperTypes is not supported");
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern)
            throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#getSuperTypes is not supported");
    }

    @Override
    public ResultSet getAttributes(
            String catalog,
            String schemaPattern,
            String typeNamePattern,
            String attributeNamePattern)
            throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#getAttributes is not supported");
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#supportsResultSetHoldability is not supported");
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#getResultSetHoldability is not supported");
    }

    @Override
    public int getSQLStateType() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#getSQLStateType is not supported");
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#locatorsUpdateCopy is not supported");
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#supportsStatementPooling is not supported");
    }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#getRowIdLifetime is not supported");
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#supportsStoredFunctionsUsingCallSyntax is not supported");
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#autoCommitFailureClosesAllResultSets is not supported");
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#getClientInfoProperties is not supported");
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern)
            throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#getFunctions is not supported");
    }

    @Override
    public ResultSet getFunctionColumns(
            String catalog,
            String schemaPattern,
            String functionNamePattern,
            String columnNamePattern)
            throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#getFunctionColumns is not supported");
    }

    @Override
    public ResultSet getPseudoColumns(
            String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
            throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#getPseudoColumns is not supported");
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#generatedKeyAlwaysReturned is not supported");
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#unwrap is not supported");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDatabaseMetaData#isWrapperFor is not supported");
    }
}
