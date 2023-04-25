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
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.StatementResult;

import javax.annotation.Nullable;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.jdbc.utils.DatabaseMetaDataUtils.createCatalogsResultSet;
import static org.apache.flink.table.jdbc.utils.DatabaseMetaDataUtils.createSchemasResultSet;

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
        try {
            String currentCatalog = connection.getCatalog();
            String currentDatabase = connection.getSchema();
            List<String> catalogList = new ArrayList<>();
            Map<String, List<String>> catalogSchemaList = new HashMap<>();
            try (StatementResult result = catalogs()) {
                while (result.hasNext()) {
                    String catalog = result.next().getString(0).toString();
                    connection.setCatalog(catalog);
                    getSchemasForCatalog(catalogList, catalogSchemaList, catalog, null);
                }
            }
            connection.setCatalog(currentCatalog);
            connection.setSchema(currentDatabase);

            return createSchemasResultSet(statement, catalogList, catalogSchemaList);
        } catch (Exception e) {
            throw new SQLException("Get schemas fail", e);
        }
    }

    private void getSchemasForCatalog(
            List<String> catalogList,
            Map<String, List<String>> catalogSchemaList,
            String catalog,
            @Nullable String schemaPattern)
            throws SQLException {
        catalogList.add(catalog);
        List<String> schemas = new ArrayList<>();
        try (StatementResult schemaResult = schemas()) {
            while (schemaResult.hasNext()) {
                String schema = schemaResult.next().getString(0).toString();
                if (schemaPattern == null || schema.contains(schemaPattern)) {
                    schemas.add(schema);
                }
            }
        }
        catalogSchemaList.put(catalog, schemas);
    }

    private StatementResult schemas() {
        return executor.executeStatement("SHOW DATABASES;");
    }

    // TODO Flink will support SHOW DATABASES LIKE statement in FLIP-297, this method will be
    // supported after that issue.
    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ResultSet getTables(
            String catalog, String schemaPattern, String tableNamePattern, String[] types)
            throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ResultSet getColumns(
            String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
            throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table)
            throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connection;
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
    public boolean usesLocalFiles() throws SQLException {
        return false;
    }

    @Override
    public boolean usesLocalFilePerTable() throws SQLException {
        return false;
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
    public boolean supportsNonNullableColumns() throws SQLException {
        return true;
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

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return true;
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
}
