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

package org.apache.flink.connector.jdbc.catalog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.PASSWORD;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.TABLE_NAME;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.URL;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.USERNAME;
import static org.apache.flink.connector.jdbc.table.JdbcDynamicTableFactory.IDENTIFIER;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;

/** Catalog for PostgreSQL. */
@Internal
public class PostgresCatalog extends AbstractJdbcCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresCatalog.class);

    public static final String DEFAULT_DATABASE = "postgres";

    // ------ Postgres default objects that shouldn't be exposed to users ------

    private static final Set<String> builtinDatabases =
            new HashSet<String>() {
                {
                    add("template0");
                    add("template1");
                }
            };

    private static final Set<String> builtinSchemas =
            new HashSet<String>() {
                {
                    add("pg_toast");
                    add("pg_temp_1");
                    add("pg_toast_temp_1");
                    add("pg_catalog");
                    add("information_schema");
                }
            };

    protected PostgresCatalog(
            String catalogName,
            String defaultDatabase,
            String username,
            String pwd,
            String baseUrl) {
        super(catalogName, defaultDatabase, username, pwd, baseUrl);
    }

    // ------ databases ------

    @Override
    public List<String> listDatabases() throws CatalogException {
        List<String> pgDatabases = new ArrayList<>();

        try (Connection conn = DriverManager.getConnection(defaultUrl, username, pwd)) {

            PreparedStatement ps = conn.prepareStatement("SELECT datname FROM pg_database;");

            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                String dbName = rs.getString(1);
                if (!builtinDatabases.contains(dbName)) {
                    pgDatabases.add(rs.getString(1));
                }
            }

            return pgDatabases;
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed listing database in catalog %s", getName()), e);
        }
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        if (listDatabases().contains(databaseName)) {
            return new CatalogDatabaseImpl(Collections.emptyMap(), null);
        } else {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
    }

    // ------ tables ------

    @Override
    public List<String> listTables(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }

        // get all schemas
        try (Connection conn = DriverManager.getConnection(baseUrl + databaseName, username, pwd)) {
            PreparedStatement ps =
                    conn.prepareStatement("SELECT schema_name FROM information_schema.schemata;");

            ResultSet rs = ps.executeQuery();

            List<String> schemas = new ArrayList<>();

            while (rs.next()) {
                String pgSchema = rs.getString(1);
                if (!builtinSchemas.contains(pgSchema)) {
                    schemas.add(pgSchema);
                }
            }

            List<String> tables = new ArrayList<>();

            for (String schema : schemas) {
                PreparedStatement stmt =
                        conn.prepareStatement(
                                "SELECT * \n"
                                        + "FROM information_schema.tables \n"
                                        + "WHERE table_type = 'BASE TABLE' \n"
                                        + "    AND table_schema = ? \n"
                                        + "ORDER BY table_type, table_name;");

                stmt.setString(1, schema);

                ResultSet rstables = stmt.executeQuery();

                while (rstables.next()) {
                    // position 1 is database name, position 2 is schema name, position 3 is table
                    // name
                    tables.add(schema + "." + rstables.getString(3));
                }
            }

            return tables;
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed listing database in catalog %s", getName()), e);
        }
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(getName(), tablePath);
        }

        PostgresTablePath pgPath = PostgresTablePath.fromFlinkTableName(tablePath.getObjectName());

        String dbUrl = baseUrl + tablePath.getDatabaseName();
        try (Connection conn = DriverManager.getConnection(dbUrl, username, pwd)) {
            DatabaseMetaData metaData = conn.getMetaData();
            Optional<UniqueConstraint> primaryKey =
                    getPrimaryKey(metaData, pgPath.getPgSchemaName(), pgPath.getPgTableName());

            PreparedStatement ps =
                    conn.prepareStatement(String.format("SELECT * FROM %s;", pgPath.getFullPath()));

            ResultSetMetaData rsmd = ps.getMetaData();

            String[] names = new String[rsmd.getColumnCount()];
            DataType[] types = new DataType[rsmd.getColumnCount()];

            for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                names[i - 1] = rsmd.getColumnName(i);
                types[i - 1] = fromJDBCType(rsmd, i);
                if (rsmd.isNullable(i) == ResultSetMetaData.columnNoNulls) {
                    types[i - 1] = types[i - 1].notNull();
                }
            }

            TableSchema.Builder tableBuilder = new TableSchema.Builder().fields(names, types);
            primaryKey.ifPresent(
                    pk ->
                            tableBuilder.primaryKey(
                                    pk.getName(), pk.getColumns().toArray(new String[0])));
            TableSchema tableSchema = tableBuilder.build();

            Map<String, String> props = new HashMap<>();
            props.put(CONNECTOR.key(), IDENTIFIER);
            props.put(URL.key(), dbUrl);
            props.put(TABLE_NAME.key(), pgPath.getFullPath());
            props.put(USERNAME.key(), username);
            props.put(PASSWORD.key(), pwd);

            return new CatalogTableImpl(tableSchema, props, "");
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed getting table %s", tablePath.getFullName()), e);
        }
    }

    // Postgres jdbc driver maps several alias to real type, we use real type rather than alias:
    // serial2 <=> int2
    // smallserial <=> int2
    // serial4 <=> serial
    // serial8 <=> bigserial
    // smallint <=> int2
    // integer <=> int4
    // int <=> int4
    // bigint <=> int8
    // float <=> float8
    // boolean <=> bool
    // decimal <=> numeric
    public static final String PG_SERIAL = "serial";
    public static final String PG_BIGSERIAL = "bigserial";
    public static final String PG_BYTEA = "bytea";
    public static final String PG_BYTEA_ARRAY = "_bytea";
    public static final String PG_SMALLINT = "int2";
    public static final String PG_SMALLINT_ARRAY = "_int2";
    public static final String PG_INTEGER = "int4";
    public static final String PG_INTEGER_ARRAY = "_int4";
    public static final String PG_BIGINT = "int8";
    public static final String PG_BIGINT_ARRAY = "_int8";
    public static final String PG_REAL = "float4";
    public static final String PG_REAL_ARRAY = "_float4";
    public static final String PG_DOUBLE_PRECISION = "float8";
    public static final String PG_DOUBLE_PRECISION_ARRAY = "_float8";
    public static final String PG_NUMERIC = "numeric";
    public static final String PG_NUMERIC_ARRAY = "_numeric";
    public static final String PG_BOOLEAN = "bool";
    public static final String PG_BOOLEAN_ARRAY = "_bool";
    public static final String PG_TIMESTAMP = "timestamp";
    public static final String PG_TIMESTAMP_ARRAY = "_timestamp";
    public static final String PG_TIMESTAMPTZ = "timestamptz";
    public static final String PG_TIMESTAMPTZ_ARRAY = "_timestamptz";
    public static final String PG_DATE = "date";
    public static final String PG_DATE_ARRAY = "_date";
    public static final String PG_TIME = "time";
    public static final String PG_TIME_ARRAY = "_time";
    public static final String PG_TEXT = "text";
    public static final String PG_TEXT_ARRAY = "_text";
    public static final String PG_CHAR = "bpchar";
    public static final String PG_CHAR_ARRAY = "_bpchar";
    public static final String PG_CHARACTER = "character";
    public static final String PG_CHARACTER_ARRAY = "_character";
    public static final String PG_CHARACTER_VARYING = "varchar";
    public static final String PG_CHARACTER_VARYING_ARRAY = "_varchar";

    /**
     * Converts Postgres type to Flink {@link DataType}.
     *
     * @see org.postgresql.jdbc.TypeInfoCache
     */
    private DataType fromJDBCType(ResultSetMetaData metadata, int colIndex) throws SQLException {
        String pgType = metadata.getColumnTypeName(colIndex);

        int precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);

        switch (pgType) {
            case PG_BOOLEAN:
                return DataTypes.BOOLEAN();
            case PG_BOOLEAN_ARRAY:
                return DataTypes.ARRAY(DataTypes.BOOLEAN());
            case PG_BYTEA:
                return DataTypes.BYTES();
            case PG_BYTEA_ARRAY:
                return DataTypes.ARRAY(DataTypes.BYTES());
            case PG_SMALLINT:
                return DataTypes.SMALLINT();
            case PG_SMALLINT_ARRAY:
                return DataTypes.ARRAY(DataTypes.SMALLINT());
            case PG_INTEGER:
            case PG_SERIAL:
                return DataTypes.INT();
            case PG_INTEGER_ARRAY:
                return DataTypes.ARRAY(DataTypes.INT());
            case PG_BIGINT:
            case PG_BIGSERIAL:
                return DataTypes.BIGINT();
            case PG_BIGINT_ARRAY:
                return DataTypes.ARRAY(DataTypes.BIGINT());
            case PG_REAL:
                return DataTypes.FLOAT();
            case PG_REAL_ARRAY:
                return DataTypes.ARRAY(DataTypes.FLOAT());
            case PG_DOUBLE_PRECISION:
                return DataTypes.DOUBLE();
            case PG_DOUBLE_PRECISION_ARRAY:
                return DataTypes.ARRAY(DataTypes.DOUBLE());
            case PG_NUMERIC:
                // see SPARK-26538: handle numeric without explicit precision and scale.
                if (precision > 0) {
                    return DataTypes.DECIMAL(precision, metadata.getScale(colIndex));
                }
                return DataTypes.DECIMAL(DecimalType.MAX_PRECISION, 18);
            case PG_NUMERIC_ARRAY:
                // see SPARK-26538: handle numeric without explicit precision and scale.
                if (precision > 0) {
                    return DataTypes.ARRAY(
                            DataTypes.DECIMAL(precision, metadata.getScale(colIndex)));
                }
                return DataTypes.ARRAY(DataTypes.DECIMAL(DecimalType.MAX_PRECISION, 18));
            case PG_CHAR:
            case PG_CHARACTER:
                return DataTypes.CHAR(precision);
            case PG_CHAR_ARRAY:
            case PG_CHARACTER_ARRAY:
                return DataTypes.ARRAY(DataTypes.CHAR(precision));
            case PG_CHARACTER_VARYING:
                return DataTypes.VARCHAR(precision);
            case PG_CHARACTER_VARYING_ARRAY:
                return DataTypes.ARRAY(DataTypes.VARCHAR(precision));
            case PG_TEXT:
                return DataTypes.STRING();
            case PG_TEXT_ARRAY:
                return DataTypes.ARRAY(DataTypes.STRING());
            case PG_TIMESTAMP:
                return DataTypes.TIMESTAMP(scale);
            case PG_TIMESTAMP_ARRAY:
                return DataTypes.ARRAY(DataTypes.TIMESTAMP(scale));
            case PG_TIMESTAMPTZ:
                return DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(scale);
            case PG_TIMESTAMPTZ_ARRAY:
                return DataTypes.ARRAY(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(scale));
            case PG_TIME:
                return DataTypes.TIME(scale);
            case PG_TIME_ARRAY:
                return DataTypes.ARRAY(DataTypes.TIME(scale));
            case PG_DATE:
                return DataTypes.DATE();
            case PG_DATE_ARRAY:
                return DataTypes.ARRAY(DataTypes.DATE());
            default:
                throw new UnsupportedOperationException(
                        String.format("Doesn't support Postgres type '%s' yet", pgType));
        }
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {

        List<String> tables = null;
        try {
            tables = listTables(tablePath.getDatabaseName());
        } catch (DatabaseNotExistException e) {
            return false;
        }

        return tables.contains(
                PostgresTablePath.fromFlinkTableName(tablePath.getObjectName()).getFullPath());
    }
}
