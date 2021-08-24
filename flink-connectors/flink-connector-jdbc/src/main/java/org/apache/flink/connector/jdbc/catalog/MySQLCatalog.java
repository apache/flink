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

import org.apache.flink.api.common.functions.FilterFunction;
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
import org.apache.flink.util.Preconditions;

import org.apache.commons.compress.utils.Lists;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.PASSWORD;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.TABLE_NAME;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.URL;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.USERNAME;
import static org.apache.flink.connector.jdbc.table.JdbcDynamicTableFactory.IDENTIFIER;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;

/** Catalog for MySQL. */
public class MySQLCatalog extends AbstractJdbcCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(MySQLCatalog.class);

    private final String databaseVersion;
    private final String driverVersion;

    // ============================data types=====================

    public static final String MYSQL_UNKNOWN = "UNKNOWN";
    public static final String MYSQL_BIT = "BIT";

    // -------------------------number----------------------------
    public static final String MYSQL_TINYINT = "TINYINT";
    public static final String MYSQL_TINYINT_UNSIGNED = "TINYINT UNSIGNED";
    public static final String MYSQL_SMALLINT = "SMALLINT";
    public static final String MYSQL_SMALLINT_UNSIGNED = "SMALLINT UNSIGNED";
    public static final String MYSQL_MEDIUMINT = "MEDIUMINT";
    public static final String MYSQL_MEDIUMINT_UNSIGNED = "MEDIUMINT UNSIGNED";
    public static final String MYSQL_INT = "INT";
    public static final String MYSQL_INT_UNSIGNED = "INT UNSIGNED";
    public static final String MYSQL_INTEGER = "INTEGER";
    public static final String MYSQL_INTEGER_UNSIGNED = "INTEGER UNSIGNED";
    public static final String MYSQL_BIGINT = "BIGINT";
    public static final String MYSQL_BIGINT_UNSIGNED = "BIGINT UNSIGNED";
    public static final String MYSQL_DECIMAL = "DECIMAL";
    public static final String MYSQL_DECIMAL_UNSIGNED = "DECIMAL UNSIGNED";
    public static final String MYSQL_FLOAT = "FLOAT";
    public static final String MYSQL_FLOAT_UNSIGNED = "FLOAT UNSIGNED";
    public static final String MYSQL_DOUBLE = "DOUBLE";
    public static final String MYSQL_DOUBLE_UNSIGNED = "DOUBLE UNSIGNED";

    // -------------------------string----------------------------
    public static final String MYSQL_CHAR = "CHAR";
    public static final String MYSQL_VARCHAR = "VARCHAR";
    public static final String MYSQL_TINYTEXT = "TINYTEXT";
    public static final String MYSQL_MEDIUMTEXT = "MEDIUMTEXT";
    public static final String MYSQL_TEXT = "TEXT";
    public static final String MYSQL_LONGTEXT = "LONGTEXT";
    public static final String MYSQL_JSON = "JSON";

    // ------------------------------time-------------------------
    public static final String MYSQL_DATE = "DATE";
    public static final String MYSQL_DATETIME = "DATETIME";
    public static final String MYSQL_TIME = "TIME";
    public static final String MYSQL_TIMESTAMP = "TIMESTAMP";
    public static final String MYSQL_YEAR = "YEAR";

    // ------------------------------blob-------------------------
    public static final String MYSQL_TINYBLOB = "TINYBLOB";
    public static final String MYSQL_MEDIUMBLOB = "MEDIUMBLOB";
    public static final String MYSQL_BLOB = "BLOB";
    public static final String MYSQL_LONGBLOB = "LONGBLOB";
    public static final String MYSQL_BINARY = "BINARY";
    public static final String MYSQL_VARBINARY = "VARBINARY";
    public static final String MYSQL_GEOMETRY = "GEOMETRY";

    // column class names
    public static final String COLUMN_CLASS_BOOLEAN = "java.lang.Boolean";
    public static final String COLUMN_CLASS_INTEGER = "java.lang.Integer";
    public static final String COLUMN_CLASS_BIG_INTEGER = "java.math.BigInteger";
    public static final String COLUMN_CLASS_LONG = "java.lang.Long";
    public static final String COLUMN_CLASS_FLOAT = "java.lang.Float";
    public static final String COLUMN_CLASS_DOUBLE = "java.lang.Double";
    public static final String COLUMN_CLASS_BIG_DECIMAL = "java.math.BigDecimal";
    public static final String COLUMN_CLASS_BYTE_ARRAY = "[B";
    public static final String COLUMN_CLASS_STRING = "java.lang.String";
    public static final String COLUMN_CLASS_DATE = "java.sql.Date";
    public static final String COLUMN_CLASS_TIME = "java.sql.Time";
    public static final String COLUMN_CLASS_TIMESTAMP = "java.sql.Timestamp";

    public static final int RAW_TIME_LENGTH = 10;
    public static final int RAW_TIMESTAMP_LENGTH = 19;

    private static final Set<String> builtinDatabases =
            new HashSet<String>() {
                {
                    add("information_schema");
                    add("mysql");
                    add("performance_schema");
                    add("sys");
                }
            };

    public MySQLCatalog(
            String catalogName,
            String defaultDatabase,
            String username,
            String pwd,
            String baseUrl) {
        super(catalogName, defaultDatabase, username, pwd, baseUrl);
        this.driverVersion =
                Preconditions.checkNotNull(getDriverVersion(), "driver version must not be null.");
        this.databaseVersion =
                Preconditions.checkNotNull(
                        getDatabaseVersion(), "database version must not be null.");
        LOG.info("Driver version: {}, database version: {}", driverVersion, databaseVersion);
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        String sql = "SELECT `SCHEMA_NAME` FROM `INFORMATION_SCHEMA`.`SCHEMATA`;";
        return extractColumnValuesBySQL(
                defaultUrl,
                sql,
                1,
                (FilterFunction<String>) dbName -> !builtinDatabases.contains(dbName));
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        Preconditions.checkState(
                StringUtils.isNotBlank(databaseName), "Database name must not be blank.");
        if (listDatabases().contains(databaseName)) {
            return new CatalogDatabaseImpl(Collections.emptyMap(), null);
        } else {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
    }

    @Override
    public List<String> listTables(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        Preconditions.checkState(
                StringUtils.isNotBlank(databaseName), "Database name must not be blank.");
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
        String connUrl = baseUrl + databaseName;
        String sql =
                String.format(
                        "SELECT TABLE_NAME FROM information_schema.`TABLES` WHERE TABLE_SCHEMA = '%s'",
                        databaseName);
        return extractColumnValuesBySQL(connUrl, sql, 1, null);
    }

    // ------ retrieve PK constraint ------

    private Optional<UniqueConstraint> getPrimaryKey(
            DatabaseMetaData metaData, String schema, ObjectPath table) throws SQLException {

        // According to the Javadoc of java.sql.DatabaseMetaData#getPrimaryKeys,
        // the returned primary key columns are ordered by COLUMN_NAME, not by KEY_SEQ.
        // We need to sort them based on the KEY_SEQ value.
        ResultSet rs =
                metaData.getPrimaryKeys(table.getDatabaseName(), schema, table.getObjectName());

        Map<Integer, String> keySeqColumnName = new HashMap<>();
        String pkName = null;
        while (rs.next()) {
            String columnName = rs.getString("COLUMN_NAME");
            pkName = rs.getString("PK_NAME"); // all the PK_NAME should be the same
            int keySeq = rs.getInt("KEY_SEQ");
            keySeqColumnName.put(keySeq - 1, columnName); // KEY_SEQ is 1-based index
        }
        List<String> pkFields =
                Arrays.asList(new String[keySeqColumnName.size()]); // initialize size
        keySeqColumnName.forEach(pkFields::set);
        if (!pkFields.isEmpty()) {
            // PK_NAME maybe null according to the javadoc, generate an unique name in that case
            pkName = pkName == null ? "pk_" + String.join("_", pkFields) : pkName;
            return Optional.of(UniqueConstraint.primaryKey(pkName, pkFields));
        }
        return Optional.empty();
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(getName(), tablePath);
        }
        try (Connection conn =
                        DriverManager.getConnection(
                                baseUrl + tablePath.getDatabaseName(), username, pwd);
                PreparedStatement ps =
                        conn.prepareStatement(
                                String.format("SELECT * FROM %s;", tablePath.getObjectName()))) {
            DatabaseMetaData metaData = conn.getMetaData();
            Optional<UniqueConstraint> primaryKey = getPrimaryKey(metaData, null, tablePath);
            ResultSetMetaData resultSetMetaData = ps.getMetaData();

            String[] columnsClassnames = new String[resultSetMetaData.getColumnCount()];
            DataType[] types = new DataType[resultSetMetaData.getColumnCount()];
            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                columnsClassnames[i - 1] = resultSetMetaData.getColumnName(i);
                types[i - 1] = fromJDBCType(tablePath, resultSetMetaData, i);
                if (resultSetMetaData.isNullable(i) == ResultSetMetaData.columnNoNulls) {
                    types[i - 1] = types[i - 1].notNull();
                }
            }
            TableSchema.Builder tableBuilder =
                    new TableSchema.Builder().fields(columnsClassnames, types);
            primaryKey.ifPresent(
                    pk ->
                            tableBuilder.primaryKey(
                                    pk.getName(), pk.getColumns().toArray(new String[0])));
            TableSchema tableSchema = tableBuilder.build();
            Map<String, String> props = new HashMap<>();
            props.put(CONNECTOR.key(), IDENTIFIER);
            props.put(URL.key(), baseUrl + tablePath.getDatabaseName());
            props.put(TABLE_NAME.key(), tablePath.getObjectName());
            props.put(USERNAME.key(), username);
            props.put(PASSWORD.key(), pwd);
            ps.close();
            return new CatalogTableImpl(tableSchema, props, "");
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed getting table %s", tablePath.getFullName()), e);
        }
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        try {
            return listTables(tablePath.getDatabaseName()).contains(tablePath.getObjectName());
        } catch (DatabaseNotExistException e) {
            return false;
        }
    }

    private List<String> extractColumnValuesBySQL(
            String connUrl, String sql, int columnIndex, FilterFunction<String> filterFunc) {
        List<String> columnValues = Lists.newArrayList();
        try (Connection conn = DriverManager.getConnection(connUrl, username, pwd);
                PreparedStatement ps = conn.prepareStatement(sql);
                ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                String columnValue = rs.getString(columnIndex);
                if (Objects.isNull(filterFunc) || filterFunc.filter(columnValue)) {
                    columnValues.add(columnValue);
                }
            }
            return columnValues;
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed in processing query sql %s, connUrl %s", sql, connUrl),
                    e);
        }
    }

    private String getDatabaseVersion() {
        try (Connection conn = DriverManager.getConnection(defaultUrl, username, pwd)) {
            return conn.getMetaData().getDatabaseProductVersion();
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed in getting MySQL version by %s.", defaultUrl), e);
        }
    }

    private String getDriverVersion() {
        try (Connection conn = DriverManager.getConnection(defaultUrl, username, pwd)) {
            String driverVersion = conn.getMetaData().getDriverVersion();
            Pattern regexp = Pattern.compile("\\d+?\\.\\d+?\\.\\d+");
            Matcher matcher = regexp.matcher(driverVersion);
            return matcher.find() ? matcher.group(0) : null;
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed in getting mysql driver version by %s.", defaultUrl), e);
        }
    }

    /** Converts MySQL type to Flink {@link DataType}. */
    private DataType fromJDBCType(ObjectPath tablePath, ResultSetMetaData metadata, int colIndex)
            throws SQLException {
        String mysqlType = metadata.getColumnTypeName(colIndex).toUpperCase();
        String columnName = metadata.getColumnName(colIndex);
        int precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);

        switch (mysqlType) {
            case MYSQL_BIT:
                return DataTypes.BOOLEAN();
            case MYSQL_TINYBLOB:
            case MYSQL_MEDIUMBLOB:
            case MYSQL_BLOB:
            case MYSQL_LONGBLOB:
            case MYSQL_VARBINARY:
            case MYSQL_BINARY:
                return DataTypes.BYTES();
            case MYSQL_TINYINT:
                return DataTypes.TINYINT();
            case MYSQL_TINYINT_UNSIGNED:
            case MYSQL_SMALLINT:
                return DataTypes.SMALLINT();
            case MYSQL_SMALLINT_UNSIGNED:
            case MYSQL_MEDIUMINT:
            case MYSQL_MEDIUMINT_UNSIGNED:
            case MYSQL_INT:
            case MYSQL_INTEGER:
                return DataTypes.INT();
            case MYSQL_INT_UNSIGNED:
            case MYSQL_INTEGER_UNSIGNED:
            case MYSQL_BIGINT:
                return DataTypes.BIGINT();
            case MYSQL_BIGINT_UNSIGNED:
                return DataTypes.DECIMAL(20, 0);
            case MYSQL_DECIMAL:
                return DataTypes.DECIMAL(precision, scale);
            case MYSQL_DECIMAL_UNSIGNED:
                checkMaxPrecision(tablePath, columnName, precision);
                return DataTypes.DECIMAL(precision + 1, scale);
            case MYSQL_FLOAT:
                return DataTypes.FLOAT();
            case MYSQL_FLOAT_UNSIGNED:
                LOG.warn("{} will probably cause value overflow.", MYSQL_FLOAT_UNSIGNED);
                return DataTypes.FLOAT();
            case MYSQL_DOUBLE:
                return DataTypes.DOUBLE();
            case MYSQL_DOUBLE_UNSIGNED:
                LOG.warn("{} will probably cause value overflow.", MYSQL_DOUBLE_UNSIGNED);
                return DataTypes.DOUBLE();
            case MYSQL_CHAR:
            case MYSQL_VARCHAR:
            case MYSQL_TINYTEXT:
            case MYSQL_MEDIUMTEXT:
            case MYSQL_TEXT:
            case MYSQL_JSON:
                return DataTypes.STRING();
            case MYSQL_LONGTEXT:
                LOG.warn(
                        "The max precision of type '{}' in mysql is 536870911, and the max precision here has to be set as 2147483647 due to the limitation of the flink sql types system.",
                        MYSQL_LONGTEXT);
                return DataTypes.STRING();
            case MYSQL_YEAR:
                LOG.warn(
                        "The type {} in mysql catalog is supported in read-mode, but not in write-mode.",
                        MYSQL_YEAR);
                return DataTypes.DATE();
            case MYSQL_DATE:
                return DataTypes.DATE();
            case MYSQL_TIME:
                return isExplicitPrecision(precision, RAW_TIME_LENGTH)
                        ? DataTypes.TIME(precision - RAW_TIME_LENGTH - 1)
                        : DataTypes.TIME(0);
            case MYSQL_DATETIME:
            case MYSQL_TIMESTAMP:
                return isExplicitPrecision(precision, RAW_TIMESTAMP_LENGTH)
                        ? DataTypes.TIMESTAMP(precision - RAW_TIMESTAMP_LENGTH - 1)
                        : DataTypes.TIMESTAMP(0);
            case MYSQL_GEOMETRY:
                LOG.warn(
                        "{} type in mysql catalog is supported in read-mode by the form of bytes, but not in write-mode.",
                        MYSQL_GEOMETRY);
                return DataTypes.BYTES();
            case MYSQL_UNKNOWN:
                return fromJDBCClassType(tablePath, metadata, colIndex);
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Doesn't support mysql type '%s' in mysql version %s, driver version %s yet.",
                                mysqlType, databaseVersion, driverVersion));
        }
    }

    private boolean isExplicitPrecision(int precision, int defaultPrecision) {
        return precision > defaultPrecision && precision - defaultPrecision - 1 <= 9;
    }

    private void checkMaxPrecision(ObjectPath tablePath, String columnName, int precision) {
        if (precision >= DecimalType.MAX_PRECISION) {
            throw new CatalogException(
                    String.format(
                            "Precision %s of table %s column name %s in type %s exceeds DecimalType.MAX_PRECISION %s.",
                            precision,
                            tablePath.getFullName(),
                            columnName,
                            MYSQL_DECIMAL_UNSIGNED,
                            DecimalType.MAX_PRECISION));
        }
    }

    /** Converts MySQL types to Flink {@link DataType}. */
    private DataType fromJDBCClassType(
            ObjectPath tablePath, ResultSetMetaData metadata, int colIndex) throws SQLException {
        final String jdbcColumnClassType = metadata.getColumnClassName(colIndex);
        final String jdbcColumnName = metadata.getColumnName(colIndex);
        int precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);
        LOG.warn(
                "Column {} in {} of mysql database, jdbcColumnClassType: {}, jdbcColumnType: {}, precision: {}, scale: {}, will use jdbc column class name to inference the type mapping. When the field '{}' of source table '{}' is unsigned numeric, please pay attention to the risk of value overflow.",
                metadata.getColumnName(colIndex),
                tablePath.getFullName(),
                jdbcColumnClassType,
                metadata.getColumnTypeName(colIndex),
                precision,
                scale,
                jdbcColumnName,
                tablePath.getFullName());

        switch (jdbcColumnClassType) {
            case COLUMN_CLASS_BYTE_ARRAY:
                return DataTypes.BYTES();
            case COLUMN_CLASS_STRING:
                return DataTypes.STRING();
            case COLUMN_CLASS_BOOLEAN:
                // If set type to boolean, it will cause a cast value error.
                return DataTypes.BOOLEAN();
            case COLUMN_CLASS_INTEGER:
                return DataTypes.INT();
            case COLUMN_CLASS_BIG_INTEGER:
                return DataTypes.DECIMAL(precision + 1, 0);
            case COLUMN_CLASS_LONG:
                return DataTypes.BIGINT();
            case COLUMN_CLASS_FLOAT:
                return DataTypes.FLOAT();
            case COLUMN_CLASS_DOUBLE:
                return DataTypes.DOUBLE();
            case COLUMN_CLASS_BIG_DECIMAL:
                return DataTypes.DECIMAL(precision, scale);
            case COLUMN_CLASS_DATE:
                LOG.warn(
                        "If the mysql type is defined with 'YEAR' type, mysql catalog is supported in read-mode, but not in write-mode.");
                return DataTypes.DATE();
            case COLUMN_CLASS_TIME:
                return isExplicitPrecision(precision, RAW_TIME_LENGTH)
                        ? DataTypes.TIME(precision - RAW_TIME_LENGTH - 1)
                        : DataTypes.TIME();
            case COLUMN_CLASS_TIMESTAMP:
                return isExplicitPrecision(precision, RAW_TIMESTAMP_LENGTH)
                        ? DataTypes.TIMESTAMP(precision - RAW_TIMESTAMP_LENGTH - 1)
                        : DataTypes.TIMESTAMP();
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Doesn't support mysql column class type '%s' in mysql version %s, driver version %s yet.",
                                jdbcColumnClassType, databaseVersion, driverVersion));
        }
    }
}
