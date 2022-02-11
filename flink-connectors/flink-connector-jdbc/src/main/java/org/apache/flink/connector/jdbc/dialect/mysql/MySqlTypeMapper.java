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

package org.apache.flink.connector.jdbc.dialect.mysql;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.dialect.JdbcDialectTypeMapper;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/** MySQLTypeMapper util class. */
@Internal
public class MySqlTypeMapper implements JdbcDialectTypeMapper {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlTypeMapper.class);

    // ============================data types=====================

    private static final String MYSQL_UNKNOWN = "UNKNOWN";
    private static final String MYSQL_BIT = "BIT";

    // -------------------------number----------------------------
    private static final String MYSQL_TINYINT = "TINYINT";
    private static final String MYSQL_TINYINT_UNSIGNED = "TINYINT UNSIGNED";
    private static final String MYSQL_SMALLINT = "SMALLINT";
    private static final String MYSQL_SMALLINT_UNSIGNED = "SMALLINT UNSIGNED";
    private static final String MYSQL_MEDIUMINT = "MEDIUMINT";
    private static final String MYSQL_MEDIUMINT_UNSIGNED = "MEDIUMINT UNSIGNED";
    private static final String MYSQL_INT = "INT";
    private static final String MYSQL_INT_UNSIGNED = "INT UNSIGNED";
    private static final String MYSQL_INTEGER = "INTEGER";
    private static final String MYSQL_INTEGER_UNSIGNED = "INTEGER UNSIGNED";
    private static final String MYSQL_BIGINT = "BIGINT";
    private static final String MYSQL_BIGINT_UNSIGNED = "BIGINT UNSIGNED";
    private static final String MYSQL_DECIMAL = "DECIMAL";
    private static final String MYSQL_DECIMAL_UNSIGNED = "DECIMAL UNSIGNED";
    private static final String MYSQL_FLOAT = "FLOAT";
    private static final String MYSQL_FLOAT_UNSIGNED = "FLOAT UNSIGNED";
    private static final String MYSQL_DOUBLE = "DOUBLE";
    private static final String MYSQL_DOUBLE_UNSIGNED = "DOUBLE UNSIGNED";

    // -------------------------string----------------------------
    private static final String MYSQL_CHAR = "CHAR";
    private static final String MYSQL_VARCHAR = "VARCHAR";
    private static final String MYSQL_TINYTEXT = "TINYTEXT";
    private static final String MYSQL_MEDIUMTEXT = "MEDIUMTEXT";
    private static final String MYSQL_TEXT = "TEXT";
    private static final String MYSQL_LONGTEXT = "LONGTEXT";
    private static final String MYSQL_JSON = "JSON";

    // ------------------------------time-------------------------
    private static final String MYSQL_DATE = "DATE";
    private static final String MYSQL_DATETIME = "DATETIME";
    private static final String MYSQL_TIME = "TIME";
    private static final String MYSQL_TIMESTAMP = "TIMESTAMP";
    private static final String MYSQL_YEAR = "YEAR";

    // ------------------------------blob-------------------------
    private static final String MYSQL_TINYBLOB = "TINYBLOB";
    private static final String MYSQL_MEDIUMBLOB = "MEDIUMBLOB";
    private static final String MYSQL_BLOB = "BLOB";
    private static final String MYSQL_LONGBLOB = "LONGBLOB";
    private static final String MYSQL_BINARY = "BINARY";
    private static final String MYSQL_VARBINARY = "VARBINARY";
    private static final String MYSQL_GEOMETRY = "GEOMETRY";

    private static final int RAW_TIME_LENGTH = 10;
    private static final int RAW_TIMESTAMP_LENGTH = 19;

    private final String databaseVersion;
    private final String driverVersion;

    public MySqlTypeMapper(String databaseVersion, String driverVersion) {
        this.databaseVersion = databaseVersion;
        this.driverVersion = driverVersion;
    }

    @Override
    public DataType mapping(ObjectPath tablePath, ResultSetMetaData metadata, int colIndex)
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
                // BINARY is not supported in MySqlDialect now.
                // VARBINARY(n) is not supported in MySqlDialect when 'n' is not equals to
                // Integer.MAX_VALUE. Please see
                // org.apache.flink.connector.jdbc.dialect.mysql.MySqlDialect#supportedTypes and
                // org.apache.flink.connector.jdbc.dialect.AbstractDialect#validate for more
                // details.
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
                return DataTypes.CHAR(precision);
            case MYSQL_VARCHAR:
            case MYSQL_TINYTEXT:
            case MYSQL_MEDIUMTEXT:
            case MYSQL_TEXT:
                return DataTypes.VARCHAR(precision);
            case MYSQL_JSON:
                return DataTypes.STRING();
            case MYSQL_LONGTEXT:
                LOG.warn(
                        "Type '{}' has a maximum precision of 536870911 in MySQL. "
                                + "Due to limitations in the Flink type system, "
                                + "the precision will be set to 2147483647.",
                        MYSQL_LONGTEXT);
                return DataTypes.STRING();
            case MYSQL_DATE:
                return DataTypes.DATE();
            case MYSQL_TIME:
                return isExplicitPrecision(precision, RAW_TIME_LENGTH)
                        ? DataTypes.TIME(precision - RAW_TIME_LENGTH - 1)
                        : DataTypes.TIME(0);
            case MYSQL_DATETIME:
            case MYSQL_TIMESTAMP:
                boolean explicitPrecision = isExplicitPrecision(precision, RAW_TIMESTAMP_LENGTH);
                if (explicitPrecision) {
                    int p = precision - RAW_TIMESTAMP_LENGTH - 1;
                    if (p <= 6 && p >= 0) {
                        return DataTypes.TIMESTAMP(p);
                    }
                    return p > 6 ? DataTypes.TIMESTAMP(6) : DataTypes.TIMESTAMP(0);
                }
                return DataTypes.TIMESTAMP(0);

            case MYSQL_YEAR:
            case MYSQL_GEOMETRY:
            case MYSQL_UNKNOWN:
            default:
                final String jdbcColumnName = metadata.getColumnName(colIndex);
                throw new UnsupportedOperationException(
                        String.format(
                                "Doesn't support MySQL type '%s' on column '%s' in MySQL version %s, driver version %s yet.",
                                mysqlType, jdbcColumnName, databaseVersion, driverVersion));
        }
    }

    private boolean isExplicitPrecision(int precision, int defaultPrecision) {

        return precision > defaultPrecision && precision - defaultPrecision - 1 <= 9;
    }

    private void checkMaxPrecision(ObjectPath tablePath, String columnName, int precision) {

        if (precision >= DecimalType.MAX_PRECISION) {
            throw new CatalogException(
                    String.format(
                            "Precision %s of table %s column name %s in type %s exceeds "
                                    + "DecimalType.MAX_PRECISION %s.",
                            precision,
                            tablePath.getFullName(),
                            columnName,
                            MYSQL_DECIMAL_UNSIGNED,
                            DecimalType.MAX_PRECISION));
        }
    }
}
