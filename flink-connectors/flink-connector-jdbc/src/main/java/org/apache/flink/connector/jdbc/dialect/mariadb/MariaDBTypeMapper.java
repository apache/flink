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

package org.apache.flink.connector.jdbc.dialect.mariadb;

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

/** MariaDBTypeMapper util class. */
@Internal
public class MariaDBTypeMapper implements JdbcDialectTypeMapper {

    private static final Logger LOG = LoggerFactory.getLogger(MariaDBTypeMapper.class);

    // ============================data types=====================

    private static final String MARIADB_UNKNOWN = "UNKNOWN";
    private static final String MARIADB_BIT = "BIT";

    // -------------------------number----------------------------
    private static final String MARIADB_TINYINT = "TINYINT";
    private static final String MARIADB_TINYINT_UNSIGNED = "TINYINT UNSIGNED";
    private static final String MARIADB_SMALLINT = "SMALLINT";
    private static final String MARIADB_SMALLINT_UNSIGNED = "SMALLINT UNSIGNED";
    private static final String MARIADB_MEDIUMINT = "MEDIUMINT";
    private static final String MARIADB_MEDIUMINT_UNSIGNED = "MEDIUMINT UNSIGNED";
    private static final String MARIADB_INT = "INT";
    private static final String MARIADB_INT_UNSIGNED = "INT UNSIGNED";
    private static final String MARIADB_INTEGER = "INTEGER";
    private static final String MARIADB_INTEGER_UNSIGNED = "INTEGER UNSIGNED";
    private static final String MARIADB_BIGINT = "BIGINT";
    private static final String MARIADB_BIGINT_UNSIGNED = "BIGINT UNSIGNED";
    private static final String MARIADB_DECIMAL = "DECIMAL";
    private static final String MARIADB_DECIMAL_UNSIGNED = "DECIMAL UNSIGNED";
    private static final String MARIADB_FLOAT = "FLOAT";
    private static final String MARIADB_FLOAT_UNSIGNED = "FLOAT UNSIGNED";
    private static final String MARIADB_DOUBLE = "DOUBLE";
    private static final String MARIADB_DOUBLE_UNSIGNED = "DOUBLE UNSIGNED";

    // -------------------------string----------------------------
    private static final String MARIADB_CHAR = "CHAR";
    private static final String MARIADB_VARCHAR = "VARCHAR";
    private static final String MARIADB_TINYTEXT = "TINYTEXT";
    private static final String MARIADB_MEDIUMTEXT = "MEDIUMTEXT";
    private static final String MARIADB_TEXT = "TEXT";
    private static final String MARIADB_LONGTEXT = "LONGTEXT";
    private static final String MARIADB_JSON = "JSON";

    // ------------------------------time-------------------------
    private static final String MARIADB_DATE = "DATE";
    private static final String MARIADB_DATETIME = "DATETIME";
    private static final String MARIADB_TIME = "TIME";
    private static final String MARIADB_TIMESTAMP = "TIMESTAMP";
    private static final String MARIADB_YEAR = "YEAR";

    // ------------------------------blob-------------------------
    private static final String MARIADB_TINYBLOB = "TINYBLOB";
    private static final String MARIADB_MEDIUMBLOB = "MEDIUMBLOB";
    private static final String MARIADB_BLOB = "BLOB";
    private static final String MARIADB_LONGBLOB = "LONGBLOB";
    private static final String MARIADB_BINARY = "BINARY";
    private static final String MARIADB_VARBINARY = "VARBINARY";
    private static final String MARIADB_GEOMETRY = "GEOMETRY";

    private static final int RAW_TIME_LENGTH = 10;
    private static final int RAW_TIMESTAMP_LENGTH = 19;

    private final String databaseVersion;
    private final String driverVersion;

    public MariaDBTypeMapper(String databaseVersion, String driverVersion) {
        this.databaseVersion = databaseVersion;
        this.driverVersion = driverVersion;
    }

    @Override
    public DataType mapping(ObjectPath tablePath, ResultSetMetaData metadata, int colIndex)
            throws SQLException {
        String mariadbType = metadata.getColumnTypeName(colIndex).toUpperCase();
        String columnName = metadata.getColumnName(colIndex);
        int precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);

        switch (mariadbType) {
            case MARIADB_BIT:
                return DataTypes.BOOLEAN();
            case MARIADB_TINYBLOB:
            case MARIADB_MEDIUMBLOB:
            case MARIADB_BLOB:
            case MARIADB_LONGBLOB:
            case MARIADB_VARBINARY:
            case MARIADB_BINARY:
                // BINARY is not supported in MariaDBDialect now.
                // VARBINARY(n) is not supported in MariaDBDialect when 'n' is not equals to
                // Integer.MAX_VALUE. Please see
                // org.apache.flink.connector.jdbc.dialect.mariadb.MariaDBDialect#supportedTypes and
                // org.apache.flink.connector.jdbc.dialect.AbstractDialect#validate for more
                // details.
                return DataTypes.BYTES();
            case MARIADB_TINYINT:
                return DataTypes.TINYINT();
            case MARIADB_TINYINT_UNSIGNED:
            case MARIADB_SMALLINT:
                return DataTypes.SMALLINT();
            case MARIADB_SMALLINT_UNSIGNED:
            case MARIADB_MEDIUMINT:
            case MARIADB_MEDIUMINT_UNSIGNED:
            case MARIADB_INT:
            case MARIADB_INTEGER:
                return DataTypes.INT();
            case MARIADB_INT_UNSIGNED:
            case MARIADB_INTEGER_UNSIGNED:
            case MARIADB_BIGINT:
                return DataTypes.BIGINT();
            case MARIADB_BIGINT_UNSIGNED:
                return DataTypes.DECIMAL(20, 0);
            case MARIADB_DECIMAL:
                return DataTypes.DECIMAL(precision, scale);
            case MARIADB_DECIMAL_UNSIGNED:
                checkMaxPrecision(tablePath, columnName, precision);
                return DataTypes.DECIMAL(precision + 1, scale);
            case MARIADB_FLOAT:
                return DataTypes.FLOAT();
            case MARIADB_FLOAT_UNSIGNED:
                LOG.warn("{} will probably cause value overflow.", MARIADB_FLOAT_UNSIGNED);
                return DataTypes.FLOAT();
            case MARIADB_DOUBLE:
                return DataTypes.DOUBLE();
            case MARIADB_DOUBLE_UNSIGNED:
                LOG.warn("{} will probably cause value overflow.", MARIADB_DOUBLE_UNSIGNED);
                return DataTypes.DOUBLE();
            case MARIADB_CHAR:
                return DataTypes.CHAR(precision);
            case MARIADB_VARCHAR:
            case MARIADB_TINYTEXT:
            case MARIADB_MEDIUMTEXT:
            case MARIADB_TEXT:
                return DataTypes.VARCHAR(precision);
            case MARIADB_JSON:
                return DataTypes.STRING();
            case MARIADB_LONGTEXT:
                LOG.warn(
                        "Type '{}' has a maximum precision of 4,294,967,295 or 4GB characters "
                                + "in MariaDB. "
                                + "Due to limitations in the Flink type system, "
                                + "the precision will be set to 2147483647.",
                        MARIADB_LONGTEXT);
                return DataTypes.STRING();
            case MARIADB_DATE:
                return DataTypes.DATE();
            case MARIADB_TIME:
                return isExplicitPrecision(precision, RAW_TIME_LENGTH)
                        ? DataTypes.TIME(precision - RAW_TIME_LENGTH - 1)
                        : DataTypes.TIME(0);
            case MARIADB_DATETIME:
            case MARIADB_TIMESTAMP:
                boolean explicitPrecision = isExplicitPrecision(precision, RAW_TIMESTAMP_LENGTH);
                if (explicitPrecision) {
                    int p = precision - RAW_TIMESTAMP_LENGTH - 1;
                    if (p <= 6 && p >= 0) {
                        return DataTypes.TIMESTAMP(p);
                    }
                    return p > 6 ? DataTypes.TIMESTAMP(6) : DataTypes.TIMESTAMP(0);
                }
                return DataTypes.TIMESTAMP(0);

            case MARIADB_YEAR:
            case MARIADB_GEOMETRY:
            case MARIADB_UNKNOWN:
            default:
                final String jdbcColumnName = metadata.getColumnName(colIndex);
                throw new UnsupportedOperationException(
                        String.format(
                                "Doesn't support MariaDB type '%s' on column '%s' in MariaDB version %s, driver version %s yet.",
                                mariadbType, jdbcColumnName, databaseVersion, driverVersion));
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
                            MARIADB_DECIMAL_UNSIGNED,
                            DecimalType.MAX_PRECISION));
        }
    }
}
