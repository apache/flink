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

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Date;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Implementation of {@link ResultSetMetaData} for flink jdbc driver. */
public class FlinkResultSetMetaData implements ResultSetMetaData {
    private final List<ColumnInfo> columnList;

    public FlinkResultSetMetaData(List<String> columnNames, List<DataType> columnTypes) {
        this.columnList = new ArrayList<>(columnNames.size());
        for (int i = 0; i < columnNames.size(); i++) {
            columnList.add(
                    ColumnInfo.fromLogicalType(
                            columnNames.get(i), columnTypes.get(i).getLogicalType()));
        }
    }

    @Override
    public int getColumnCount() throws SQLException {
        return columnList.size();
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        checkIndexBound(column);
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        checkIndexBound(column);
        return false;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        checkIndexBound(column);
        return true;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        checkIndexBound(column);
        return false;
    }

    @Override
    public int isNullable(int column) throws SQLException {
        checkIndexBound(column);
        boolean nullable = column(column).isNullable();
        return nullable ? columnNullable : columnNoNulls;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        checkIndexBound(column);
        return column(column).isSigned();
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        checkIndexBound(column);
        return column(column).getColumnDisplaySize();
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return getColumnName(column);
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        checkIndexBound(column);
        return column(column).getColumnName();
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkResultSetMetaData#getSchemaName is not supported");
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        checkIndexBound(column);
        return column(column).getPrecision();
    }

    @Override
    public int getScale(int column) throws SQLException {
        checkIndexBound(column);
        return column(column).getScale();
    }

    @Override
    public String getTableName(int column) throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkResultSetMetaData#getTableName is not supported");
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkResultSetMetaData#getCatalogName is not supported");
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        checkIndexBound(column);
        return column(column).getColumnType();
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        checkIndexBound(column);
        return column(column).columnTypeName();
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        checkIndexBound(column);
        return true;
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        checkIndexBound(column);
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        checkIndexBound(column);
        return false;
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        return getType(getColumnType(column));
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException("FlinkResultSetMetaData#unwrap is not supported");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkResultSetMetaData#isWrapperFor is not supported");
    }

    private ColumnInfo column(int columnIndex) {
        return columnList.get(columnIndex - 1);
    }

    private void checkIndexBound(int column) throws SQLException {
        int columnNum = getColumnCount();
        if (column <= 0) {
            throw new SQLException(String.format("Column index[%s] must be positive.", column));
        }
        if (column > columnNum) {
            throw new SQLException(
                    String.format(
                            "Column index %s out of bound. There are only %s columns.",
                            column, columnNum));
        }
    }

    /**
     * Get column type name according type in {@link Types}.
     *
     * @param type the type in {@link Types}
     * @return type class name
     */
    static String getType(int type) throws SQLException {
        // see javax.sql.rowset.RowSetMetaDataImpl
        switch (type) {
            case Types.NUMERIC:
            case Types.DECIMAL:
                return BigDecimal.class.getName();
            case Types.BOOLEAN:
            case Types.BIT:
                return Boolean.class.getName();
            case Types.TINYINT:
                return Byte.class.getName();
            case Types.SMALLINT:
                return Short.class.getName();
            case Types.INTEGER:
                return Integer.class.getName();
            case Types.BIGINT:
                return Long.class.getName();
            case Types.REAL:
            case Types.FLOAT:
                return Float.class.getName();
            case Types.DOUBLE:
                return Double.class.getName();
            case Types.VARCHAR:
            case Types.CHAR:
                return String.class.getName();
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return "byte[]";
            case Types.DATE:
                return Date.class.getName();
            case Types.TIME:
                return Time.class.getName();
            case Types.TIMESTAMP:
                return Timestamp.class.getName();
            case Types.TIMESTAMP_WITH_TIMEZONE:
                return OffsetDateTime.class.getName();
            case Types.JAVA_OBJECT:
                return Map.class.getName();
            case Types.ARRAY:
                return Array.class.getName();
            case Types.STRUCT:
                return RowData.class.getName();
        }
        throw new SQLFeatureNotSupportedException(
                String.format("Not support data type [%s]", type));
    }
}
