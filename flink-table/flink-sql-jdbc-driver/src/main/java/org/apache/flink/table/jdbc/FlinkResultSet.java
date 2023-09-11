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

import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.client.gateway.StatementResult;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.jdbc.utils.CloseableResultIterator;
import org.apache.flink.table.jdbc.utils.StatementResultIterator;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Array;
import java.sql.Date;
import java.sql.ResultSetMetaData;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.jdbc.utils.DriverUtils.checkNotNull;

/**
 * ResultSet for flink jdbc driver. Only Batch Mode queries are supported. If you force to submit
 * streaming queries, you may get unrecognized updates, deletions and other results.
 */
public class FlinkResultSet extends BaseResultSet {
    private final List<DataType> dataTypeList;
    private final List<String> columnNameList;
    private final List<RowData.FieldGetter> fieldGetterList;
    private final Statement statement;
    private final CloseableResultIterator<RowData> iterator;
    private final FlinkResultSetMetaData resultSetMetaData;
    private RowData currentRow;
    private boolean wasNull;

    private volatile boolean closed;

    public FlinkResultSet(Statement statement, StatementResult result) {
        this(statement, new StatementResultIterator(result), result.getResultSchema());
    }

    public FlinkResultSet(
            Statement statement, CloseableResultIterator<RowData> iterator, ResolvedSchema schema) {
        this.statement = checkNotNull(statement, "Statement cannot be null");
        this.iterator = checkNotNull(iterator, "Statement result cannot be null");
        this.currentRow = null;
        this.wasNull = false;

        this.dataTypeList = schema.getColumnDataTypes();
        this.columnNameList = schema.getColumnNames();
        this.fieldGetterList = createFieldGetterList(dataTypeList);
        this.resultSetMetaData = new FlinkResultSetMetaData(columnNameList, dataTypeList);
    }

    private List<RowData.FieldGetter> createFieldGetterList(List<DataType> dataTypeList) {
        List<RowData.FieldGetter> fieldGetterList = new ArrayList<>(dataTypeList.size());
        for (int i = 0; i < dataTypeList.size(); i++) {
            fieldGetterList.add(RowData.createFieldGetter(dataTypeList.get(i).getLogicalType(), i));
        }

        return fieldGetterList;
    }

    @Override
    public boolean next() throws SQLException {
        checkClosed();

        if (iterator.hasNext()) {
            // TODO check the kind of currentRow
            currentRow = iterator.next();
            wasNull = currentRow == null;
            return true;
        } else {
            return false;
        }
    }

    private void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("This result set is already closed");
        }
    }

    private void checkValidRow() throws SQLException {
        if (currentRow == null) {
            throw new SQLException("Not on a valid row");
        }
        if (currentRow.getArity() <= 0) {
            throw new SQLException("Empty row with no data");
        }
    }

    private void checkValidColumn(int columnIndex) throws SQLException {
        if (columnIndex <= 0) {
            throw new SQLException(
                    String.format("Column index[%s] must be positive.", columnIndex));
        }

        final int columnCount = currentRow.getArity();
        if (columnIndex > columnCount) {
            throw new SQLException(
                    String.format(
                            "Column index %s out of bound. There are only %s columns.",
                            columnIndex, columnCount));
        }
    }

    @Override
    public void close() throws SQLException {
        if (closed) {
            return;
        }
        closed = true;

        try {
            iterator.close();
        } catch (Exception e) {
            throw new SQLException("Close result iterator fail", e);
        }
    }

    @Override
    public boolean wasNull() throws SQLException {
        checkClosed();

        return wasNull;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        checkClosed();
        checkValidRow();
        checkValidColumn(columnIndex);

        StringData stringData = currentRow.getString(columnIndex - 1);
        try {
            return stringData == null ? null : stringData.toString();
        } catch (Exception e) {
            throw new SQLDataException(e);
        }
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        checkClosed();
        checkValidRow();
        checkValidColumn(columnIndex);
        try {
            return !currentRow.isNullAt(columnIndex - 1) && currentRow.getBoolean(columnIndex - 1);
        } catch (Exception e) {
            throw new SQLDataException(e);
        }
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        checkClosed();
        checkValidRow();
        checkValidColumn(columnIndex);
        try {
            return currentRow.isNullAt(columnIndex - 1) ? 0 : currentRow.getByte(columnIndex - 1);
        } catch (Exception e) {
            throw new SQLDataException(e);
        }
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        checkClosed();
        checkValidRow();
        checkValidColumn(columnIndex);
        try {
            return currentRow.isNullAt(columnIndex - 1) ? 0 : currentRow.getShort(columnIndex - 1);
        } catch (Exception e) {
            throw new SQLDataException(e);
        }
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        checkClosed();
        checkValidRow();
        checkValidColumn(columnIndex);
        try {
            return currentRow.isNullAt(columnIndex - 1) ? 0 : currentRow.getInt(columnIndex - 1);
        } catch (Exception e) {
            throw new SQLDataException(e);
        }
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        checkClosed();
        checkValidRow();
        checkValidColumn(columnIndex);

        try {
            return currentRow.isNullAt(columnIndex - 1) ? 0L : currentRow.getLong(columnIndex - 1);
        } catch (Exception e) {
            throw new SQLDataException(e);
        }
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        checkClosed();
        checkValidRow();
        checkValidColumn(columnIndex);
        try {
            return currentRow.isNullAt(columnIndex - 1) ? 0 : currentRow.getFloat(columnIndex - 1);
        } catch (Exception e) {
            throw new SQLDataException(e);
        }
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        checkClosed();
        checkValidRow();
        checkValidColumn(columnIndex);
        try {
            return currentRow.isNullAt(columnIndex - 1) ? 0 : currentRow.getDouble(columnIndex - 1);
        } catch (Exception e) {
            throw new SQLDataException(e);
        }
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        return getBigDecimal(columnIndex).setScale(scale, RoundingMode.HALF_EVEN);
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        checkClosed();
        checkValidRow();
        checkValidColumn(columnIndex);
        try {
            return currentRow.getBinary(columnIndex - 1);
        } catch (Exception e) {
            throw new SQLDataException(e);
        }
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        return (Date) getObject(columnIndex);
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        return (Time) getObject(columnIndex);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return (Timestamp) getObject(columnIndex);
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return getString(getColumnIndex(columnLabel));
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(getColumnIndex(columnLabel));
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return getByte(getColumnIndex(columnLabel));
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return getShort(getColumnIndex(columnLabel));
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getInt(getColumnIndex(columnLabel));
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getLong(getColumnIndex(columnLabel));
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(getColumnIndex(columnLabel));
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(getColumnIndex(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return getBigDecimal(getColumnIndex(columnLabel), scale);
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return getBytes(getColumnIndex(columnLabel));
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return getDate(getColumnIndex(columnLabel));
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return getTime(getColumnIndex(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(getColumnIndex(columnLabel));
    }

    private int getColumnIndex(String columnLabel) throws SQLException {
        int columnIndex = columnNameList.indexOf(columnLabel) + 1;
        if (columnIndex <= 0) {
            throw new SQLDataException(String.format("Column[%s] is not exist", columnLabel));
        }
        return columnIndex;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return resultSetMetaData;
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        checkClosed();
        checkValidRow();
        checkValidColumn(columnIndex);
        try {
            Object object = fieldGetterList.get(columnIndex - 1).getFieldOrNull(currentRow);
            DataType dataType = dataTypeList.get(columnIndex - 1);
            return convertToJavaObject(object, dataType.getLogicalType());
        } catch (Exception e) {
            throw new SQLDataException(e);
        }
    }

    private Object convertToJavaObject(Object object, LogicalType dataType) throws SQLException {
        if (object == null) {
            return null;
        }

        switch (dataType.getTypeRoot()) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case BINARY:
            case VARBINARY:
                {
                    return object;
                }
            case VARCHAR:
            case CHAR:
                {
                    return object.toString();
                }
            case DECIMAL:
                {
                    return ((DecimalData) object).toBigDecimal();
                }
            case MAP:
                {
                    LogicalType keyType = ((MapType) dataType).getKeyType();
                    LogicalType valueType = ((MapType) dataType).getValueType();
                    ArrayData.ElementGetter keyGetter = ArrayData.createElementGetter(keyType);
                    ArrayData.ElementGetter valueGetter = ArrayData.createElementGetter(valueType);
                    MapData mapData = (MapData) object;
                    int size = mapData.size();
                    ArrayData keyArrayData = mapData.keyArray();
                    ArrayData valueArrayData = mapData.valueArray();
                    Map<Object, Object> mapResult = new HashMap<>();
                    for (int i = 0; i < size; i++) {
                        mapResult.put(
                                convertToJavaObject(
                                        keyGetter.getElementOrNull(keyArrayData, i), keyType),
                                convertToJavaObject(
                                        valueGetter.getElementOrNull(valueArrayData, i),
                                        valueType));
                    }
                    return mapResult;
                }
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                {
                    return ((TimestampData) object).toTimestamp();
                }
            case TIMESTAMP_WITH_TIME_ZONE:
                {
                    // TODO should be supported after
                    // https://issues.apache.org/jira/browse/FLINK-20869
                    throw new SQLDataException(
                            "TIMESTAMP WITH TIME ZONE is not supported, use TIMESTAMP or TIMESTAMP_LTZ instead");
                }
            case TIME_WITHOUT_TIME_ZONE:
                {
                    return Time.valueOf(
                            LocalTime.ofNanoOfDay(((Number) object).intValue() * 1_000_000L));
                }
            case DATE:
                {
                    return Date.valueOf(LocalDate.ofEpochDay(((Number) object).intValue()));
                }
            default:
                {
                    throw new SQLDataException(
                            String.format("Not supported value type %s", dataType));
                }
        }
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(getColumnIndex(columnLabel));
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        return getColumnIndex(columnLabel);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        checkClosed();
        checkValidRow();
        checkValidColumn(columnIndex);

        DataType dataType = dataTypeList.get(columnIndex - 1);
        if (!(dataType.getLogicalType() instanceof DecimalType)) {
            throw new SQLException(
                    String.format(
                            "Invalid data type, expect %s but was %s",
                            DecimalType.class.getSimpleName(),
                            dataType.getLogicalType().getClass().getSimpleName()));
        }
        DecimalType decimalType = (DecimalType) dataType.getLogicalType();
        try {
            return currentRow.isNullAt(columnIndex - 1)
                    ? null
                    : currentRow
                            .getDecimal(
                                    columnIndex - 1,
                                    decimalType.getPrecision(),
                                    decimalType.getScale())
                            .toBigDecimal();
        } catch (Exception e) {
            throw new SQLDataException(e);
        }
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return getBigDecimal(getColumnIndex(columnLabel));
    }

    @Override
    public Statement getStatement() throws SQLException {
        return statement;
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        // TODO support array data
        throw new SQLFeatureNotSupportedException("FlinkResultSet#getArray is not supported");
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        return getArray(getColumnIndex(columnLabel));
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        // TODO get date with timezone
        throw new SQLFeatureNotSupportedException("FlinkResultSet#getObject is not supported");
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        // TODO get date with timezone
        throw new SQLFeatureNotSupportedException("FlinkResultSet#getObject is not supported");
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        // TODO get time with timezone
        throw new SQLFeatureNotSupportedException("FlinkResultSet#getObject is not supported");
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        // TODO get time with timezone
        throw new SQLFeatureNotSupportedException("FlinkResultSet#getObject is not supported");
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        // TODO get timestamp with timezone
        throw new SQLFeatureNotSupportedException("FlinkResultSet#getObject is not supported");
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        // TODO get timestamp with timezone
        throw new SQLFeatureNotSupportedException("FlinkResultSet#getObject is not supported");
    }

    @Override
    public boolean isClosed() throws SQLException {
        return this.closed;
    }
}
