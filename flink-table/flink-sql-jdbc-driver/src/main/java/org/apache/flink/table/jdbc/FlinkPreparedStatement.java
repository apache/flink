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

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** PreparedStatement for flink jdbc driver. Notice that the statement is not thread safe. */
public class FlinkPreparedStatement extends FlinkStatement implements PreparedStatement {
    private final String sql;
    private final HashMap<Integer, String> parameters = new HashMap<>();

    public FlinkPreparedStatement(FlinkConnection connection, String sql) {
        super(connection);
        this.sql = sql;
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        String realSql = updateSql(sql, parameters);
        return super.executeQuery(realSql);
    }

    @Override
    public int executeUpdate() throws SQLException {
        return super.executeUpdate(updateSql(sql, parameters));
    }

    @Override
    public boolean execute() throws SQLException {
        return super.execute(updateSql(sql, parameters));
    }

    @Override
    public void clearParameters() throws SQLException {
        parameters.clear();
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        setNull(parameterIndex, sqlType);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        parameters.put(parameterIndex, typedNull(sqlType));
    }

    private static String typedNull(int sqlType) throws SQLException {
        switch (sqlType) {
            case Types.BOOLEAN:
            case Types.BIT:
                return typedNull("BOOLEAN");
            case Types.TINYINT:
                return typedNull("TINYINT");
            case Types.SMALLINT:
                return typedNull("SMALLINT");
            case Types.INTEGER:
                return typedNull("INTEGER");
            case Types.BIGINT:
                return typedNull("BIGINT");
            case Types.FLOAT:
            case Types.REAL:
                return typedNull("FLOAT");
            case Types.DOUBLE:
                return typedNull("DOUBLE");
            case Types.DECIMAL:
            case Types.NUMERIC:
                return typedNull("DECIMAL");
            case Types.CHAR:
            case Types.NCHAR:
                return typedNull("CHAR");
            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.CLOB:
            case Types.NCLOB:
                return typedNull("VARCHAR");
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.BLOB:
                return typedNull("BYTES");
            case Types.DATE:
                return typedNull("DATE");
            case Types.TIME:
                return typedNull("TIME");
            case Types.TIMESTAMP:
                return typedNull("TIMESTAMP");
            case Types.NULL:
                return "NULL";
        }
        throw new SQLException("Unsupported target SQL type: " + sqlType);
    }

    private static String typedNull(String type) {
        return String.format("CAST(NULL AS %s)", type);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        parameters.put(parameterIndex, "" + x);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        parameters.put(parameterIndex, "" + x);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        parameters.put(parameterIndex, "" + x);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        parameters.put(parameterIndex, "" + x);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        parameters.put(parameterIndex, "" + x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        parameters.put(parameterIndex, "" + x);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        parameters.put(parameterIndex, "" + x);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        x = x.replace("'", "\\'");
        parameters.put(parameterIndex, "'" + x + "'");
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        parameters.put(parameterIndex, "'" + x.toString() + "'");
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        parameters.put(parameterIndex, "'" + x.toString() + "'");
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        parameters.put(parameterIndex, "'" + x.toString() + "'");
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        parameters.put(parameterIndex, "X'" + bytesToHex(x) + "'");
    }

    private static String bytesToHex(byte[] bytes) {
        char[] hexArray = "0123456789ABCDEF".toCharArray();
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        parameters.put(parameterIndex, x.toString());
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        if (x == null) {
            this.setNull(parameterIndex, Types.NULL);
        } else if (x instanceof String) {
            this.setString(parameterIndex, (String) x);
        } else if (x instanceof Short) {
            this.setShort(parameterIndex, (Short) x);
        } else if (x instanceof Integer) {
            this.setInt(parameterIndex, (Integer) x);
        } else if (x instanceof Long) {
            this.setLong(parameterIndex, (Long) x);
        } else if (x instanceof Float) {
            this.setFloat(parameterIndex, (Float) x);
        } else if (x instanceof Double) {
            this.setDouble(parameterIndex, (Double) x);
        } else if (x instanceof Boolean) {
            this.setBoolean(parameterIndex, (Boolean) x);
        } else if (x instanceof Byte) {
            this.setByte(parameterIndex, (Byte) x);
        } else if (x instanceof byte[]) {
            this.setBytes(parameterIndex, (byte[]) x);
        } else if (x instanceof Character) {
            this.setString(parameterIndex, x.toString());
        }  else if (x instanceof Date) {
            this.setDate(parameterIndex, (Date) x);
        }  else if (x instanceof Time) {
            this.setTime(parameterIndex, (Time) x);
        }  else if (x instanceof Timestamp) {
            this.setTimestamp(parameterIndex, (Timestamp) x);
        } else if (x instanceof BigDecimal) {
            this.setBigDecimal(parameterIndex, (BigDecimal) x);
        } else {
            throw new SQLException(
                    String.format(
                            "Cannot infer the SQL type for objects with class %s!",
                            x.getClass().getName()));
        }
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkPreparedStatement#setAsciiStream(int parameterIndex, InputStream x, int length) is not supported");
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length)
            throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkPreparedStatement#setUnicodeStream(int parameterIndex, InputStream x, int length) is not supported");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkPreparedStatement#setBinaryStream(int parameterIndex, InputStream x, int length) is not supported");
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkPreparedStatement#setObject(int parameterIndex, Object x, int targetSqlType) is not supported");
    }

    @Override
    public void addBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkPreparedStatement#addBatch() is not supported");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length)
            throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkPreparedStatement#setCharacterStream(int parameterIndex, Reader reader, int length) is not supported");
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkPreparedStatement#setRef(int parameterIndex, Ref x) is not supported");
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkPreparedStatement#setBlob(int parameterIndex, Blob x) is not supported");
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkPreparedStatement#setClob(int parameterIndex, Clob x) is not supported");
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkPreparedStatement#setArray(int parameterIndex, Array x) is not supported");
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkPreparedStatement#getMetaData() is not supported");
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkPreparedStatement#setDate(int parameterIndex, Date x, Calendar cal) is not supported");
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkPreparedStatement#setTime(int parameterIndex, Time x, Calendar cal) is not supported");
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkPreparedStatement#setTimestamp(int parameterIndex, Timestamp x, Calendar cal) is not supported");
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkPreparedStatement#setURL(int parameterIndex, URL x) is not supported");
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkPreparedStatement#getParameterMetaData() is not supported");
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkPreparedStatement#setRowId(int parameterIndex, RowId x) is not supported");
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkPreparedStatement#setNString(int parameterIndex, String value) is not supported");
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length)
            throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkPreparedStatement#setNCharacterStream(int parameterIndex, Reader value, long length) is not supported");
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {

        throw new SQLFeatureNotSupportedException(
                "FlinkPreparedStatement#setNClob(int parameterIndex, NClob value) is not supported");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkPreparedStatement#setClob(int parameterIndex, Reader reader, long length) is not supported");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length)
            throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkPreparedStatement#setBlob(int parameterIndex, InputStream inputStream, long length) is not supported");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkPreparedStatement#setNClob(int parameterIndex, Reader reader, long length) is not supported");
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkPreparedStatement#setSQLXML(int parameterIndex, SQLXML xmlObject) is not supported");
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength)
            throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkPreparedStatement#setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) is not supported");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkPreparedStatement#setAsciiStream(int parameterIndex, InputStream x, long length) is not supported");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length)
            throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkPreparedStatement#setBinaryStream(int parameterIndex, InputStream x, long length) is not supported");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length)
            throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkPreparedStatement#setCharacterStream(int parameterIndex, Reader reader, long length) is not supported");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkPreparedStatement#setAsciiStream(int parameterIndex, InputStream x) is not supported");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkPreparedStatement#setBinaryStream(int parameterIndex, InputStream x) is not supported");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkPreparedStatement#setCharacterStream(int parameterIndex, Reader reader) is not supported");
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkPreparedStatement#setNCharacterStream(int parameterIndex, Reader value) is not supported");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkPreparedStatement#setClob(int parameterIndex, Reader reader) is not supported");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkPreparedStatement#setBlob(int parameterIndex, InputStream inputStream) is not supported");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkPreparedStatement#setNClob(int parameterIndex, Reader reader) is not supported");
    }

    private static String updateSql(
            final String sql,
            Map<Integer, String> parameters) throws SQLException {
        List<String> parts = splitSqlStatement(sql);
        System.out.println(parts);

        StringBuilder newSql = new StringBuilder(parts.get(0));
        for (int i = 1; i < parts.size(); i++) {
            if (!parameters.containsKey(i)) {
                throw new SQLException("Parameter #" + i + " is unset");
            }
            newSql.append(parameters.get(i));
            newSql.append(parts.get(i));
        }
        return newSql.toString();
    }

    private static List<String> splitSqlStatement(String sql) {
        List<String> parts = new ArrayList<>();
        int singleQuotationMarkCount = 0;
        int offset = 0;
        boolean skip = false;

        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (skip) {
                skip = false;
                continue;
            }
            switch (c) {
                case '\'':
                    singleQuotationMarkCount++;
                    break;
                case '\\':
                    skip = true;
                    break;
                case '?':
                    if ((singleQuotationMarkCount & 1) == 0) {
                        parts.add(sql.substring(offset, i));
                        offset = i + 1;
                    }
                    break;
                default:
                    break;
            }
        }
        parts.add(sql.substring(offset));
        return parts;
    }
}
