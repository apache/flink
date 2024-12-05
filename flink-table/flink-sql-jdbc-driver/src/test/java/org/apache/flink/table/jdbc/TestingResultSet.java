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
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

public class TestingResultSet implements ResultSet {
    @Override
    public boolean next() {
        return false;
    }

    @Override
    public void close() {}

    @Override
    public boolean wasNull() {
        return false;
    }

    @Override
    public String getString(int columnIndex) {
        return "";
    }

    @Override
    public boolean getBoolean(int columnIndex) {
        return false;
    }

    @Override
    public byte getByte(int columnIndex) {
        return 0;
    }

    @Override
    public short getShort(int columnIndex) {
        return 0;
    }

    @Override
    public int getInt(int columnIndex) {
        return 0;
    }

    @Override
    public long getLong(int columnIndex) {
        return 0;
    }

    @Override
    public float getFloat(int columnIndex) {
        return 0;
    }

    @Override
    public double getDouble(int columnIndex) {
        return 0;
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) {
        return null;
    }

    @Override
    public byte[] getBytes(int columnIndex) {
        return new byte[0];
    }

    @Override
    public Date getDate(int columnIndex) {
        return null;
    }

    @Override
    public Time getTime(int columnIndex) {
        return null;
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) {
        return null;
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) {
        return null;
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) {
        return null;
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) {
        return null;
    }

    @Override
    public String getString(String columnLabel) {
        return "";
    }

    @Override
    public boolean getBoolean(String columnLabel) {
        return false;
    }

    @Override
    public byte getByte(String columnLabel) {
        return 0;
    }

    @Override
    public short getShort(String columnLabel) {
        return 0;
    }

    @Override
    public int getInt(String columnLabel) {
        return 0;
    }

    @Override
    public long getLong(String columnLabel) {
        return 0;
    }

    @Override
    public float getFloat(String columnLabel) {
        return 0;
    }

    @Override
    public double getDouble(String columnLabel) {
        return 0;
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) {
        return null;
    }

    @Override
    public byte[] getBytes(String columnLabel) {
        return new byte[0];
    }

    @Override
    public Date getDate(String columnLabel) {
        return null;
    }

    @Override
    public Time getTime(String columnLabel) {
        return null;
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) {
        return null;
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) {
        return null;
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) {
        return null;
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) {
        return null;
    }

    @Override
    public SQLWarning getWarnings() {
        return null;
    }

    @Override
    public void clearWarnings() {}

    @Override
    public String getCursorName() {
        return "";
    }

    @Override
    public ResultSetMetaData getMetaData() {
        return null;
    }

    @Override
    public Object getObject(int columnIndex) {
        return null;
    }

    @Override
    public Object getObject(String columnLabel) {
        return null;
    }

    @Override
    public int findColumn(String columnLabel) {
        return 0;
    }

    @Override
    public Reader getCharacterStream(int columnIndex) {
        return null;
    }

    @Override
    public Reader getCharacterStream(String columnLabel) {
        return null;
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) {
        return null;
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) {
        return null;
    }

    @Override
    public boolean isBeforeFirst() {
        return false;
    }

    @Override
    public boolean isAfterLast() {
        return false;
    }

    @Override
    public boolean isFirst() {
        return false;
    }

    @Override
    public boolean isLast() {
        return false;
    }

    @Override
    public void beforeFirst() {}

    @Override
    public void afterLast() {}

    @Override
    public boolean first() {
        return false;
    }

    @Override
    public boolean last() {
        return false;
    }

    @Override
    public int getRow() {
        return 0;
    }

    @Override
    public boolean absolute(int row) {
        return false;
    }

    @Override
    public boolean relative(int rows) {
        return false;
    }

    @Override
    public boolean previous() {
        return false;
    }

    @Override
    public void setFetchDirection(int direction) {}

    @Override
    public int getFetchDirection() {
        return 0;
    }

    @Override
    public void setFetchSize(int rows) {}

    @Override
    public int getFetchSize() {
        return 0;
    }

    @Override
    public int getType() {
        return 0;
    }

    @Override
    public int getConcurrency() {
        return 0;
    }

    @Override
    public boolean rowUpdated() {
        return false;
    }

    @Override
    public boolean rowInserted() {
        return false;
    }

    @Override
    public boolean rowDeleted() {
        return false;
    }

    @Override
    public void updateNull(int columnIndex) {}

    @Override
    public void updateBoolean(int columnIndex, boolean x) {}

    @Override
    public void updateByte(int columnIndex, byte x) {}

    @Override
    public void updateShort(int columnIndex, short x) {}

    @Override
    public void updateInt(int columnIndex, int x) {}

    @Override
    public void updateLong(int columnIndex, long x) {}

    @Override
    public void updateFloat(int columnIndex, float x) {}

    @Override
    public void updateDouble(int columnIndex, double x) {}

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) {}

    @Override
    public void updateString(int columnIndex, String x) {}

    @Override
    public void updateBytes(int columnIndex, byte[] x) {}

    @Override
    public void updateDate(int columnIndex, Date x) {}

    @Override
    public void updateTime(int columnIndex, Time x) {}

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) {}

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) {}

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) {}

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) {}

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) {}

    @Override
    public void updateObject(int columnIndex, Object x) {}

    @Override
    public void updateNull(String columnLabel) {}

    @Override
    public void updateBoolean(String columnLabel, boolean x) {}

    @Override
    public void updateByte(String columnLabel, byte x) {}

    @Override
    public void updateShort(String columnLabel, short x) {}

    @Override
    public void updateInt(String columnLabel, int x) {}

    @Override
    public void updateLong(String columnLabel, long x) {}

    @Override
    public void updateFloat(String columnLabel, float x) {}

    @Override
    public void updateDouble(String columnLabel, double x) {}

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) {}

    @Override
    public void updateString(String columnLabel, String x) {}

    @Override
    public void updateBytes(String columnLabel, byte[] x) {}

    @Override
    public void updateDate(String columnLabel, Date x) {}

    @Override
    public void updateTime(String columnLabel, Time x) {}

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) {}

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) {}

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) {}

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) {}

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) {}

    @Override
    public void updateObject(String columnLabel, Object x) {}

    @Override
    public void insertRow() {}

    @Override
    public void updateRow() {}

    @Override
    public void deleteRow() {}

    @Override
    public void refreshRow() {}

    @Override
    public void cancelRowUpdates() {}

    @Override
    public void moveToInsertRow() {}

    @Override
    public void moveToCurrentRow() {}

    @Override
    public Statement getStatement() {
        return null;
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) {
        return null;
    }

    @Override
    public Ref getRef(int columnIndex) {
        return null;
    }

    @Override
    public Blob getBlob(int columnIndex) {
        return null;
    }

    @Override
    public Clob getClob(int columnIndex) {
        return null;
    }

    @Override
    public Array getArray(int columnIndex) {
        return null;
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) {
        return null;
    }

    @Override
    public Ref getRef(String columnLabel) {
        return null;
    }

    @Override
    public Blob getBlob(String columnLabel) {
        return null;
    }

    @Override
    public Clob getClob(String columnLabel) {
        return null;
    }

    @Override
    public Array getArray(String columnLabel) {
        return null;
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) {
        return null;
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) {
        return null;
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) {
        return null;
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) {
        return null;
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) {
        return null;
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) {
        return null;
    }

    @Override
    public URL getURL(int columnIndex) {
        return null;
    }

    @Override
    public URL getURL(String columnLabel) {
        return null;
    }

    @Override
    public void updateRef(int columnIndex, Ref x) {}

    @Override
    public void updateRef(String columnLabel, Ref x) {}

    @Override
    public void updateBlob(int columnIndex, Blob x) {}

    @Override
    public void updateBlob(String columnLabel, Blob x) {}

    @Override
    public void updateClob(int columnIndex, Clob x) {}

    @Override
    public void updateClob(String columnLabel, Clob x) {}

    @Override
    public void updateArray(int columnIndex, Array x) {}

    @Override
    public void updateArray(String columnLabel, Array x) {}

    @Override
    public RowId getRowId(int columnIndex) {
        return null;
    }

    @Override
    public RowId getRowId(String columnLabel) {
        return null;
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) {}

    @Override
    public void updateRowId(String columnLabel, RowId x) {}

    @Override
    public int getHoldability() {
        return 0;
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public void updateNString(int columnIndex, String nString) {}

    @Override
    public void updateNString(String columnLabel, String nString) {}

    @Override
    public void updateNClob(int columnIndex, NClob nClob) {}

    @Override
    public void updateNClob(String columnLabel, NClob nClob) {}

    @Override
    public NClob getNClob(int columnIndex) {
        return null;
    }

    @Override
    public NClob getNClob(String columnLabel) {
        return null;
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) {
        return null;
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) {
        return null;
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) {}

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) {}

    @Override
    public String getNString(int columnIndex) {
        return "";
    }

    @Override
    public String getNString(String columnLabel) {
        return "";
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) {
        return null;
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) {
        return null;
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) {}

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) {}

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) {}

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) {}

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) {}

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) {}

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) {}

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) {}

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) {}

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) {}

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) {}

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) {}

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) {}

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) {}

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) {}

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) {}

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) {}

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) {}

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) {}

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) {}

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) {}

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) {}

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) {}

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) {}

    @Override
    public void updateClob(int columnIndex, Reader reader) {}

    @Override
    public void updateClob(String columnLabel, Reader reader) {}

    @Override
    public void updateNClob(int columnIndex, Reader reader) {}

    @Override
    public void updateNClob(String columnLabel, Reader reader) {}

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) {
        return null;
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) {
        return null;
    }

    @Override
    public <T> T unwrap(Class<T> iface) {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return false;
    }
}
