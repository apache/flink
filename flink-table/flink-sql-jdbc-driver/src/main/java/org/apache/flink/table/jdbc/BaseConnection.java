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

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.concurrent.Executor;

/** Base connection in flink driver. */
abstract class BaseConnection implements Connection {

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkConnection#prepareStatement is not supported yet.");
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkConnection#prepareCall is not supported yet.");
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkConnection#nativeSQL is not supported yet.");
    }

    @Override
    public void commit() throws SQLException {
        throw new SQLFeatureNotSupportedException("FlinkConnection#commit is not supported yet.");
    }

    @Override
    public void rollback() throws SQLException {
        throw new SQLFeatureNotSupportedException("FlinkConnection#rollback is not supported yet.");
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkConnection#setReadOnly is not supported yet.");
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkConnection#isReadOnly is not supported yet.");
    }

    @Override
    public void clearWarnings() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkConnection#clearWarnings is not supported yet.");
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency)
            throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkConnection#createStatement is not supported yet.");
    }

    @Override
    public PreparedStatement prepareStatement(
            String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkConnection#prepareStatement is not supported yet.");
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkConnection#prepareCall is not supported yet.");
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkConnection#getTypeMap is not supported yet.");
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkConnection#setTypeMap is not supported yet.");
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkConnection#setHoldability is not supported yet.");
    }

    @Override
    public int getHoldability() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkConnection#getHoldability is not supported yet.");
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkConnection#setSavepoint is not supported yet.");
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkConnection#setSavepoint is not supported yet.");
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("FlinkConnection#rollback is not supported yet.");
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkConnection#releaseSavepoint is not supported yet.");
    }

    @Override
    public Statement createStatement(
            int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkConnection#createStatement is not supported yet.");
    }

    @Override
    public PreparedStatement prepareStatement(
            String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkConnection#prepareStatement is not supported yet.");
    }

    @Override
    public CallableStatement prepareCall(
            String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkConnection#prepareCall is not supported yet.");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
            throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkConnection#prepareStatement is not supported yet.");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkConnection#prepareStatement is not supported yet.");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames)
            throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkConnection#prepareStatement is not supported yet.");
    }

    @Override
    public Clob createClob() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkConnection#createClob is not supported yet.");
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkConnection#createBlob is not supported yet.");
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkConnection#createNClob is not supported yet.");
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkConnection#createSQLXML is not supported yet.");
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        throw new SQLFeatureNotSupportedException("FlinkConnection#isValid is not supported yet.");
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkConnection#createArrayOf is not supported yet.");
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkConnection#createStruct is not supported yet.");
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        throw new SQLFeatureNotSupportedException("FlinkConnection#abort is not supported yet.");
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkConnection#setNetworkTimeout is not supported yet.");
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkConnection#getNetworkTimeout is not supported yet.");
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException("FlinkConnection#unwrap is not supported yet.");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "FlinkConnection#isWrapperFor is not supported yet.");
    }
}
