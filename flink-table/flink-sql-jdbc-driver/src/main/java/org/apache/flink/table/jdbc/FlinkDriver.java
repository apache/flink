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

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Jdbc Driver for flink sql gateway. Only Batch Mode queries are supported. If you force to submit
 * streaming queries, you may get unrecognized updates, deletions and other results in result set.
 */
public class FlinkDriver implements Driver {
    public static final String URL_PREFIX = "jdbc:flink://";

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented yet.");
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url.startsWith(URL_PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    /**
     * Major version of flink.
     *
     * @return the major version
     */
    @Override
    public int getMajorVersion() {
        throw new RuntimeException("Not implemented yet, will use major version of flink.");
    }

    /**
     * Minor version of flink.
     *
     * @return the minor version
     */
    @Override
    public int getMinorVersion() {
        throw new RuntimeException("Not implemented yet, will use minor version of flink.");
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDriver#getParentLogger is not supported yet.");
    }
}
