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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;

import static org.apache.flink.table.jdbc.DriverInfo.DRIVER_VERSION_MAJOR;
import static org.apache.flink.table.jdbc.DriverInfo.DRIVER_VERSION_MINOR;

/**
 * Jdbc Driver for flink sql gateway. Only Batch Mode queries are supported. If you force to submit
 * streaming queries, you may get unrecognized updates, deletions and other results in result set.
 */
public class FlinkDriver implements Driver {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkDriver.class);

    static {
        try {
            DriverManager.registerDriver(new FlinkDriver());
        } catch (SQLException e) {
            // log message since DriverManager hides initialization exceptions
            LOG.error("Failed to register flink driver", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public Connection connect(String url, Properties driverProperties) throws SQLException {
        return new FlinkConnection(DriverUri.create(url, driverProperties));
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return DriverUri.acceptsURL(url);
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
        return DRIVER_VERSION_MAJOR;
    }

    /**
     * Minor version of flink.
     *
     * @return the minor version
     */
    @Override
    public int getMinorVersion() {
        return DRIVER_VERSION_MINOR;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException(
                "FlinkDriver#getParentLogger is not supported yet.");
    }
}
