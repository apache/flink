/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.internal.connection;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.Properties;

/** Simple JDBC connection provider. */
@NotThreadSafe
public class SimpleJdbcConnectionProvider implements JdbcConnectionProvider, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleJdbcConnectionProvider.class);

    private static final long serialVersionUID = 1L;

    private final JdbcConnectionOptions jdbcOptions;

    private transient Driver cachedDriver;
    private transient Connection connection;

    static {
        // Load DriverManager first to avoid deadlock between DriverManager's
        // static initialization block and specific driver class's static
        // initialization block when two different driver classes are loading
        // concurrently using Class.forName while DriverManager is uninitialized
        // before.
        //
        // This could happen in JDK 8 but not above as driver loading has been
        // moved out of DriverManager's static initialization block since JDK 9.
        DriverManager.getDrivers();
    }

    public SimpleJdbcConnectionProvider(JdbcConnectionOptions jdbcOptions) {
        this.jdbcOptions = jdbcOptions;
    }

    @Override
    public Connection getConnection() {
        return connection;
    }

    @Override
    public boolean isConnectionValid() throws SQLException {
        return connection != null
                && connection.isValid(jdbcOptions.getConnectionCheckTimeoutSeconds());
    }

    private Driver getDriver() throws SQLException, ClassNotFoundException {
        if (cachedDriver != null) {
            return cachedDriver;
        }
        String driverName = jdbcOptions.getDriverName();
        Enumeration<Driver> drivers = DriverManager.getDrivers();
        while (drivers.hasMoreElements()) {
            Driver driver = drivers.nextElement();
            if (driver.getClass().getName().equals(driverName)) {
                cachedDriver = driver;
                return driver;
            }
        }
        // We could reach here for reasons:
        // * Class loader hell of DriverManager(see JDK-8146872).
        // * driver is not installed as a service provider.
        Class<?> clazz =
                Class.forName(driverName, true, Thread.currentThread().getContextClassLoader());
        try {
            cachedDriver = (Driver) clazz.newInstance();
            return cachedDriver;
        } catch (Exception ex) {
            throw new SQLException("Fail to create driver of class " + driverName, ex);
        }
    }

    @Override
    public Connection getOrEstablishConnection() throws SQLException, ClassNotFoundException {
        if (connection == null) {
            Driver driver = getDriver();
            Properties info = new Properties();
            jdbcOptions.getUsername().ifPresent(user -> info.setProperty("user", user));
            jdbcOptions.getPassword().ifPresent(password -> info.setProperty("password", password));
            connection = driver.connect(jdbcOptions.getDbURL(), info);
            if (connection == null) {
                // Throw same exception as DriverManager.getConnection when no driver found to match
                // caller expectation.
                throw new SQLException(
                        "No suitable driver found for " + jdbcOptions.getDbURL(), "08001");
            }
        }
        return connection;
    }

    @Override
    public void closeConnection() {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                LOG.warn("JDBC connection close failed.", e);
            } finally {
                connection = null;
            }
        }
    }

    @Override
    public Connection reestablishConnection() throws SQLException, ClassNotFoundException {
        closeConnection();
        return getOrEstablishConnection();
    }
}
