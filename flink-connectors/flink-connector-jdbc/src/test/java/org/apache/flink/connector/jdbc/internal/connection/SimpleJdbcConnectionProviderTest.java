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
import org.apache.flink.connector.jdbc.fakedb.FakeDBUtils;
import org.apache.flink.connector.jdbc.fakedb.driver.FakeConnection;
import org.apache.flink.connector.jdbc.fakedb.driver.FakeConnection1;
import org.apache.flink.connector.jdbc.fakedb.driver.FakeConnection2;
import org.apache.flink.connector.jdbc.fakedb.driver.FakeConnection3;

import org.junit.Test;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Test for {@link SimpleJdbcConnectionProvider}. */
public class SimpleJdbcConnectionProviderTest {

    private static JdbcConnectionProvider newFakeConnectionProviderWithDriverName(
            String driverName) {
        return newProvider(FakeDBUtils.TEST_DB_URL, driverName);
    }

    private static JdbcConnectionProvider newProvider(String url, String driverName) {
        JdbcConnectionOptions options =
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(url)
                        .withDriverName(driverName)
                        .build();
        return new SimpleJdbcConnectionProvider(options);
    }

    private static JdbcConnectionProvider newFakeConnectionProvider() {
        return newFakeConnectionProviderWithDriverName(FakeDBUtils.DRIVER1_CLASS_NAME);
    }

    @Test
    public void testEstablishConnection() throws Exception {
        JdbcConnectionProvider provider = newFakeConnectionProvider();
        assertNull(provider.getConnection());
        assertFalse(provider.isConnectionValid());

        Connection connection = provider.getOrEstablishConnection();
        assertNotNull(connection);
        assertFalse(connection.isClosed());
        assertTrue(provider.isConnectionValid());
        assertThat(connection, instanceOf(FakeConnection.class));

        assertNotNull(provider.getConnection());
        assertSame(connection, provider.getConnection());
        assertSame(connection, provider.getOrEstablishConnection());
    }

    @Test
    public void testEstablishConnectionWithoutDriverName() throws Exception {
        JdbcConnectionProvider provider = newProvider(FakeDBUtils.TEST_DB_URL, null);
        assertNull(provider.getConnection());
        assertFalse(provider.isConnectionValid());

        Connection connection = provider.getOrEstablishConnection();
        assertNotNull(connection);
        assertFalse(connection.isClosed());
        assertTrue(provider.isConnectionValid());
        assertThat(connection, instanceOf(FakeConnection.class));
        assertThat(connection, not(instanceOf(FakeConnection3.class)));

        assertNotNull(provider.getConnection());
        assertSame(connection, provider.getConnection());
        assertSame(connection, provider.getOrEstablishConnection());
    }

    @Test
    public void testEstablishDriverConnection() throws Exception {
        JdbcConnectionProvider provider1 =
                newFakeConnectionProviderWithDriverName(FakeDBUtils.DRIVER1_CLASS_NAME);
        Connection connection1 = provider1.getOrEstablishConnection();
        assertThat(connection1, instanceOf(FakeConnection1.class));

        JdbcConnectionProvider provider2 =
                newFakeConnectionProviderWithDriverName(FakeDBUtils.DRIVER2_CLASS_NAME);
        Connection connection2 = provider2.getOrEstablishConnection();
        assertThat(connection2, instanceOf(FakeConnection2.class));
    }

    @Test
    public void testEstablishUnregisteredDriverConnection() throws Exception {
        String unregisteredDriverName = FakeDBUtils.DRIVER3_CLASS_NAME;
        Set<String> registeredDriverNames =
                Collections.list(DriverManager.getDrivers()).stream()
                        .map(Driver::getClass)
                        .map(Class::getName)
                        .collect(Collectors.toSet());
        assertThat(registeredDriverNames, not(hasItem(unregisteredDriverName)));

        JdbcConnectionProvider provider =
                newFakeConnectionProviderWithDriverName(unregisteredDriverName);
        Connection connection = provider.getOrEstablishConnection();
        assertThat(connection, instanceOf(FakeConnection3.class));
    }

    @Test
    public void testInvalidDriverUrl() throws Exception {
        JdbcConnectionProvider provider =
                newProvider(FakeDBUtils.TEST_DB_INVALID_URL, FakeDBUtils.DRIVER1_CLASS_NAME);
        try {
            provider.getOrEstablishConnection();
            fail("expect exception");
        } catch (SQLException ex) {
            assertThat(
                    ex.getMessage(),
                    containsString(
                            "No suitable driver found for " + FakeDBUtils.TEST_DB_INVALID_URL));
        }
    }

    @Test
    public void testCloseNullConnection() throws Exception {
        JdbcConnectionProvider provider = newFakeConnectionProvider();
        provider.closeConnection();
        assertNull(provider.getConnection());
        assertFalse(provider.isConnectionValid());
    }

    @Test
    public void testCloseConnection() throws Exception {
        JdbcConnectionProvider provider = newFakeConnectionProvider();

        Connection connection1 = provider.getOrEstablishConnection();
        provider.closeConnection();
        assertNull(provider.getConnection());
        assertFalse(provider.isConnectionValid());
        assertTrue(connection1.isClosed());

        Connection connection2 = provider.getOrEstablishConnection();
        assertNotSame(connection1, connection2);
        assertFalse(connection2.isClosed());

        connection2.close();
        assertNotNull(provider.getConnection());
        assertFalse(provider.isConnectionValid());
    }

    @Test
    public void testReestablishCachedConnection() throws Exception {
        JdbcConnectionProvider provider = newFakeConnectionProvider();

        Connection connection1 = provider.reestablishConnection();
        assertNotNull(connection1);
        assertFalse(connection1.isClosed());
        assertSame(connection1, provider.getConnection());
        assertSame(connection1, provider.getOrEstablishConnection());

        Connection connection2 = provider.reestablishConnection();
        assertNotNull(connection2);
        assertFalse(connection2.isClosed());
        assertSame(connection2, provider.getConnection());
        assertSame(connection2, provider.getOrEstablishConnection());

        assertTrue(connection1.isClosed());
        assertNotSame(connection1, connection2);
    }
}
