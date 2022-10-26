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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
        assertThat(provider.getConnection()).isNull();
        assertThat(provider.isConnectionValid()).isFalse();

        Connection connection = provider.getOrEstablishConnection();
        assertThat(connection).isNotNull();
        assertThat(connection.isClosed()).isFalse();
        assertThat(provider.isConnectionValid()).isTrue();
        assertThat(connection).isInstanceOf(FakeConnection.class);

        assertThat(provider.getConnection()).isNotNull().isSameAs(connection);
        assertThat(provider.getOrEstablishConnection()).isSameAs(connection);
    }

    @Test
    public void testEstablishConnectionWithoutDriverName() throws Exception {
        JdbcConnectionProvider provider = newProvider(FakeDBUtils.TEST_DB_URL, null);
        assertThat(provider.getConnection()).isNull();
        assertThat(provider.isConnectionValid()).isFalse();

        Connection connection = provider.getOrEstablishConnection();
        assertThat(connection).isNotNull();
        assertThat(connection.isClosed()).isFalse();
        assertThat(provider.isConnectionValid()).isTrue();
        assertThat(connection)
                .isInstanceOf(FakeConnection.class)
                .isNotInstanceOf(FakeConnection3.class);

        assertThat(provider.getConnection()).isNotNull().isSameAs(connection);
        assertThat(provider.getOrEstablishConnection()).isSameAs(connection);
    }

    @Test
    public void testEstablishDriverConnection() throws Exception {
        JdbcConnectionProvider provider1 =
                newFakeConnectionProviderWithDriverName(FakeDBUtils.DRIVER1_CLASS_NAME);
        Connection connection1 = provider1.getOrEstablishConnection();
        assertThat(connection1).isInstanceOf(FakeConnection1.class);

        JdbcConnectionProvider provider2 =
                newFakeConnectionProviderWithDriverName(FakeDBUtils.DRIVER2_CLASS_NAME);
        Connection connection2 = provider2.getOrEstablishConnection();
        assertThat(connection2).isInstanceOf(FakeConnection2.class);
    }

    @Test
    public void testEstablishUnregisteredDriverConnection() throws Exception {
        String unregisteredDriverName = FakeDBUtils.DRIVER3_CLASS_NAME;
        Set<String> registeredDriverNames =
                Collections.list(DriverManager.getDrivers()).stream()
                        .map(Driver::getClass)
                        .map(Class::getName)
                        .collect(Collectors.toSet());
        assertThat(registeredDriverNames).doesNotContain(unregisteredDriverName);

        JdbcConnectionProvider provider =
                newFakeConnectionProviderWithDriverName(unregisteredDriverName);
        Connection connection = provider.getOrEstablishConnection();
        assertThat(connection).isInstanceOf(FakeConnection3.class);
    }

    @Test
    public void testInvalidDriverUrl() {
        JdbcConnectionProvider provider =
                newProvider(FakeDBUtils.TEST_DB_INVALID_URL, FakeDBUtils.DRIVER1_CLASS_NAME);

        assertThatThrownBy(provider::getOrEstablishConnection)
                .isInstanceOf(SQLException.class)
                .hasMessageContaining(
                        "No suitable driver found for " + FakeDBUtils.TEST_DB_INVALID_URL);
    }

    @Test
    public void testCloseNullConnection() throws Exception {
        JdbcConnectionProvider provider = newFakeConnectionProvider();
        provider.closeConnection();
        assertThat(provider.getConnection()).isNull();
        assertThat(provider.isConnectionValid()).isFalse();
    }

    @Test
    public void testCloseConnection() throws Exception {
        JdbcConnectionProvider provider = newFakeConnectionProvider();

        Connection connection1 = provider.getOrEstablishConnection();
        provider.closeConnection();
        assertThat(provider.getConnection()).isNull();
        assertThat(provider.isConnectionValid()).isFalse();
        assertThat(connection1.isClosed()).isTrue();

        Connection connection2 = provider.getOrEstablishConnection();
        assertThat(connection2).isNotSameAs(connection1);
        assertThat(connection2.isClosed()).isFalse();

        connection2.close();
        assertThat(provider.getConnection()).isNotNull();
        assertThat(provider.isConnectionValid()).isFalse();
    }

    @Test
    public void testReestablishCachedConnection() throws Exception {
        JdbcConnectionProvider provider = newFakeConnectionProvider();

        Connection connection1 = provider.reestablishConnection();
        assertThat(connection1).isNotNull();
        assertThat(connection1.isClosed()).isFalse();
        assertThat(provider.getConnection()).isSameAs(connection1);
        assertThat(provider.getOrEstablishConnection()).isSameAs(connection1);

        Connection connection2 = provider.reestablishConnection();
        assertThat(connection2).isNotNull();
        assertThat(connection2.isClosed()).isFalse();
        assertThat(provider.getConnection()).isSameAs(connection2);
        assertThat(provider.getOrEstablishConnection()).isSameAs(connection2);

        assertThat(connection1.isClosed()).isTrue();
        assertThat(connection2).isNotSameAs(connection1);
    }
}
