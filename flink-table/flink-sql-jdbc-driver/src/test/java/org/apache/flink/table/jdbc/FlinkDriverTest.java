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

import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Properties;

import static org.apache.flink.table.jdbc.DriverInfo.DRIVER_NAME;
import static org.apache.flink.table.jdbc.DriverInfo.DRIVER_VERSION_MAJOR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FlinkDriver}. */
public class FlinkDriverTest {
    @Test
    void testDriverInfo() {
        assertThat(DRIVER_VERSION_MAJOR).isEqualTo(2);
        assertThat(DRIVER_NAME).isEqualTo("Flink JDBC Driver");
    }

    @Test
    void testDriverUri() throws Exception {
        String uri =
                "jdbc:flink://localhost:8888/catalog_name/database_name?sessionId=123&key1=val1&key2=val2";
        assertThat(DriverUri.acceptsURL(uri)).isTrue();

        Properties properties = new Properties();
        properties.put("key3", "val3");
        properties.put("key4", "val4");
        DriverUri driverUri = DriverUri.create(uri, properties);
        assertThat(driverUri.getCatalog().get()).isEqualTo("catalog_name");
        assertThat(driverUri.getDatabase().get()).isEqualTo("database_name");
        assertThat(driverUri.getAddress().getHostName()).isEqualTo("localhost");
        assertThat(driverUri.getAddress().getPort()).isEqualTo(8888);
        assertThat(driverUri.getProperties().size()).isEqualTo(5);

        String uriWithoutDBUri =
                "jdbc:flink://localhost:8888/catalog_name?sessionId=123&key1=val1&key2=val2";
        DriverUri driverWithoutDBUri = DriverUri.create(uriWithoutDBUri, properties);
        assertThat(driverWithoutDBUri.getCatalog().get()).isEqualTo("catalog_name");
        assertThat(driverWithoutDBUri.getDatabase().isPresent()).isFalse();
        assertThat(driverWithoutDBUri.getAddress().getHostName()).isEqualTo("localhost");
        assertThat(driverWithoutDBUri.getAddress().getPort()).isEqualTo(8888);
        assertThat(driverWithoutDBUri.getProperties().size()).isEqualTo(5);
    }

    @Test
    void testDriverInvalidUri() throws Exception {
        // Invalid prefix
        for (String invalidPrefixUri :
                Arrays.asList(
                        "flink://localhost:8888/catalog_name/database_name?sessionId=123&key1=val1&key2=val2",
                        "jdbc::flink://localhost:8888/catalog_name/database_name?sessionId=123&key1=val1&key2=val2",
                        "jdbc::flink//localhost:8888/catalog_name/database_name?sessionId=123&key1=val1&key2=val2")) {
            assertThat(DriverUri.acceptsURL(invalidPrefixUri)).isFalse();
            assertThatThrownBy(() -> DriverUri.create(invalidPrefixUri, new Properties()))
                    .as(
                            String.format(
                                    "Flink JDBC URL[%s] must start with [jdbc:flink:]",
                                    invalidPrefixUri))
                    .isExactlyInstanceOf(SQLException.class);
        }

        // Without host or port
        String noPortUri = "jdbc:flink://localhost/catalog";
        assertThatThrownBy(() -> DriverUri.create(noPortUri, new Properties()))
                .as(String.format("No port specified in uri: %s", noPortUri))
                .isExactlyInstanceOf(SQLException.class);

        Properties properties = new Properties();
        properties.setProperty("key3", "val33");
        for (String dupPropUri :
                Arrays.asList(
                        "jdbc:flink://localhost:8088/catalog?key1=val1&key2=val2&key3=val3",
                        "jdbc:flink://localhost:8088/catalog?key1=val1&key2=val2&key1=val3")) {
            assertThatThrownBy(() -> DriverUri.create(dupPropUri, properties))
                    .as("Connection property 'key3' is both in the URL and an argument")
                    .isExactlyInstanceOf(SQLException.class);
        }
    }
}
