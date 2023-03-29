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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link FlinkDriver}. */
public class FlinkDriverTest {
    @Test
    public void testDriverInfo() {
        assertEquals(DRIVER_VERSION_MAJOR, 1);
        assertEquals(DRIVER_NAME, "Flink JDBC Driver");
    }

    @Test
    public void testDriverUri() throws Exception {
        String uri =
                "jdbc:flink://localhost:8888/catalog_name/database_name?sessionId=123&key1=val1&key2=val2";
        assertTrue(DriverUri.acceptsURL(uri));

        Properties properties = new Properties();
        properties.put("key3", "val3");
        properties.put("key4", "val4");
        DriverUri driverUri = DriverUri.create(uri, properties);
        assertEquals("catalog_name", driverUri.getCatalog().get());
        assertEquals("database_name", driverUri.getDatabase().get());
        assertEquals("localhost", driverUri.getAddress().getHostName());
        assertEquals(8888, driverUri.getAddress().getPort());
        assertEquals(5, driverUri.getProperties().size());

        String uriWithoutDBUri =
                "jdbc:flink://localhost:8888/catalog_name?sessionId=123&key1=val1&key2=val2";
        DriverUri driverWithoutDBUri = DriverUri.create(uriWithoutDBUri, properties);
        assertEquals("catalog_name", driverWithoutDBUri.getCatalog().get());
        assertFalse(driverWithoutDBUri.getDatabase().isPresent());
        assertEquals("localhost", driverWithoutDBUri.getAddress().getHostName());
        assertEquals(8888, driverWithoutDBUri.getAddress().getPort());
        assertEquals(5, driverWithoutDBUri.getProperties().size());
    }

    @Test
    public void testDriverInvalidUri() throws Exception {
        // Invalid prefix
        for (String invalidPrefixUri :
                Arrays.asList(
                        "flink://localhost:8888/catalog_name/database_name?sessionId=123&key1=val1&key2=val2",
                        "jdbc::flink://localhost:8888/catalog_name/database_name?sessionId=123&key1=val1&key2=val2",
                        "jdbc::flink//localhost:8888/catalog_name/database_name?sessionId=123&key1=val1&key2=val2")) {
            assertFalse(DriverUri.acceptsURL(invalidPrefixUri));
            assertThrowsExactly(
                    SQLException.class,
                    () -> DriverUri.create(invalidPrefixUri, new Properties()),
                    String.format(
                            "Flink JDBC URL[%s] must start with [jdbc:flink:]", invalidPrefixUri));
        }

        // Without host or port
        String noPortUri = "jdbc:flink://localhost/catalog";
        assertThrowsExactly(
                SQLException.class,
                () -> DriverUri.create(noPortUri, new Properties()),
                String.format("No port specified in uri: %s", noPortUri));

        Properties properties = new Properties();
        properties.setProperty("key3", "val33");
        for (String dupPropUri :
                Arrays.asList(
                        "jdbc:flink://localhost:8088/catalog?key1=val1&key2=val2&key3=val3",
                        "jdbc:flink://localhost:8088/catalog?key1=val1&key2=val2&key1=val3")) {
            assertThrowsExactly(
                    SQLException.class,
                    () -> DriverUri.create(dupPropUri, properties),
                    "Connection property 'key3' is both in the URL and an argument");
        }
    }
}
