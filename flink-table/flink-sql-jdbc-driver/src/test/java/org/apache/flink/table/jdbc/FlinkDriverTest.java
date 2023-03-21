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
import java.util.Properties;

import static org.apache.flink.table.jdbc.DriverInfo.DRIVER_NAME;
import static org.apache.flink.table.jdbc.DriverInfo.DRIVER_VERSION_MAJOR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
        assertEquals("localhost", driverUri.getHost());
        assertEquals(8888, driverUri.getPort());
        assertEquals(5, driverUri.getProperties().size());

        properties.put("key1", "val11");
        assertThrows(
                SQLException.class,
                () -> DriverUri.create(uri, properties),
                "Connection property 'key1' is both in the URL and an argument");
    }
}
