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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for flink data source. */
public class FlinkDataSourceTest extends FlinkJdbcDriverTestBase {

    @Test
    public void testDataSource() throws Exception {
        FlinkDataSource dataSource = new FlinkDataSource(getDriverUri().getURL(), new Properties());
        try (Connection connection = dataSource.getConnection()) {
            assertEquals("default_catalog", connection.getCatalog());
            assertEquals("default_database", connection.getSchema());
        }
    }

    @Test
    public void testUnwrapSuccessful() throws Exception {
        FlinkDataSource dataSource = new FlinkDataSource(getDriverUri().getURL(), new Properties());
        FlinkDataSource unwrap = dataSource.unwrap(FlinkDataSource.class);
        assertNotNull(unwrap);
    }

    @Test
    public void testUnwrapFailed() throws Exception {
        FlinkDataSource dataSource = new FlinkDataSource(getDriverUri().getURL(), new Properties());
        assertThrows(SQLException.class, () -> dataSource.unwrap(TestingDataSource.class));
    }

    @Test
    public void testIsWrapperFor() throws Exception {
        FlinkDataSource dataSource = new FlinkDataSource(getDriverUri().getURL(), new Properties());
        assertTrue(dataSource.isWrapperFor(FlinkDataSource.class));
        assertFalse(dataSource.isWrapperFor(TestingDataSource.class));
    }
}
