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

import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.StatementResult;
import org.apache.flink.table.data.RowData;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for flink database metadata. */
public class FlinkDatabaseMetaDataTest extends FlinkJdbcDriverTestBase {
    @Test
    public void testCatalogSchemas() throws Exception {

        DriverUri driverUri = getDriverUri();
        try (FlinkConnection connection = new FlinkConnection(driverUri)) {
            Executor executor = connection.getExecutor();
            // create databases in default catalog
            executeDDL("CREATE DATABASE database11", executor);
            executeDDL("CREATE DATABASE database12", executor);
            executeDDL("CREATE DATABASE database13", executor);

            // create catalog2 and databases
            executeDDL("CREATE CATALOG test_catalog2 WITH ('type'='generic_in_memory');", executor);
            executeDDL("CREATE DATABASE test_catalog2.database11", executor);
            executeDDL("CREATE DATABASE test_catalog2.database21", executor);
            executeDDL("CREATE DATABASE test_catalog2.database31", executor);

            // create catalog1 and databases
            executeDDL("CREATE CATALOG test_catalog1 WITH ('type'='generic_in_memory');", executor);
            executeDDL("CREATE DATABASE test_catalog1.database11", executor);
            executeDDL("CREATE DATABASE test_catalog1.database21", executor);
            executeDDL("CREATE DATABASE test_catalog1.database13", executor);

            connection.setCatalog("test_catalog2");
            connection.setSchema("database21");
            assertEquals("test_catalog2", connection.getCatalog());
            assertEquals("database21", connection.getSchema());

            DatabaseMetaData databaseMetaData =
                    new FlinkDatabaseMetaData(
                            driverUri.getURL(), connection, new TestingStatement());
            // Show all catalogs
            assertThat(resultSetToListAndClose(databaseMetaData.getCatalogs()))
                    .containsExactly("default_catalog", "test_catalog1", "test_catalog2");
            // Show all databases
            assertThat(resultSetToListAndClose(databaseMetaData.getSchemas()))
                    .containsExactly(
                            "database11,default_catalog",
                            "database12,default_catalog",
                            "database13,default_catalog",
                            "default_database,default_catalog",
                            "database11,test_catalog1",
                            "database13,test_catalog1",
                            "database21,test_catalog1",
                            "default,test_catalog1",
                            "database11,test_catalog2",
                            "database21,test_catalog2",
                            "database31,test_catalog2",
                            "default,test_catalog2");

            // Validate that the default catalog and database are not changed.
            assertEquals("test_catalog2", connection.getCatalog());
            assertEquals("database21", connection.getSchema());
        }
    }

    private List<String> resultSetToListAndClose(ResultSet resultSet) throws Exception {
        List<String> resultList = new ArrayList<>();
        int columnCount = resultSet.getMetaData().getColumnCount();
        while (resultSet.next()) {
            List<String> columnStringList = new ArrayList<>(columnCount);
            for (int i = 1; i <= columnCount; i++) {
                columnStringList.add(resultSet.getString(i));
            }
            resultList.add(StringUtils.join(columnStringList, ","));
        }
        resultSet.close();

        return resultList;
    }

    private void executeDDL(String sql, Executor executor) {
        try (StatementResult result = executor.executeStatement(sql)) {
            assertTrue(result.hasNext());
            RowData rowData = result.next();
            assertEquals(1, rowData.getArity());
            assertEquals("OK", rowData.getString(0).toString());

            assertFalse(result.hasNext());
        }
    }
}
