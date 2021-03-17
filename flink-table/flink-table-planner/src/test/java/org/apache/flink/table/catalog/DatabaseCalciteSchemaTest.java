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

package org.apache.flink.table.catalog;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.TestExternalTableSourceFactory.TestExternalTableSource;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.plan.schema.TableSourceTable;
import org.apache.flink.table.utils.CatalogManagerMocks;
import org.apache.flink.table.utils.ExpressionResolverMocks;

import org.apache.calcite.schema.Table;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.catalog.TestExternalTableSourceFactory.TEST_EXTERNAL_CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/** Tests for {@link DatabaseCalciteSchema}. */
public class DatabaseCalciteSchemaTest {

    private static final String catalogName = "cat";
    private static final String databaseName = "db";
    private static final String tableName = "tab";

    @Test
    public void testCatalogTable() throws TableAlreadyExistException, DatabaseNotExistException {
        GenericInMemoryCatalog catalog = new GenericInMemoryCatalog(catalogName, databaseName);
        CatalogManager catalogManager =
                CatalogManagerMocks.preparedCatalogManager()
                        .defaultCatalog(catalogName, catalog)
                        .build();
        catalogManager.initSchemaResolver(true, ExpressionResolverMocks.dummyResolver());

        DatabaseCalciteSchema calciteSchema =
                new DatabaseCalciteSchema(
                        true, databaseName, catalogName, catalogManager, new TableConfig());

        catalog.createTable(
                new ObjectPath(databaseName, tableName), new TestCatalogBaseTable(), false);
        Table table = calciteSchema.getTable(tableName);

        assertThat(table, instanceOf(TableSourceTable.class));
        TableSourceTable tableSourceTable = (TableSourceTable) table;
        assertThat(tableSourceTable.tableSource(), instanceOf(TestExternalTableSource.class));
        assertThat(tableSourceTable.isStreamingMode(), is(true));
    }

    private static final class TestCatalogBaseTable extends CatalogTableImpl {
        private TestCatalogBaseTable() {
            super(TableSchema.builder().build(), new HashMap<>(), "");
        }

        @Override
        public CatalogTable copy() {
            return this;
        }

        public CatalogTable copy(TableSchema tableSchema) {
            return this;
        }

        @Override
        public Map<String, String> toProperties() {
            Map<String, String> properties = new HashMap<>();
            properties.put(CONNECTOR_TYPE, TEST_EXTERNAL_CONNECTOR_TYPE);
            return properties;
        }
    }
}
