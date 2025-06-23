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

package org.apache.flink.table.planner.catalog;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogModel;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.utils.CatalogManagerMocks;

import org.apache.calcite.schema.Table;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.apache.flink.table.utils.CatalogManagerMocks.DEFAULT_CATALOG;
import static org.apache.flink.table.utils.CatalogManagerMocks.DEFAULT_DATABASE;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DatabaseCalciteSchema}. */
class DatabaseCalciteSchemaTest {

    private static final String TABLE_NAME = "tab";
    private static final String TEMP_TABLE_NAME = "tab_temp";
    private static final String MODEL_NAME = "model";
    private static final String TEMP_MODEL_NAME = "model_temp";

    @ParameterizedTest(name = "Permanent table {0}")
    @ValueSource(booleans = {true, false})
    void testGetTableWithPrimaryKey(boolean isPermanent) {
        final CatalogManager catalogManager = CatalogManagerMocks.createEmptyCatalogManager();

        final DatabaseCalciteSchema calciteSchema =
                new DatabaseCalciteSchema(DEFAULT_CATALOG, DEFAULT_DATABASE, catalogManager, true);

        if (isPermanent) {
            catalogManager.createTable(
                    createTable(),
                    ObjectIdentifier.of(DEFAULT_CATALOG, DEFAULT_DATABASE, TABLE_NAME),
                    false);
        } else {
            catalogManager.createTemporaryTable(
                    createTable(),
                    ObjectIdentifier.of(DEFAULT_CATALOG, DEFAULT_DATABASE, TEMP_TABLE_NAME),
                    false);
        }

        final Table table = calciteSchema.getTable(isPermanent ? TABLE_NAME : TEMP_TABLE_NAME);
        assertThat(table).isInstanceOf(CatalogSchemaTable.class);
        assertThat(((CatalogSchemaTable) table).getStatistic().getUniqueKeys().iterator().next())
                .containsExactlyInAnyOrder("a", "b");
    }

    @ParameterizedTest(name = "Permanent model {0}")
    @ValueSource(booleans = {true, false})
    void testGetModel(boolean isPermanent) {
        final CatalogManager catalogManager = CatalogManagerMocks.createEmptyCatalogManager();

        final DatabaseCalciteSchema calciteSchema =
                new DatabaseCalciteSchema(DEFAULT_CATALOG, DEFAULT_DATABASE, catalogManager, true);

        if (isPermanent) {
            catalogManager.createModel(
                    createModel(),
                    ObjectIdentifier.of(DEFAULT_CATALOG, DEFAULT_DATABASE, MODEL_NAME),
                    false);
        } else {
            catalogManager.createTemporaryModel(
                    createModel(),
                    ObjectIdentifier.of(DEFAULT_CATALOG, DEFAULT_DATABASE, TEMP_MODEL_NAME),
                    false);
        }

        final CatalogSchemaModel model =
                calciteSchema.getModel(isPermanent ? MODEL_NAME : TEMP_MODEL_NAME);
        assertThat(model).isInstanceOf(CatalogSchemaModel.class);
    }

    private CatalogBaseTable createTable() {
        final Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT().notNull())
                        .column("b", DataTypes.STRING().notNull())
                        .column("c", DataTypes.STRING())
                        .primaryKey("a", "b")
                        .build();

        return CatalogTable.newBuilder().schema(schema).build();
    }

    private CatalogModel createModel() {
        return CatalogModel.of(
                Schema.newBuilder().column("f1", DataTypes.INT()).build(),
                Schema.newBuilder().column("label", DataTypes.STRING()).build(),
                null,
                "some comment");
    }
}
