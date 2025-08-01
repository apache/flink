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

package org.apache.flink.table.planner.lineage;

import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.planner.lineage.TableLineageUtils.createTableLineageDataset;
import static org.apache.flink.table.utils.CatalogManagerMocks.DEFAULT_CATALOG;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link TableLineageUtils}. */
class TableLineageUtilsTest {

    private static final Catalog CATALOG = new GenericInMemoryCatalog(DEFAULT_CATALOG, "db1");
    private static final ResolvedSchema CATALOG_TABLE_RESOLVED_SCHEMA =
            new ResolvedSchema(
                    Arrays.asList(
                            Column.physical("a", DataTypes.STRING()),
                            Column.physical("b", DataTypes.INT()),
                            Column.physical("c", DataTypes.BOOLEAN())),
                    Collections.emptyList(),
                    null,
                    Collections.emptyList());
    private static final Schema CATALOG_TABLE_SCHEMA =
            Schema.newBuilder().fromResolvedSchema(CATALOG_TABLE_RESOLVED_SCHEMA).build();
    private static final Map<String, String> TABLE_OPTIONS = new HashMap<>();

    static {
        TABLE_OPTIONS.put("a", "1");
        TABLE_OPTIONS.put("b", "10");
        TABLE_OPTIONS.put("d", "4");
    }

    @Test
    void testCreateTableLineageDatasetWithCatalog() {
        final ObjectIdentifier objectIdentifier =
                ObjectIdentifier.of(DEFAULT_CATALOG, "my_db", "my_permanent_table");
        final ContextResolvedTable resolvedTable =
                ContextResolvedTable.permanent(
                        objectIdentifier,
                        CATALOG,
                        new ResolvedCatalogTable(
                                CatalogTable.newBuilder()
                                        .schema(CATALOG_TABLE_SCHEMA)
                                        .comment("my table")
                                        .partitionKeys(Collections.emptyList())
                                        .options(TABLE_OPTIONS)
                                        .build(),
                                CATALOG_TABLE_RESOLVED_SCHEMA));

        LineageDataset lineageDataset = createTableLineageDataset(resolvedTable, Optional.empty());
        assertThat(lineageDataset).isInstanceOf(TableLineageDatasetImpl.class);

        TableLineageDatasetImpl tableLineageDataset = (TableLineageDatasetImpl) lineageDataset;
        assertThat(tableLineageDataset.catalogContext().getCatalogName())
                .isEqualTo(DEFAULT_CATALOG);
        assertThat(tableLineageDataset.name()).isEqualTo(objectIdentifier.asSummaryString());
    }

    @Test
    void testCreateTableLineageDatasetWithoutCatalog() {
        final ObjectIdentifier objectIdentifier =
                ObjectIdentifier.of("default_cat", "default_db", "my_temporary_table");
        final ContextResolvedTable resolvedTable =
                ContextResolvedTable.temporary(
                        objectIdentifier,
                        new ResolvedCatalogTable(
                                CatalogTable.newBuilder()
                                        .schema(CATALOG_TABLE_SCHEMA)
                                        .comment("my table")
                                        .partitionKeys(Collections.emptyList())
                                        .options(TABLE_OPTIONS)
                                        .build(),
                                CATALOG_TABLE_RESOLVED_SCHEMA));

        LineageDataset lineageDataset = createTableLineageDataset(resolvedTable, Optional.empty());
        assertThat(lineageDataset).isInstanceOf(TableLineageDatasetImpl.class);

        TableLineageDatasetImpl tableLineageDataset = (TableLineageDatasetImpl) lineageDataset;
        assertThat(tableLineageDataset.catalogContext().getCatalogName()).isEmpty();
        assertThat(tableLineageDataset.name()).isEqualTo(objectIdentifier.asSummaryString());
    }
}
