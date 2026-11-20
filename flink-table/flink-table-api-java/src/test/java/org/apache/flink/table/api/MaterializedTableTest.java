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

package org.apache.flink.table.api;

import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.CatalogMaterializedTable.LogicalRefreshMode;
import org.apache.flink.table.catalog.CatalogMaterializedTable.RefreshMode;
import org.apache.flink.table.catalog.CatalogMaterializedTable.RefreshStatus;
import org.apache.flink.table.catalog.IntervalFreshness;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.utils.TableEnvironmentMock;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link MaterializedTable}. */
class MaterializedTableTest {

    private static final ObjectIdentifier IDENTIFIER =
            ObjectIdentifier.of("default_catalog", "default_database", "mt");

    private TableEnvironmentMock tEnv;
    private Catalog catalog;

    @BeforeEach
    void setUp() throws Exception {
        tEnv = TableEnvironmentMock.getStreamingInstance();
        catalog = tEnv.getCatalog(IDENTIFIER.getCatalogName()).orElseThrow(AssertionError::new);
        catalog.createTable(
                IDENTIFIER.toObjectPath(),
                createMaterializedTable(
                        "SELECT 1 AS f0", "SELECT 1 AS `f0`", RefreshStatus.ACTIVATED, "job 1"),
                false);
    }

    @Test
    void testBasic() {
        final MaterializedTable materializedTable = tEnv.fromMaterializedTable("mt");

        assertThat(materializedTable.getIdentifier()).isEqualTo(IDENTIFIER);
        assertThat(materializedTable.getRefreshStatus()).isSameAs(RefreshStatus.ACTIVATED);
        assertThat(materializedTable.getOriginalQuery()).isEqualTo("SELECT 1 AS f0");
        assertThat(materializedTable.getExpandedQuery()).isEqualTo("SELECT 1 AS `f0`");
        assertThat(materializedTable.getFreshness()).isEqualTo(IntervalFreshness.ofSecond("30"));
        assertThat(materializedTable.getRefreshMode()).isSameAs(RefreshMode.CONTINUOUS);
        assertThat(materializedTable.getRefreshHandlerDescription()).hasValue("job 1");
    }

    @Test
    void testDefinitionIsKeptAndRefreshStateIsReadLive() throws Exception {
        final MaterializedTable materializedTable = tEnv.fromMaterializedTable("mt");

        catalog.alterTable(
                IDENTIFIER.toObjectPath(),
                createMaterializedTable(
                        "SELECT 2 AS f0", "SELECT 2 AS `f0`", RefreshStatus.SUSPENDED, "job 2"),
                false);

        assertThat(materializedTable.getExpandedQuery()).isEqualTo("SELECT 1 AS `f0`");
        assertThat(materializedTable.getRefreshStatus()).isSameAs(RefreshStatus.SUSPENDED);
        assertThat(materializedTable.getRefreshHandlerDescription()).hasValue("job 2");
        assertThat(tEnv.fromMaterializedTable("mt").getExpandedQuery())
                .isEqualTo("SELECT 2 AS `f0`");
    }

    private static CatalogMaterializedTable createMaterializedTable(
            String originalQuery,
            String expandedQuery,
            RefreshStatus refreshStatus,
            String refreshHandlerDescription) {
        return CatalogMaterializedTable.newBuilder()
                .schema(Schema.newBuilder().column("f0", DataTypes.INT()).build())
                .options(Map.of())
                .originalQuery(originalQuery)
                .expandedQuery(expandedQuery)
                .freshness(IntervalFreshness.ofSecond("30"))
                .logicalRefreshMode(LogicalRefreshMode.AUTOMATIC)
                .refreshMode(RefreshMode.CONTINUOUS)
                .refreshStatus(refreshStatus)
                .refreshHandlerDescription(refreshHandlerDescription)
                .build();
    }
}
