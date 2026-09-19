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
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.ObjectPath;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests that a catalog view's expanded query resolves unqualified table references against the
 * view's own catalog/database rather than falling back to the current session database
 * (FLINK-40637, FLIP-71). This mirrors external catalogs (e.g. Iceberg) that return portable,
 * unqualified SQL from {@link CatalogView#getExpandedQuery()}.
 */
class HermeticViewExpansionTest {

    private TableEnvironment tEnv;
    private Catalog catalog;

    @BeforeEach
    void setUp() throws Exception {
        tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        catalog = tEnv.getCatalog(tEnv.getCurrentCatalog()).get();
        catalog.createDatabase("db2", new CatalogDatabaseImpl(Collections.emptyMap(), null), false);
    }

    private CatalogView unqualifiedView() {
        // Simulates an external catalog returning portable SQL with an unqualified reference.
        return CatalogView.of(
                Schema.newBuilder().column("a", DataTypes.INT()).build(),
                null,
                "SELECT * FROM t1",
                "SELECT * FROM t1",
                Collections.emptyMap());
    }

    @Test
    void testViewExpansionDoesNotFallBackToSessionDatabase() throws Exception {
        // t1 exists only in the session database (default_database), NOT in db2 where the view
        // lives. Expanding the view must resolve `t1` against db2 and therefore fail, instead of
        // silently picking up the session's default_database.t1.
        tEnv.executeSql("CREATE TABLE t1 (a INT) WITH ('connector' = 'datagen')");
        catalog.createTable(new ObjectPath("db2", "v"), unqualifiedView(), false);

        assertThatThrownBy(() -> tEnv.explainSql("SELECT * FROM db2.v"))
                .hasMessageContaining("Object 't1' not found");
    }

    @Test
    void testViewExpansionResolvesAgainstViewDatabase() throws Exception {
        // t1 exists in the view's own database (db2). Expansion must resolve `t1` to db2.t1,
        // regardless of the session sitting on default_database.
        tEnv.executeSql("CREATE TABLE db2.t1 (a INT) WITH ('connector' = 'datagen')");
        catalog.createTable(new ObjectPath("db2", "v"), unqualifiedView(), false);

        assertThat(tEnv.explainSql("SELECT * FROM db2.v"))
                .contains("default_catalog, db2, t1")
                .doesNotContain("default_database, t1");
    }
}
