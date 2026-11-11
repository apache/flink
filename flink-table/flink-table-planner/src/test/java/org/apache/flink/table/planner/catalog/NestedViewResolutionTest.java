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

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.functions.TableFunction;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests resolution of nested views stored unresolved in a catalog, as persistent catalogs do.
 * Resolving such a view parses its query, so the resolution of a view referenced from other views
 * must be reused instead of being repeated for every lookup.
 */
class NestedViewResolutionTest {

    private static final int DEPTH = 6;

    private final Map<String, Integer> lookups = new HashMap<>();

    private TableEnvironment tEnv;
    private GenericInMemoryCatalog catalog;

    @BeforeEach
    void setUp() {
        tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        catalog = new LookupCountingCatalog();
        tEnv.registerCatalog("cat", catalog);
        tEnv.useCatalog("cat");
        tEnv.executeSql(
                "CREATE TABLE t (a INT, ts TIMESTAMP(3), WATERMARK FOR ts AS ts)"
                        + " WITH ('connector' = 'datagen')");
    }

    @Test
    void testRepeatedlyReferencedViewsAreLookedUpIndependentlyOfDepth() throws Exception {
        createView("v0", "SELECT * FROM t");
        for (int i = 1; i <= DEPTH; i++) {
            createView(
                    "v" + i,
                    String.format("SELECT * FROM v%1$d UNION ALL SELECT * FROM v%1$d", i - 1));
        }
        lookups.clear();

        tEnv.explainSql("SELECT * FROM v" + DEPTH);

        final int topNestedLookups = lookups.get("v" + (DEPTH - 1));
        assertThat(lookups.get("v0"))
                .as("lookups of the innermost view must not grow with the nesting depth")
                .isEqualTo(topNestedLookups);
        assertThat(lookups.get("t"))
                .as("lookups of the base table must not grow with the nesting depth")
                .isEqualTo(topNestedLookups);
    }

    /** Resolutions must not leak into later statements, which may see changed tables. */
    @Test
    void testEveryStatementResolvesViewsAgain() throws Exception {
        createView("v0", "SELECT * FROM t");
        createView("v1", "SELECT * FROM v0 UNION ALL SELECT * FROM v0");
        lookups.clear();
        final String firstPlan = tEnv.explainSql("SELECT * FROM v1");
        final Map<String, Integer> firstLookups = new HashMap<>(lookups);

        tEnv.executeSql("DROP TABLE t");
        tEnv.executeSql("CREATE TABLE t (a INT, ts TIMESTAMP(3)) WITH ('connector' = 'datagen')");
        lookups.clear();
        final String secondPlan = tEnv.explainSql("SELECT * FROM v1");

        assertThat(lookups).isEqualTo(firstLookups);
        assertThat(firstPlan).contains("WatermarkAssigner");
        assertThat(secondPlan).doesNotContain("WatermarkAssigner");
    }

    /** References to a reused resolution share its plan, including correlation variables. */
    @Test
    void testRepeatedlyReferencedCorrelatedViewsPlan() throws Exception {
        tEnv.createTemporarySystemFunction("split", SplitFunction.class);
        createView(
                "v0",
                "SELECT a, ts FROM t WHERE EXISTS (SELECT 1 FROM t t2 WHERE t2.a = t.a)"
                        + " AND a IN (SELECT t3.a FROM t t3 WHERE t3.ts = t.ts)");
        createView("v1", "SELECT a, ts FROM v0, LATERAL TABLE(split(a)) AS s(x) WHERE x > 0");
        createView("v2", "SELECT * FROM v1 UNION ALL SELECT * FROM v1");

        assertThat(tEnv.explainSql("SELECT * FROM v2 UNION ALL SELECT * FROM v2"))
                .contains("Correlate");
    }

    /** Emits its argument and its negation. */
    public static class SplitFunction extends TableFunction<Integer> {
        public void eval(Integer value) {
            collect(value);
            collect(-value);
        }
    }

    @Test
    void testTimeAttributesArePreservedThroughNestedViews() throws Exception {
        createView("v1", "SELECT * FROM t");
        createView("v2", "SELECT * FROM v1 UNION ALL SELECT * FROM v1");

        assertThat(
                        tEnv.explainSql(
                                "SELECT window_start, COUNT(*) FROM TABLE("
                                        + "TUMBLE(TABLE v2, DESCRIPTOR(ts), INTERVAL '1' MINUTE))"
                                        + " GROUP BY window_start, window_end"))
                .contains("WindowAggregate");
    }

    /**
     * Declares the view's columns with SQL type strings, as persistent catalogs restore them, so
     * the schema holds unresolved data types.
     */
    private void createView(String name, String query) throws Exception {
        catalog.createTable(
                new ObjectPath(catalog.getDefaultDatabase(), name),
                CatalogView.of(
                        Schema.newBuilder().column("a", "INT").column("ts", "TIMESTAMP(3)").build(),
                        null,
                        query,
                        query,
                        Collections.emptyMap()),
                false);
    }

    /**
     * Counts {@link #getTable} calls per object name and returns a fresh copy of each view, as
     * persistent catalogs do when restoring it from storage.
     */
    private class LookupCountingCatalog extends GenericInMemoryCatalog {

        LookupCountingCatalog() {
            super("cat");
        }

        @Override
        public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException {
            lookups.merge(tablePath.getObjectName(), 1, Integer::sum);
            final CatalogBaseTable table = super.getTable(tablePath);
            if (!(table instanceof CatalogView)) {
                return table;
            }
            final CatalogView view = (CatalogView) table;
            return CatalogView.of(
                    Schema.newBuilder().fromSchema(view.getUnresolvedSchema()).build(),
                    view.getComment(),
                    new String(view.getOriginalQuery()),
                    new String(view.getExpandedQuery()),
                    view.getOptions());
        }
    }
}
