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
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.ObjectPath;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests that views which reference themselves, directly or through other views, are rejected. */
class RecursiveViewTest {

    private TableEnvironment tEnv;
    private Catalog catalog;

    @BeforeEach
    void setUp() {
        tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        catalog = tEnv.getCatalog(tEnv.getCurrentCatalog()).get();
        tEnv.executeSql("CREATE TABLE t (a INT) WITH ('connector' = 'datagen')");
    }

    @Test
    void testAlterViewReferencingItself() {
        tEnv.executeSql("CREATE VIEW v AS SELECT * FROM t");

        assertThatThrownBy(() -> tEnv.executeSql("ALTER VIEW v AS SELECT * FROM v"))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "View `default_catalog`.`default_database`.`v` references itself: "
                                + "`default_catalog`.`default_database`.`v` -> "
                                + "`default_catalog`.`default_database`.`v`. "
                                + "Recursive views are not supported.");
        // The rejected statement leaves the stored definition, which reads from t, unchanged.
        assertThat(expandedQuery("v")).contains("`t`");
    }

    @Test
    void testAlterViewReferencingItselfThroughOtherViews() {
        tEnv.executeSql("CREATE VIEW v1 AS SELECT * FROM t");
        tEnv.executeSql("CREATE VIEW v2 AS SELECT * FROM v1");
        tEnv.executeSql("CREATE VIEW v3 AS SELECT * FROM v2");

        assertThatThrownBy(() -> tEnv.executeSql("ALTER VIEW v1 AS SELECT * FROM v3"))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "View `default_catalog`.`default_database`.`v1` references itself: "
                                + "`default_catalog`.`default_database`.`v1` -> "
                                + "`default_catalog`.`default_database`.`v3` -> "
                                + "`default_catalog`.`default_database`.`v2` -> "
                                + "`default_catalog`.`default_database`.`v1`. "
                                + "Recursive views are not supported.");
        // The rejected statement leaves the stored definition, which reads from t, unchanged.
        assertThat(expandedQuery("v1")).contains("`t`");
    }

    @Test
    void testCreateViewReadingTheSameViewTwice() {
        tEnv.executeSql("CREATE VIEW v1 AS SELECT * FROM t");
        tEnv.executeSql("CREATE VIEW v2 AS SELECT * FROM v1");

        assertThatNoException()
                .isThrownBy(
                        () -> {
                            tEnv.executeSql(
                                    "CREATE VIEW v3 AS SELECT * FROM v2 UNION ALL SELECT * FROM v2");
                            tEnv.explainSql("SELECT * FROM v3");
                        });
    }

    @Test
    void testQueryOnStoredRecursiveView() throws Exception {
        final String query = "SELECT * FROM `default_catalog`.`default_database`.`v`";
        catalog.createTable(
                new ObjectPath(catalog.getDefaultDatabase(), "v"),
                CatalogView.of(
                        Schema.newBuilder().column("a", "INT").build(),
                        null,
                        query,
                        query,
                        Collections.emptyMap()),
                false);

        assertThatThrownBy(() -> tEnv.explainSql("SELECT * FROM v"))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "View `default_catalog`.`default_database`.`v` references itself: "
                                + "`default_catalog`.`default_database`.`v` -> "
                                + "`default_catalog`.`default_database`.`v`. "
                                + "Recursive views are not supported.");
    }

    @Test
    void testCreateTemporaryViewShadowingTheViewItReferences() {
        tEnv.executeSql("CREATE VIEW v AS SELECT * FROM t");

        assertThatThrownBy(() -> tEnv.executeSql("CREATE TEMPORARY VIEW v AS SELECT * FROM v"))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "View `default_catalog`.`default_database`.`v` references itself: "
                                + "`default_catalog`.`default_database`.`v` -> "
                                + "`default_catalog`.`default_database`.`v`. "
                                + "Recursive views are not supported.");
        assertThat(tEnv.listTemporaryViews()).isEmpty();
    }

    private String expandedQuery(String viewName) {
        try {
            return ((CatalogView)
                            catalog.getTable(
                                    new ObjectPath(catalog.getDefaultDatabase(), viewName)))
                    .getExpandedQuery();
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }
}
