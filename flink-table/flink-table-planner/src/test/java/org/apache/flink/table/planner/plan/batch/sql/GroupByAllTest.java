/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * imitations under the License.
 */

package org.apache.flink.table.planner.plan.batch.sql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/* Tests for the {@code GROUP BY ALL} clause. */
class GroupByAllTest {

    // TODO: test non aggregated only columns
    
    @Test
    void testGroupByAllByNonAggregateColumns() {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.getConfig().set(TableConfigOptions.TABLE_GROUP_BY_ALL_ENABLED, true);

        final List<Row> actual = 
                CollectionUtil.iteratorToList(
                    tEnv.executeSql(
                            "SELECT city, COUNT(*) AS cnt "
                                    + "FROM (VALUES ('Beijing'), ('Shanghai'), ('Beijing')) AS t(city) "
                                    + "GROUP BY ALL")
                            .collect()
                    );
        assertThat(actual).containsExactlyInAnyOrder(Row.of("Beijing", 2L), Row.of("Shanghai", 1L));
    }

    @Test
    void testGroupByAllDisabledByDefault() {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());

        assertThatThrownBy(
                        () -> tEnv.executeSql(
                                "SELECT city, COUNT(*) AS cnt "
                                        + "FROM (VALUES ('Beijing'), ('Shanghai'), ('Beijing')) AS t(city) "
                                        + "GROUP BY ALL")
                                .collect())
                .hasMessageContaining("GROUP BY ALL is not enabled");
    }

    @Test
    void testGroupByAllWithOnlyAggregates() {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.getConfig().set(TableConfigOptions.TABLE_GROUP_BY_ALL_ENABLED, true);

        final List<Row> actual =
                CollectionUtil.iteratorToList(
                        tEnv.executeSql(
                                        "SELECT COUNT(*) AS cnt "
                                                + "FROM (VALUES ('Beijing'), ('Shanghai'), ('Beijing')) AS t(city) "
                                                + "GROUP BY ALL")
                                .collect());
        assertThat(actual).containsExactly(Row.of(3L));
    }

    @Test
    void testGroupByAllGroupsByWholeExpression() {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.getConfig().set(TableConfigOptions.TABLE_GROUP_BY_ALL_ENABLED, true);

        final List<Row> actual =
                CollectionUtil.iteratorToList(
                        tEnv.executeSql(
                                        "SELECT a + b AS s, COUNT(*) AS cnt "
                                                + "FROM (VALUES (1, 2), (3, 4), (1, 2)) AS t(a, b) "
                                                + "GROUP BY ALL")
                                .collect());
        assertThat(actual)
                .containsExactlyInAnyOrder(
                        Row.of(3,2L), Row.of(7,1L));
    }

    @Test
    void testGroupByAllExcludesWindowFunctions() {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.getConfig().set(TableConfigOptions.TABLE_GROUP_BY_ALL_ENABLED, true);

        final List<Row> actual =
                 CollectionUtil.iteratorToList(
                         tEnv.executeSql(
                                         "SELECT city, ROW_NUMBER() OVER (ORDER BY city) AS rn "
                                                 + "FROM (VALUES ('Beijing'), ('Shanghai'), ('Beijing')) AS t(city) "
                                                 + "GROUP BY ALL")
                                 .collect());
        assertThat(actual).containsExactlyInAnyOrder(Row.of("Beijing", 1L), Row.of("Shanghai", 2L));
    }

    @Test
    void testGroupByAllErrorsOnBareColumnInsideAggregateExpression() {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.getConfig().set(TableConfigOptions.TABLE_GROUP_BY_ALL_ENABLED, true);

        assertThatThrownBy(
                        () -> tEnv.executeSql(
                                        "SELECT a + COUNT(*) "
                                                + "FROM (VALUES (1), (2)) AS t(a) "
                                                + "GROUP BY ALL")
                                .collect())
                .hasMessageContaining("not being grouped");
    }
}
