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
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@code GROUP BY ALL} clause. */
class GroupByAllTest {

    private TableEnvironment tEnv;

    @BeforeEach
    void setUp() {
        // Batch mode: these assert final, non-updating results over bounded
        // VALUES inputs. Streaming coverage is tracked separately.
        tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.getConfig().set(TableConfigOptions.TABLE_GROUP_BY_ALL_ENABLED, true);
    }

    static Stream<Arguments> groupByAllQueries() {
        return Stream.of(
                Arguments.of(
                        "groups by non-aggregated columns",
                        "SELECT city, COUNT(*) AS cnt "
                                + "FROM (VALUES ('Beijing'), ('Shanghai'), ('Beijing')) AS t(city) "
                                + "GROUP BY ALL",
                        new Row[] {Row.of("Beijing", 2L), Row.of("Shanghai", 1L)}),
                Arguments.of(
                        "only aggregates produce one global group",
                        "SELECT COUNT(*) AS cnt "
                                + "FROM (VALUES ('Beijing'), ('Shanghai'), ('Beijing')) AS t(city) "
                                + "GROUP BY ALL",
                        new Row[] {Row.of(3L)}),
                Arguments.of(
                        "groups by a whole expression",
                        "SELECT a + b AS s, COUNT(*) AS cnt "
                                + "FROM (VALUES (1, 2), (3, 4), (1, 2)) AS t(a, b) "
                                + "GROUP BY ALL",
                        new Row[] {Row.of(3, 2L), Row.of(7, 1L)}),
                Arguments.of(
                        "excludes window functions from grouping",
                        "SELECT city, ROW_NUMBER() OVER (ORDER BY city) AS rn "
                                + "FROM (VALUES ('Beijing'), ('Shanghai'), ('Beijing')) AS t(city) "
                                + "GROUP BY ALL",
                        new Row[] {Row.of("Beijing", 1L), Row.of("Shanghai", 2L)}),
                Arguments.of(
                        "expands SELECT * to underlying columns before grouping",
                        "SELECT *, COUNT(*) AS cnt "
                                + "FROM (VALUES ('Beijing'), ('Shanghai'), ('Beijing')) AS t(city) "
                                + "GROUP BY ALL",
                        new Row[] {Row.of("Beijing", 2L), Row.of("Shanghai", 1L)}),
                Arguments.of(
                        "groups by multiple non-aggregated columns",
                        "SELECT a, b, COUNT(*) AS cnt "
                                + "FROM (VALUES (1, 2), (1, 2), (3, 4)) AS t(a, b) "
                                + "GROUP BY ALL",
                        new Row[] {Row.of(1, 2, 2L), Row.of(3, 4, 1L)}),
                Arguments.of(
                        "keeps a non-aggregated column that also appears in an aggregate expression",
                        "SELECT a, a + COUNT(*) AS x "
                                + "FROM (VALUES (1), (1), (2)) AS t(a) "
                                + "GROUP BY ALL",
                        new Row[] {Row.of(1, 3L), Row.of(2, 3L)}),
                Arguments.of(
                        "a single non-aggregated column behaves like SELECT DISTINCT",
                        "SELECT city "
                                + "FROM (VALUES ('Beijing'), ('Shanghai'), ('Beijing')) AS t(city) "
                                + "GROUP BY ALL",
                        new Row[] {Row.of("Beijing"), Row.of("Shanghai")}),
                Arguments.of(
                        "expands a qualified star (t.*) before grouping",
                        "SELECT t.*, COUNT(*) AS cnt "
                                + "FROM (VALUES ('Beijing'), ('Shanghai'), ('Beijing')) AS t(city) "
                                + "GROUP BY ALL",
                        new Row[] {Row.of("Beijing", 2L), Row.of("Shanghai", 1L)}));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("groupByAllQueries")
    void testGroupByAll(String description, String sql, Row[] expected) {
        final List<Row> actual = CollectionUtil.iteratorToList(tEnv.executeSql(sql).collect());
        assertThat(actual).containsExactlyInAnyOrder(expected);
    }

    @Test
    void testGroupByAllDisabledThrows() {
        tEnv.getConfig().set(TableConfigOptions.TABLE_GROUP_BY_ALL_ENABLED, false);
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                                "SELECT city, COUNT(*) AS cnt "
                                                        + "FROM (VALUES ('Beijing'), ('Shanghai'), ('Beijing')) AS t(city) "
                                                        + "GROUP BY ALL")
                                        .collect())
                .hasMessageContaining("GROUP BY ALL is not enabled");
    }

    @Test
    void testGroupByAllErrorsOnBareColumnInsideAggregateExpression() {
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                                "SELECT a + COUNT(*) "
                                                        + "FROM (VALUES (1), (2)) AS t(a) "
                                                        + "GROUP BY ALL")
                                        .collect())
                .hasMessageContaining("not being grouped");
    }

    @Test
    void testGroupByAllUnderCtas() throws Exception {
        final String dataId =
                TestValuesTableFactory.registerData(
                        Arrays.asList(Row.of(1, 10, 5), Row.of(1, 10, 7), Row.of(2, 20, 3)));
        tEnv.executeSql(
                "CREATE TABLE src (a INT, b INT, x INT) "
                        + "WITH ('connector' = 'values', 'bounded' = 'true', 'data-id' = '"
                        + dataId
                        + "')");
        // GROUP BY ALL groups by the non-aggregated SELECT columns (a, b); the CTAS target
        // schema is inferred from the query.
        tEnv.executeSql(
                        "CREATE TABLE sink WITH ('connector' = 'values', 'bounded' = 'true') "
                                + "AS SELECT a, b, SUM(x) AS total FROM src GROUP BY ALL")
                .await();
        assertThat(TestValuesTableFactory.getResultsAsStrings("sink"))
                .containsExactlyInAnyOrder("+I[1, 10, 12]", "+I[2, 20, 3]");
    }
}
