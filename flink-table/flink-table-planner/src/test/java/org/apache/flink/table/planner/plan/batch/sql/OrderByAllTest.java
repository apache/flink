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
 * limitations under the License.
 */

package org.apache.flink.table.planner.plan.batch.sql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@code ORDER BY ALL} clause. */
class OrderByAllTest {

    private TableEnvironment tEnv;

    @BeforeEach
    void setUp() {
        // Batch mode: ORDER BY ALL performs a global sort over bounded VALUES inputs, so results
        // are asserted in order. Streaming coverage (where ORDER BY requires a leading ascending
        // time attribute) is tracked separately.
        tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.getConfig().set(TableConfigOptions.TABLE_ORDER_BY_ALL_ENABLED, true);
    }

    static Stream<Arguments> orderByAllQueries() {
        return Stream.of(
                Arguments.of(
                        "sorts by every column, left to right",
                        "SELECT x, y "
                                + "FROM (VALUES (2, 'b'), (1, 'a'), (1, 'c')) AS t(x, y) "
                                + "ORDER BY ALL",
                        new Row[] {Row.of(1, "a"), Row.of(1, "c"), Row.of(2, "b")}),
                Arguments.of(
                        "a trailing DESC applies to all keys",
                        "SELECT x, y "
                                + "FROM (VALUES (2, 'b'), (1, 'a'), (1, 'c')) AS t(x, y) "
                                + "ORDER BY ALL DESC",
                        new Row[] {Row.of(2, "b"), Row.of(1, "c"), Row.of(1, "a")}),
                Arguments.of(
                        "expands SELECT * to every column before sorting",
                        "SELECT * "
                                + "FROM (VALUES (2, 'b'), (1, 'a'), (1, 'c')) AS t(x, y) "
                                + "ORDER BY ALL",
                        new Row[] {Row.of(1, "a"), Row.of(1, "c"), Row.of(2, "b")}),
                Arguments.of(
                        "expands a qualified star (t.*) before sorting",
                        "SELECT t.* "
                                + "FROM (VALUES (2, 'b'), (1, 'a'), (1, 'c')) AS t(x, y) "
                                + "ORDER BY ALL",
                        new Row[] {Row.of(1, "a"), Row.of(1, "c"), Row.of(2, "b")}));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("orderByAllQueries")
    void testOrderByAll(String description, String sql, Row[] expected) {
        final List<Row> actual = CollectionUtil.iteratorToList(tEnv.executeSql(sql).collect());
        assertThat(actual).containsExactly(expected);
    }

    @Test
    void testOrderByAllNullsFirstAppliesToAllKeys() {
        // The trailing NULLS FIRST applies to the (single) expanded sort key.
        final List<Row> actual =
                CollectionUtil.iteratorToList(
                        tEnv.executeSql(
                                        "SELECT x "
                                                + "FROM (VALUES (2), (CAST(NULL AS INT)), (1)) AS t(x) "
                                                + "ORDER BY ALL NULLS FIRST")
                                .collect());
        assertThat(actual).containsExactly(Row.of((Integer) null), Row.of(1), Row.of(2));
    }

    @Test
    void testOrderByAllDisabledThrows() {
        tEnv.getConfig().set(TableConfigOptions.TABLE_ORDER_BY_ALL_ENABLED, false);
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                                "SELECT x, y "
                                                        + "FROM (VALUES (2, 'b'), (1, 'a')) AS t(x, y) "
                                                        + "ORDER BY ALL")
                                        .collect())
                .hasMessageContaining("ORDER BY ALL is not enabled");
    }
}
