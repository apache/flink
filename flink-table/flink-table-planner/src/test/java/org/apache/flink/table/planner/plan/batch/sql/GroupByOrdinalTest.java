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

/** Tests for positional references in the {@code GROUP BY} clause ({@code GROUP BY <n>}). */
class GroupByOrdinalTest {

    private TableEnvironment tEnv;

    @BeforeEach
    void setUp() {
        tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.getConfig().set(TableConfigOptions.TABLE_GROUP_BY_ORDINAL_ENABLED, true);
    }

    static Stream<Arguments> groupByOrdinalQueries() {
        return Stream.of(
                Arguments.of(
                        "ordinal refers to the n-th select column",
                        "SELECT city, COUNT(*) AS cnt "
                                + "FROM (VALUES ('Beijing'), ('Shanghai'), ('Beijing')) AS t(city) "
                                + "GROUP BY 1",
                        new Row[] {Row.of("Beijing", 2L), Row.of("Shanghai", 1L)}),
                Arguments.of(
                        "multiple ordinals",
                        "SELECT a, b, COUNT(*) AS cnt "
                                + "FROM (VALUES (1, 2), (1, 2), (3, 4)) AS t(a, b) "
                                + "GROUP BY 1, 2",
                        new Row[] {Row.of(1, 2, 2L), Row.of(3, 4, 1L)}),
                Arguments.of(
                        "ordinal mixed with an explicit column",
                        "SELECT a, b, COUNT(*) AS cnt "
                                + "FROM (VALUES (1, 2), (1, 2), (3, 4)) AS t(a, b) "
                                + "GROUP BY 1, b",
                        new Row[] {Row.of(1, 2, 2L), Row.of(3, 4, 1L)}),
                Arguments.of(
                        "ordinal refers to a whole expression",
                        "SELECT a + b AS s, COUNT(*) AS cnt "
                                + "FROM (VALUES (1, 2), (3, 4), (1, 2)) AS t(a, b) "
                                + "GROUP BY 1",
                        new Row[] {Row.of(3, 2L), Row.of(7, 1L)}));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("groupByOrdinalQueries")
    void testGroupByOrdinal(String description, String sql, Row[] expected) {
        final List<Row> actual = CollectionUtil.iteratorToList(tEnv.executeSql(sql).collect());
        assertThat(actual).containsExactlyInAnyOrder(expected);
    }

    @Test
    void testGroupByOrdinalDisabledTreatsLiteralAsConstant() {
        tEnv.getConfig().set(TableConfigOptions.TABLE_GROUP_BY_ORDINAL_ENABLED, false);
        // With ordinals off, "GROUP BY 1" groups by the constant 1, so the
        // ungrouped "city" column is rejected during validation.
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                                "SELECT city, COUNT(*) AS cnt "
                                                        + "FROM (VALUES ('Beijing'), ('Shanghai'), ('Beijing')) AS t(city) "
                                                        + "GROUP BY 1")
                                        .collect())
                .hasMessageContaining("not being grouped");
    }

    @Test
    void testGroupByOrdinalOutOfRangeThrows() {
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                                "SELECT a "
                                                        + "FROM (VALUES (1), (2)) AS t(a) "
                                                        + "GROUP BY 5")
                                        .collect())
                .hasMessageContaining("GROUP BY position 5 is not in the SELECT list");
    }

    @Test
    void testGroupByOrdinalZeroThrows() {
        // Ordinals are 1-based; 0 is below the valid range.
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                                "SELECT a "
                                                        + "FROM (VALUES (1), (2)) AS t(a) "
                                                        + "GROUP BY 0")
                                        .collect())
                .hasMessageContaining("GROUP BY position 0 is not in the SELECT list");
    }

    @Test
    void testGroupByOrdinalPointingAtAggregateThrows() {
        // Ordinal 1 resolves to COUNT(*), which is not a legal grouping key.
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                                "SELECT COUNT(*) AS cnt "
                                                        + "FROM (VALUES (1), (2)) AS t(a) "
                                                        + "GROUP BY 1")
                                        .collect())
                .hasMessageContaining("Aggregate expression is illegal in GROUP BY clause");
    }
}
