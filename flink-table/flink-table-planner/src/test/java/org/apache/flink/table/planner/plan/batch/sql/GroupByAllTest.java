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
                        new Row[] {Row.of("Beijing", 1L), Row.of("Shanghai", 2L)}));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("groupByAllQueries")
    void testGroupByAll(String description, String sql, Row[] expected) {
        final List<Row> actual =
                CollectionUtil.iteratorToList(tEnv.executeSql(sql).collect());
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
}
