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

package org.apache.flink.table.planner.runtime.batch.sql.agg;

import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.config.AggregatePhaseStrategy;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.planner.runtime.utils.BatchTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;
import static org.assertj.core.api.Assertions.assertThat;

/** Regression tests for draining batch sliding group windows after an empty window. */
class GroupWindowLastPaneITCase extends BatchTestBase {

    @BeforeEach
    @Override
    public void before() {
        env().setParallelism(1);
        tEnv().createTemporaryView(
                        "src",
                        tEnv().fromValues(
                                        Row.of(LocalDateTime.of(2023, 1, 1, 0, 0, 0), "a"),
                                        Row.of(LocalDateTime.of(2023, 1, 1, 0, 0, 6), "b"))
                                .as("ts", "v"));
    }

    @Test
    void testOnePhaseCount() throws Exception {
        tEnv().getConfig()
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY,
                        AggregatePhaseStrategy.ONE_PHASE);
        assertWindows(
                tEnv().sqlQuery(
                                "SELECT HOP_START(ts, INTERVAL '2' SECOND, INTERVAL '4' SECOND), "
                                        + "COUNT(*) FROM src "
                                        + "GROUP BY HOP(ts, INTERVAL '2' SECOND, INTERVAL '4' SECOND)"),
                1L,
                1L);
    }

    @Test
    void testStringMax() throws Exception {
        assertWindows(
                tEnv().sqlQuery(
                                "SELECT HOP_START(ts, INTERVAL '2' SECOND, INTERVAL '4' SECOND), "
                                        + "MAX(v) FROM src "
                                        + "GROUP BY HOP(ts, INTERVAL '2' SECOND, INTERVAL '4' SECOND)"),
                "a",
                "b");
    }

    @Test
    void testTableApi() throws Exception {
        final Table result =
                tEnv().from("src")
                        .window(
                                Slide.over(lit(4).seconds())
                                        .every(lit(2).seconds())
                                        .on($("ts"))
                                        .as("w"))
                        .groupBy($("w"))
                        .select($("w").start(), $("v").max());
        assertWindows(result, "a", "b");
    }

    private void assertWindows(Table table, Object firstValue, Object lastValue) throws Exception {
        assertThat(table.explain()).contains("SortWindowAggregate");
        final List<Row> expected =
                Arrays.asList(
                        Row.of(LocalDateTime.of(2022, 12, 31, 23, 59, 58), firstValue),
                        Row.of(LocalDateTime.of(2023, 1, 1, 0, 0, 0), firstValue),
                        Row.of(LocalDateTime.of(2023, 1, 1, 0, 0, 4), lastValue),
                        Row.of(LocalDateTime.of(2023, 1, 1, 0, 0, 6), lastValue));
        try (CloseableIterator<Row> rows = table.execute().collect()) {
            assertThat(rows).toIterable().containsExactlyInAnyOrderElementsOf(expected);
        }
    }
}
