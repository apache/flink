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

package org.apache.flink.table.planner.runtime.stream.sql;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.runtime.utils.StreamingTestBase;
import org.apache.flink.types.Row;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class OuterJoinAggregateITCase extends StreamingTestBase {

    @ParameterizedTest
    @ValueSource(strings = {"LEFT", "RIGHT", "FULL"})
    void testAggregateRetractsUnmatchedRows(String joinType) throws Exception {
        env().setParallelism(1);
        String sink = "result_" + UUID.randomUUID().toString().replace('-', '_');
        String dataId =
                TestValuesTableFactory.registerData(
                        Arrays.asList(Row.of(3), Row.of(-9), Row.of(-4)));
        tEnv().executeSql(
                        "CREATE TABLE a (x INT) WITH ('connector' = 'values', 'data-id' = '"
                                + dataId
                                + "', 'bounded' = 'true')");

        DataGeneratorSource<Row> matches =
                new DataGeneratorSource<>(
                        new MatchesAfterInitialAggregate(sink),
                        2,
                        Types.ROW_NAMED(new String[] {"y"}, Types.INT));
        tEnv().createTemporaryView(
                        "b",
                        tEnv().fromDataStream(
                                        env().fromSource(
                                                        matches,
                                                        WatermarkStrategy.noWatermarks(),
                                                        "matches")));
        tEnv().executeSql(
                        "CREATE TABLE "
                                + sink
                                + " (max_x INT, min_x INT, cnt BIGINT, sum_x INT) "
                                + "WITH ('connector' = 'values', 'sink-insert-only' = 'false')");

        String join =
                joinType.equals("RIGHT")
                        ? "b RIGHT JOIN a ON x = y"
                        : "a " + joinType + " JOIN b ON x = y";
        tEnv().executeSql(
                        "INSERT INTO "
                                + sink
                                + " "
                                + "SELECT MAX(x), MIN(x), COUNT(*), SUM(x) FROM "
                                + join
                                + " WHERE y IS NULL")
                .await();

        assertThat(TestValuesTableFactory.getResults(sink)).containsExactly(Row.of(-4, -4, 1L, -4));
    }

    private static class MatchesAfterInitialAggregate implements GeneratorFunction<Long, Row> {
        private final String sink;

        private MatchesAfterInitialAggregate(String sink) {
            this.sink = sink;
        }

        @Override
        public Row map(Long index) throws Exception {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            while (TestValuesTableFactory.getRawResults(sink).stream()
                    .noneMatch(row -> Long.valueOf(3).equals(row.getField(2)))) {
                if (System.nanoTime() >= deadline) {
                    throw new IllegalStateException("Initial aggregate did not reach sink");
                }
                Thread.sleep(10);
            }
            return Row.of(index == 0 ? 3 : -9);
        }
    }
}
