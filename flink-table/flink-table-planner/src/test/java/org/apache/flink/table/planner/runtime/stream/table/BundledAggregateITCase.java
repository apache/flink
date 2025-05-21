/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.	See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.runtime.stream.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.RowDataUtil;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.BundledAggregateFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.agg.BundledKeySegment;
import org.apache.flink.table.functions.agg.BundledKeySegmentApplied;
import org.apache.flink.table.planner.runtime.utils.StreamingTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests {@link BundledAggregateFunction}. */
public class BundledAggregateITCase extends StreamingTestBase {

    private TableEnvironment tEnv;

    @BeforeEach
    public void before() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        tEnv = StreamTableEnvironment.create(env, EnvironmentSettings.inStreamingMode());
    }

    @Test
    public void testGroupByWithRetractions() {
        tEnv.createTemporarySystemFunction("func", new SumAggregate());
        final List<Row> results =
                executeSql(
                        "select v1 % 2, func(v1) from (select k1, LAST_VALUE(v1) as v1 from (VALUES (1, 1), (2, 2), (5, 5), (2, 6), (1, 3)) AS t (k1, v1) group by k1) group by v1 % 2");
        final List<Row> expectedRows =
                Arrays.asList(
                        Row.ofKind(RowKind.INSERT, 1, 1L),
                        Row.ofKind(RowKind.INSERT, 0, 2L),
                        Row.ofKind(RowKind.UPDATE_BEFORE, 1, 1L),
                        Row.ofKind(RowKind.UPDATE_AFTER, 1, 6L),
                        Row.ofKind(RowKind.DELETE, 0, 2L),
                        Row.ofKind(RowKind.INSERT, 0, 6L),
                        Row.ofKind(RowKind.UPDATE_BEFORE, 1, 6L),
                        Row.ofKind(RowKind.UPDATE_AFTER, 1, 5L),
                        Row.ofKind(RowKind.UPDATE_BEFORE, 1, 5L),
                        Row.ofKind(RowKind.UPDATE_AFTER, 1, 8L));
        assertThat(results).containsSequence(expectedRows);
    }

    @Test
    public void testMissingRetractionSupport() {
        tEnv.createTemporarySystemFunction("func", new SumAggregateNoRetraction());
        assertThatThrownBy(
                        () ->
                                executeSql(
                                        "select v1 % 2, func(v1) from (select k1, LAST_VALUE(v1) as v1 from (VALUES (1, 1), (2, 2), (5, 5), (2, 6), (1, 3)) AS t (k1, v1) group by k1) group by v1 % 2"))
                .hasMessageContaining(
                        "Retract functionality is not implemented for the aggregate function");
    }

    @Test
    public void testNoRetractionsGlobal() {
        tEnv.createTemporarySystemFunction("MySum", new SumAggregate());
        final List<Row> results =
                executeSql(
                        "select MySum(v1) from (VALUES (1, 1), (2, 2), (5, 5), (2, 6), (1, 3)) AS t (k1, v1)");
        final List<Row> expectedRows =
                Arrays.asList(
                        Row.ofKind(RowKind.INSERT, 1L),
                        Row.ofKind(RowKind.UPDATE_BEFORE, 1L),
                        Row.ofKind(RowKind.UPDATE_AFTER, 3L),
                        Row.ofKind(RowKind.UPDATE_BEFORE, 3L),
                        Row.ofKind(RowKind.UPDATE_AFTER, 8L),
                        Row.ofKind(RowKind.UPDATE_BEFORE, 8L),
                        Row.ofKind(RowKind.UPDATE_AFTER, 14L),
                        Row.ofKind(RowKind.UPDATE_BEFORE, 14L),
                        Row.ofKind(RowKind.UPDATE_AFTER, 17L));
        assertThat(results).containsSequence(expectedRows);
    }

    @Test
    public void testNoRetractionsGlobalFilter() {
        tEnv.createTemporarySystemFunction("MySum", new SumAggregate());
        assertThatThrownBy(
                        () ->
                                executeSql(
                                        "select MySum(v1) FILTER(WHERE v1 % 2 = 0) from (VALUES (1, 1)) AS t (k1, v1)"))
                .hasMessageContaining("Filter operations not handled on bundled aggregates yet");
    }

    @Test
    public void testGroupByNoRetractions() {
        tEnv.createTemporarySystemFunction("MySum", new SumAggregate());
        final List<Row> results =
                executeSql(
                        "select v1 % 2, MySum(v1) from (VALUES (1, 1), (2, 2), (5, 5), (2, 6), (1, 3)) AS t (k1, v1) group by v1 % 2");
        final List<Row> expectedRows =
                Arrays.asList(
                        Row.ofKind(RowKind.INSERT, 1, 1L),
                        Row.ofKind(RowKind.INSERT, 0, 2L),
                        Row.ofKind(RowKind.UPDATE_BEFORE, 1, 1L),
                        Row.ofKind(RowKind.UPDATE_AFTER, 1, 6L),
                        Row.ofKind(RowKind.UPDATE_BEFORE, 0, 2L),
                        Row.ofKind(RowKind.UPDATE_AFTER, 0, 8L),
                        Row.ofKind(RowKind.UPDATE_BEFORE, 1, 6L),
                        Row.ofKind(RowKind.UPDATE_AFTER, 1, 9L));
        assertThat(results).containsSequence(expectedRows);
    }

    @Test
    public void testGroupMultipleNoRetractions() {
        tEnv.createTemporarySystemFunction("MySum", new SumAggregate());
        tEnv.createTemporarySystemFunction("MyAvg", new AvgAggregate());
        final List<Row> results =
                executeSql(
                        "select v1 % 2, MySum(v1), MyAvg(v1) from (VALUES (1, 1), (2, 2), (5, 5), (2, 6), (1, 3)) AS t (k1, v1) group by v1 % 2");
        final List<Row> expectedRows =
                Arrays.asList(
                        Row.ofKind(RowKind.INSERT, 1, 1L, 1L),
                        Row.ofKind(RowKind.INSERT, 0, 2L, 2L),
                        Row.ofKind(RowKind.UPDATE_BEFORE, 1, 1L, 1L),
                        Row.ofKind(RowKind.UPDATE_AFTER, 1, 6L, 3L),
                        Row.ofKind(RowKind.UPDATE_BEFORE, 0, 2L, 2L),
                        Row.ofKind(RowKind.UPDATE_AFTER, 0, 8L, 4L),
                        Row.ofKind(RowKind.UPDATE_BEFORE, 1, 6L, 3L),
                        Row.ofKind(RowKind.UPDATE_AFTER, 1, 9L, 3L));
        assertThat(results).containsSequence(expectedRows);
    }

    @Test
    public void testGroupByWithMultipleWithRetractions() {
        tEnv.createTemporarySystemFunction("MySum", new SumAggregate());
        tEnv.createTemporarySystemFunction("MyAvg", new AvgAggregate());
        final List<Row> results =
                executeSql(
                        "select v1 % 2, MySum(v1), MyAvg(v1) from (select k1, LAST_VALUE(v1) as v1 from (VALUES (1, 1), (2, 2), (5, 5), (2, 6), (1, 3)) AS t (k1, v1) group by k1) group by v1 % 2");
        final List<Row> expectedRows =
                Arrays.asList(
                        Row.ofKind(RowKind.INSERT, 1, 1L, 1L),
                        Row.ofKind(RowKind.INSERT, 0, 2L, 2L),
                        Row.ofKind(RowKind.UPDATE_BEFORE, 1, 1L, 1L),
                        Row.ofKind(RowKind.UPDATE_AFTER, 1, 6L, 3L),
                        Row.ofKind(RowKind.DELETE, 0, 2L, 2L),
                        Row.ofKind(RowKind.INSERT, 0, 6L, 6L),
                        Row.ofKind(RowKind.UPDATE_BEFORE, 1, 6L, 3L),
                        Row.ofKind(RowKind.UPDATE_AFTER, 1, 5L, 5L),
                        Row.ofKind(RowKind.UPDATE_BEFORE, 1, 5L, 5L),
                        Row.ofKind(RowKind.UPDATE_AFTER, 1, 8L, 4L));
        assertThat(results).containsSequence(expectedRows);
    }

    @Test
    public void testGroupMultipleOneSystem() {
        tEnv.createTemporarySystemFunction("MySum", new SumAggregate());
        final List<Row> results =
                executeSql(
                        "select v1 % 2, MySum(v1), AVG(v1) from (VALUES (1, 1), (2, 2), (5, 5), (2, 6), (1, 3)) AS t (k1, v1) group by v1 % 2");
        final List<Row> expectedRows =
                Arrays.asList(
                        Row.ofKind(RowKind.INSERT, 1, 1L, 1),
                        Row.ofKind(RowKind.INSERT, 0, 2L, 2),
                        Row.ofKind(RowKind.UPDATE_BEFORE, 1, 1L, 1),
                        Row.ofKind(RowKind.UPDATE_AFTER, 1, 6L, 3),
                        Row.ofKind(RowKind.UPDATE_BEFORE, 0, 2L, 2),
                        Row.ofKind(RowKind.UPDATE_AFTER, 0, 8L, 4),
                        Row.ofKind(RowKind.UPDATE_BEFORE, 1, 6L, 3),
                        Row.ofKind(RowKind.UPDATE_AFTER, 1, 9L, 3));
        assertThat(results).containsSequence(expectedRows);
    }

    @Test
    public void testGroupMultipleOneSystemDifferentOrder() {
        tEnv.createTemporarySystemFunction("MySum", new SumAggregate());
        final List<Row> results =
                executeSql(
                        "select v1 % 2, AVG(v1), MySum(v1) from (VALUES (1, 1), (2, 2), (5, 5), (2, 6), (1, 3)) AS t (k1, v1) group by v1 % 2");
        final List<Row> expectedRows =
                Arrays.asList(
                        Row.ofKind(RowKind.INSERT, 1, 1, 1L),
                        Row.ofKind(RowKind.INSERT, 0, 2, 2L),
                        Row.ofKind(RowKind.UPDATE_BEFORE, 1, 1, 1L),
                        Row.ofKind(RowKind.UPDATE_AFTER, 1, 3, 6L),
                        Row.ofKind(RowKind.UPDATE_BEFORE, 0, 2, 2L),
                        Row.ofKind(RowKind.UPDATE_AFTER, 0, 4, 8L),
                        Row.ofKind(RowKind.UPDATE_BEFORE, 1, 3, 6L),
                        Row.ofKind(RowKind.UPDATE_AFTER, 1, 3, 9L));
        assertThat(results).containsSequence(expectedRows);
    }

    @Test
    public void testGroupSingleBatchMultipleSystem() {
        tEnv.createTemporarySystemFunction("MySum", new SumAggregate());
        final List<Row> results =
                executeSql(
                        "select v1 % 2, MySum(v1), AVG(v1 + 10), AVG(v1) from (VALUES (1, 1), (2, 2), (5, 5), (2, 6), (1, 3)) AS t (k1, v1) group by v1 % 2");
        final List<Row> expectedRows =
                Arrays.asList(
                        Row.ofKind(RowKind.INSERT, 1, 1L, 11, 1),
                        Row.ofKind(RowKind.INSERT, 0, 2L, 12, 2),
                        Row.ofKind(RowKind.UPDATE_BEFORE, 1, 1L, 11, 1),
                        Row.ofKind(RowKind.UPDATE_AFTER, 1, 6L, 13, 3),
                        Row.ofKind(RowKind.UPDATE_BEFORE, 0, 2L, 12, 2),
                        Row.ofKind(RowKind.UPDATE_AFTER, 0, 8L, 14, 4),
                        Row.ofKind(RowKind.UPDATE_BEFORE, 1, 6L, 13, 3),
                        Row.ofKind(RowKind.UPDATE_AFTER, 1, 9L, 13, 3));
        assertThat(results).containsSequence(expectedRows);
    }

    @Test
    public void testGroupMultipleOneConventional() {
        tEnv.createTemporarySystemFunction("MySum", new SumAggregate());
        tEnv.createTemporarySystemFunction("MyAvg", new AvgAggregateFunction());
        final List<Row> results =
                executeSql(
                        "select v1 % 2, MySum(v1), MyAvg(v1) from (VALUES (1, 1), (2, 2), (5, 5), (2, 6), (1, 3)) AS t (k1, v1) group by v1 % 2");
        final List<Row> expectedRows =
                Arrays.asList(
                        Row.ofKind(RowKind.INSERT, 1, 1L, 1L),
                        Row.ofKind(RowKind.INSERT, 0, 2L, 2L),
                        Row.ofKind(RowKind.UPDATE_BEFORE, 1, 1L, 1L),
                        Row.ofKind(RowKind.UPDATE_AFTER, 1, 6L, 3L),
                        Row.ofKind(RowKind.UPDATE_BEFORE, 0, 2L, 2L),
                        Row.ofKind(RowKind.UPDATE_AFTER, 0, 8L, 4L),
                        Row.ofKind(RowKind.UPDATE_BEFORE, 1, 6L, 3L),
                        Row.ofKind(RowKind.UPDATE_AFTER, 1, 9L, 3L));
        assertThat(results).containsSequence(expectedRows);
    }

    /** Average bundled aggregate function. */
    @FunctionHint(accumulator = @DataTypeHint("ROW<sum BIGINT, count BIGINT>"))
    public static class AvgAggregate extends AggregateFunction<Long, Row>
            implements BundledAggregateFunction {

        private static final long serialVersionUID = 4585229396060575732L;

        @Override
        public boolean canBundle() {
            return true;
        }

        @Override
        public boolean canRetract() {
            return true;
        }

        private Long getAverage(Long sum, Long count) {
            if (count == 0) {
                return null;
            }
            return sum / count;
        }

        public void bundledAccumulateRetract(
                CompletableFuture<BundledKeySegmentApplied> future, BundledKeySegment segment)
                throws Exception {
            final GenericRowData acc;
            if (segment.getAccumulator() == null) {
                acc = GenericRowData.of(0L, 0L);
            } else {
                acc = (GenericRowData) segment.getAccumulator();
            }

            RowData previousValue = GenericRowData.of(getAverage(acc.getLong(0), acc.getLong(1)));

            List<RowData> valueUpdates = new ArrayList<>();
            for (RowData row : segment.getRows()) {
                if (RowDataUtil.isAccumulateMsg(row)) {
                    acc.setField(0, acc.getLong(0) + row.getLong(0));
                    acc.setField(1, acc.getLong(1) + 1);
                } else {
                    acc.setField(0, acc.getLong(0) - row.getLong(0));
                    acc.setField(1, acc.getLong(1) - 1);
                }
                if (segment.getUpdatedValuesAfterEachRow()) {
                    valueUpdates.add(GenericRowData.of(getAverage(acc.getLong(0), acc.getLong(1))));
                }
            }

            RowData newValue = GenericRowData.of(getAverage(acc.getLong(0), acc.getLong(1)));
            future.complete(
                    new BundledKeySegmentApplied(acc, previousValue, newValue, valueUpdates));
        }

        public void accumulate(Row acc, Long value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Long getValue(Row accumulator) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Row createAccumulator() {
            throw new UnsupportedOperationException();
        }
    }

    /** Sum bundled aggregate function. */
    @FunctionHint(accumulator = @DataTypeHint("ROW<sum BIGINT>"))
    public static class SumAggregate extends AggregateFunction<Long, Row>
            implements BundledAggregateFunction {

        private static final long serialVersionUID = -31497660659269145L;

        @Override
        public boolean canBundle() {
            return true;
        }

        @Override
        public boolean canRetract() {
            return true;
        }

        public void bundledAccumulateRetract(
                CompletableFuture<BundledKeySegmentApplied> future, BundledKeySegment segment)
                throws Exception {
            final GenericRowData acc;
            if (segment.getAccumulator() == null) {
                acc = GenericRowData.of(0L);
            } else {
                acc = (GenericRowData) segment.getAccumulator();
            }

            RowData previousValue = GenericRowData.of(acc.getLong(0));

            List<RowData> valueUpdates = new ArrayList<>();
            for (RowData row : segment.getRows()) {
                if (RowDataUtil.isAccumulateMsg(row)) {
                    acc.setField(0, acc.getLong(0) + row.getLong(0));
                } else {
                    acc.setField(0, acc.getLong(0) - row.getLong(0));
                }
                if (segment.getUpdatedValuesAfterEachRow()) {
                    valueUpdates.add(GenericRowData.of(acc.getLong(0)));
                }
            }

            RowData newValue = GenericRowData.of(acc.getLong(0));
            BundledKeySegmentApplied result =
                    new BundledKeySegmentApplied(acc, previousValue, newValue, valueUpdates);
            future.complete(result);
        }

        public void accumulate(Row acc, Long value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Long getValue(Row accumulator) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Row createAccumulator() {
            throw new UnsupportedOperationException();
        }
    }

    /** Sum bundled aggregate function without retraction. */
    public static class SumAggregateNoRetraction extends SumAggregate {

        @Override
        public boolean canRetract() {
            return false;
        }
    }

    private List<Row> executeSql(String sql) {
        TableResult result = tEnv.executeSql(sql);
        final List<Row> rows = new ArrayList<>();
        result.collect().forEachRemaining(rows::add);
        return rows;
    }

    /** Normal average aggregate function. */
    @FunctionHint(accumulator = @DataTypeHint("ROW<sum BIGINT, count BIGINT>"))
    public static class AvgAggregateFunction extends AggregateFunction<Long, Row> {

        @Override
        public void open(FunctionContext context) {
            System.out.println();
        }

        @Override
        public Long getValue(Row acc) {
            WrapperAvg wrapper = new WrapperAvg(acc);
            if (wrapper.getCount() == 0) {
                return null;
            }
            return (wrapper.getSum() / wrapper.getCount());
        }

        @Override
        public Row createAccumulator() {
            return Row.of(0L, 0L);
        }

        public void accumulate(Row acc, Long value) {
            WrapperAvg wrapper = new WrapperAvg(acc);
            wrapper.addCount(1);
            wrapper.addSum(value);
        }

        public void retract(Row acc, Long value) {
            WrapperAvg wrapper = new WrapperAvg(acc);
            wrapper.addCount(-1);
            wrapper.addSum(-value);
        }
    }

    private static class WrapperAvg {
        private final Row acc;

        public WrapperAvg(Row acc) {
            this.acc = acc;
        }

        public long getSum() {
            return (Long) acc.getField(0);
        }

        public void addSum(long sum) {
            acc.setField(0, getSum() + sum);
        }

        public long getCount() {
            return (Long) acc.getField(1);
        }

        public void addCount(long count) {
            acc.setField(1, getCount() + count);
        }
    }
}
