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

package org.apache.flink.table.planner.functions.aggfunctions;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.DecimalDataUtils;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.runtime.functions.aggregate.ApproxCountDistinctAggFunctions.ApproxCountDistinctAggFunction;
import org.apache.flink.table.runtime.functions.aggregate.ApproxCountDistinctAggFunctions.ByteApproxCountDistinctAggFunction;
import org.apache.flink.table.runtime.functions.aggregate.ApproxCountDistinctAggFunctions.DateApproxCountDistinctAggFunction;
import org.apache.flink.table.runtime.functions.aggregate.ApproxCountDistinctAggFunctions.DecimalApproxCountDistinctAggFunction;
import org.apache.flink.table.runtime.functions.aggregate.ApproxCountDistinctAggFunctions.DoubleApproxCountDistinctAggFunction;
import org.apache.flink.table.runtime.functions.aggregate.ApproxCountDistinctAggFunctions.FloatApproxCountDistinctAggFunction;
import org.apache.flink.table.runtime.functions.aggregate.ApproxCountDistinctAggFunctions.IntApproxCountDistinctAggFunction;
import org.apache.flink.table.runtime.functions.aggregate.ApproxCountDistinctAggFunctions.LongApproxCountDistinctAggFunction;
import org.apache.flink.table.runtime.functions.aggregate.ApproxCountDistinctAggFunctions.ShortApproxCountDistinctAggFunction;
import org.apache.flink.table.runtime.functions.aggregate.ApproxCountDistinctAggFunctions.StringApproxCountDistinctAggFunction;
import org.apache.flink.table.runtime.functions.aggregate.ApproxCountDistinctAggFunctions.TimeApproxCountDistinctAggFunction;
import org.apache.flink.table.runtime.functions.aggregate.ApproxCountDistinctAggFunctions.TimestampApproxCountDistinctAggFunction;
import org.apache.flink.table.runtime.functions.aggregate.ApproxCountDistinctAggFunctions.TimestampLtzApproxCountDistinctAggFunction;
import org.apache.flink.table.runtime.functions.aggregate.hyperloglog.HllBuffer;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.TimestampType;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test case for built-in APPROX_COUNT_DISTINCT aggregate function supporting both batch and
 * streaming modes.
 */
final class ApproxCountDistinctAggFunctionTest {

    /** Base class for the test. */
    abstract static class ApproxCountDistinctAggFunctionTestBase<IN>
            extends AggFunctionTestBase<IN, Long, HllBuffer> {

        @Override
        protected Class<?> getAccClass() {
            return HllBuffer.class;
        }
    }

    /** Test for merge functionality which is essential for Window TVF support. */
    abstract static class MergeTestBase<IN> extends ApproxCountDistinctAggFunctionTestBase<IN> {

        protected abstract ApproxCountDistinctAggFunction<IN> getTypedAggregator();

        @Override
        protected AggregateFunction<Long, HllBuffer> getAggregator() {
            return getTypedAggregator();
        }

        @Test
        void testMerge() {
            ApproxCountDistinctAggFunction<IN> agg = getTypedAggregator();

            // Create two accumulators
            HllBuffer buffer1 = agg.createAccumulator();
            HllBuffer buffer2 = agg.createAccumulator();

            // Add data to each accumulator separately
            List<List<IN>> inputs = getInputValueSets();
            if (inputs.size() >= 2) {
                for (IN value : inputs.get(0)) {
                    agg.accumulate(buffer1, value);
                }
                for (IN value : inputs.get(1)) {
                    agg.accumulate(buffer2, value);
                }

                // Merge two accumulators
                HllBuffer mergedBuffer = agg.createAccumulator();
                agg.merge(mergedBuffer, Arrays.asList(buffer1, buffer2));

                // Verify merged result is not null
                Long mergedResult = agg.getValue(mergedBuffer);
                assertThat(mergedResult).isNotNull();
                assertThat(mergedResult).isGreaterThanOrEqualTo(0L);

                // 合并结果应该小于等于两个独立结果之和
                Long result1 = agg.getValue(buffer1);
                Long result2 = agg.getValue(buffer2);
                assertThat(mergedResult).isLessThanOrEqualTo(result1 + result2);
            }
        }

        @Test
        void testMergeWithNullBuffer() {
            ApproxCountDistinctAggFunction<IN> agg = getTypedAggregator();

            HllBuffer buffer1 = agg.createAccumulator();
            List<List<IN>> inputs = getInputValueSets();
            if (!inputs.isEmpty()) {
                for (IN value : inputs.get(0)) {
                    agg.accumulate(buffer1, value);
                }
            }

            Long beforeMerge = agg.getValue(buffer1);

            // Merge with null buffer should not affect the result
            agg.merge(buffer1, Collections.singletonList(null));

            Long afterMerge = agg.getValue(buffer1);
            assertThat(afterMerge).isEqualTo(beforeMerge);
        }

        @Test
        void testResetAccumulator() {
            ApproxCountDistinctAggFunction<IN> agg = getTypedAggregator();

            HllBuffer buffer = agg.createAccumulator();
            List<List<IN>> inputs = getInputValueSets();
            if (!inputs.isEmpty()) {
                for (IN value : inputs.get(0)) {
                    agg.accumulate(buffer, value);
                }
            }

            // Reset and verify
            agg.resetAccumulator(buffer);
            assertThat(agg.getValue(buffer)).isEqualTo(0L);
        }
    }

    /** Test for {@link ByteApproxCountDistinctAggFunction}. */
    @Nested
    final class ByteApproxCountDistinctAggFunctionTest extends MergeTestBase<Byte> {

        @Override
        protected ApproxCountDistinctAggFunction<Byte> getTypedAggregator() {
            return new ByteApproxCountDistinctAggFunction();
        }

        @Override
        protected List<List<Byte>> getInputValueSets() {
            return Arrays.asList(
                    Arrays.asList(Byte.valueOf("1"), Byte.valueOf("1"), Byte.valueOf("1")),
                    Arrays.asList(Byte.valueOf("1"), Byte.valueOf("2"), Byte.valueOf("3")),
                    Arrays.asList(null, null, null, null, null, null),
                    Arrays.asList(null, Byte.valueOf("10")));
        }

        @Override
        protected List<Long> getExpectedResults() {
            return Arrays.asList(1L, 3L, 0L, 1L);
        }
    }

    /** Test for {@link ShortApproxCountDistinctAggFunction}. */
    @Nested
    final class ShortApproxCountDistinctAggFunctionTest extends MergeTestBase<Short> {

        @Override
        protected ApproxCountDistinctAggFunction<Short> getTypedAggregator() {
            return new ShortApproxCountDistinctAggFunction();
        }

        @Override
        protected List<List<Short>> getInputValueSets() {
            return Arrays.asList(
                    Arrays.asList(Short.valueOf("1"), Short.valueOf("1"), Short.valueOf("1")),
                    Arrays.asList(Short.valueOf("1"), Short.valueOf("2"), Short.valueOf("3")),
                    Arrays.asList(null, null, null, null, null, null),
                    Arrays.asList(null, Short.valueOf("10")));
        }

        @Override
        protected List<Long> getExpectedResults() {
            return Arrays.asList(1L, 3L, 0L, 1L);
        }
    }

    /** Test for {@link IntApproxCountDistinctAggFunction}. */
    @Nested
    final class IntegerApproxCountDistinctAggFunctionTest extends MergeTestBase<Integer> {

        @Override
        protected ApproxCountDistinctAggFunction<Integer> getTypedAggregator() {
            return new IntApproxCountDistinctAggFunction();
        }

        @Override
        protected List<List<Integer>> getInputValueSets() {
            return Arrays.asList(
                    Arrays.asList(Integer.valueOf("1"), Integer.valueOf("1"), Integer.valueOf("1")),
                    Arrays.asList(Integer.valueOf("1"), Integer.valueOf("2"), Integer.valueOf("3")),
                    Arrays.asList(null, null, null, null, null, null),
                    Arrays.asList(null, Integer.valueOf("10")));
        }

        @Override
        protected List<Long> getExpectedResults() {
            return Arrays.asList(1L, 3L, 0L, 1L);
        }
    }

    /** Test for {@link LongApproxCountDistinctAggFunction}. */
    @Nested
    final class LongApproxCountDistinctAggFunctionTest extends MergeTestBase<Long> {

        @Override
        protected ApproxCountDistinctAggFunction<Long> getTypedAggregator() {
            return new LongApproxCountDistinctAggFunction();
        }

        @Override
        protected List<List<Long>> getInputValueSets() {
            return Arrays.asList(
                    Arrays.asList(Long.valueOf("1"), Long.valueOf("1"), Long.valueOf("1")),
                    Arrays.asList(Long.valueOf("1"), Long.valueOf("2"), Long.valueOf("3")),
                    Arrays.asList(null, null, null, null, null, null),
                    Arrays.asList(null, Long.valueOf("10")));
        }

        @Override
        protected List<Long> getExpectedResults() {
            return Arrays.asList(1L, 3L, 0L, 1L);
        }
    }

    /** Test for {@link FloatApproxCountDistinctAggFunction}. */
    @Nested
    final class FloatApproxCountDistinctAggFunctionTest extends MergeTestBase<Float> {

        @Override
        protected ApproxCountDistinctAggFunction<Float> getTypedAggregator() {
            return new FloatApproxCountDistinctAggFunction();
        }

        @Override
        protected List<List<Float>> getInputValueSets() {
            return Arrays.asList(
                    Arrays.asList(1.0f, 1.0f, 1.0f),
                    Arrays.asList(1.0f, 2.0f, 3.0f),
                    Arrays.asList(0.0f, -0.0f),
                    Arrays.asList(Float.NaN, 1.1f, 2.2f),
                    Arrays.asList(null, null, null, null, null, null),
                    Arrays.asList(null, 10f));
        }

        @Override
        protected List<Long> getExpectedResults() {
            return Arrays.asList(1L, 3L, 1L, 3L, 0L, 1L);
        }
    }

    /** Test for {@link DoubleApproxCountDistinctAggFunction}. */
    @Nested
    final class DoubleApproxCountDistinctAggFunctionTest extends MergeTestBase<Double> {

        @Override
        protected ApproxCountDistinctAggFunction<Double> getTypedAggregator() {
            return new DoubleApproxCountDistinctAggFunction();
        }

        @Override
        protected List<List<Double>> getInputValueSets() {
            return Arrays.asList(
                    Arrays.asList(1.0d, 1.0d, 1.0d),
                    Arrays.asList(1.0d, 2.0d, 3.0d),
                    Arrays.asList(0.0d, -0.0d),
                    Arrays.asList(Double.NaN, 1.1d, 2.2d),
                    Arrays.asList(null, null, null, null, null, null),
                    Arrays.asList(null, 10d));
        }

        @Override
        protected List<Long> getExpectedResults() {
            return Arrays.asList(1L, 3L, 1L, 3L, 0L, 1L);
        }
    }

    /** Test for {@link DecimalApproxCountDistinctAggFunction}. */
    abstract static class DecimalApproxCountDistinctAggFunctionTestBase
            extends MergeTestBase<DecimalData> {

        private final int precision;
        private final int scale;

        DecimalApproxCountDistinctAggFunctionTestBase(int precision, int scale) {
            this.precision = precision;
            this.scale = scale;
        }

        @Override
        protected ApproxCountDistinctAggFunction<DecimalData> getTypedAggregator() {
            return new DecimalApproxCountDistinctAggFunction(new DecimalType(precision, scale));
        }

        @Override
        protected List<List<DecimalData>> getInputValueSets() {
            return Arrays.asList(
                    Arrays.asList(
                            DecimalDataUtils.castFrom("1", precision, scale),
                            DecimalDataUtils.castFrom("1000.000001", precision, scale),
                            DecimalDataUtils.castFrom("-1", precision, scale),
                            DecimalDataUtils.castFrom("-999.998999", precision, scale),
                            null,
                            DecimalDataUtils.castFrom("0", precision, scale),
                            DecimalDataUtils.castFrom("-999.999", precision, scale),
                            null,
                            DecimalDataUtils.castFrom("999.999", precision, scale)),
                    Arrays.asList(null, null, null, null, null),
                    Arrays.asList(null, DecimalDataUtils.castFrom("0", precision, scale)));
        }

        @Override
        protected List<Long> getExpectedResults() {
            return Arrays.asList(7L, 0L, 1L);
        }
    }

    /** Test for {@link DecimalApproxCountDistinctAggFunction} for 20 precision and 6 scale. */
    @Nested
    final class Decimal20ApproxCountDistinctAggFunctionTest
            extends DecimalApproxCountDistinctAggFunctionTestBase {

        Decimal20ApproxCountDistinctAggFunctionTest() {
            super(20, 6);
        }
    }

    /** Test for {@link DecimalApproxCountDistinctAggFunction} for 12 precision and 6 scale. */
    @Nested
    final class Decimal12ApproxCountDistinctAggFunctionTest
            extends DecimalApproxCountDistinctAggFunctionTestBase {

        Decimal12ApproxCountDistinctAggFunctionTest() {
            super(12, 6);
        }
    }

    /** Test for {@link DateApproxCountDistinctAggFunction}. */
    @Nested
    final class DateApproxCountDistinctAggFunctionTest extends MergeTestBase<Integer> {

        @Override
        protected ApproxCountDistinctAggFunction<Integer> getTypedAggregator() {
            return new DateApproxCountDistinctAggFunction();
        }

        @Override
        protected List<List<Integer>> getInputValueSets() {
            return Arrays.asList(
                    Arrays.asList(Integer.valueOf("1"), Integer.valueOf("1"), Integer.valueOf("1")),
                    Arrays.asList(Integer.valueOf("1"), Integer.valueOf("2"), Integer.valueOf("3")),
                    Arrays.asList(null, null, null, null, null, null),
                    Arrays.asList(null, Integer.valueOf("10")));
        }

        @Override
        protected List<Long> getExpectedResults() {
            return Arrays.asList(1L, 3L, 0L, 1L);
        }
    }

    /** Test for {@link TimeApproxCountDistinctAggFunction}. */
    @Nested
    final class TimeApproxCountDistinctAggFunctionTest extends MergeTestBase<Integer> {

        @Override
        protected ApproxCountDistinctAggFunction<Integer> getTypedAggregator() {
            return new TimeApproxCountDistinctAggFunction();
        }

        @Override
        protected List<List<Integer>> getInputValueSets() {
            return Arrays.asList(
                    Arrays.asList(Integer.valueOf("1"), Integer.valueOf("1"), Integer.valueOf("1")),
                    Arrays.asList(Integer.valueOf("1"), Integer.valueOf("2"), Integer.valueOf("3")),
                    Arrays.asList(null, null, null, null, null, null),
                    Arrays.asList(null, Integer.valueOf("10")));
        }

        @Override
        protected List<Long> getExpectedResults() {
            return Arrays.asList(1L, 3L, 0L, 1L);
        }
    }

    /** Test for {@link TimestampApproxCountDistinctAggFunction}. */
    @Nested
    final class TimestampApproxCountDistinctAggFunctionTest extends MergeTestBase<TimestampData> {

        @Override
        protected ApproxCountDistinctAggFunction<TimestampData> getTypedAggregator() {
            return new TimestampApproxCountDistinctAggFunction(new TimestampType(3));
        }

        @Override
        protected List<List<TimestampData>> getInputValueSets() {
            return Arrays.asList(
                    Arrays.asList(
                            TimestampData.fromEpochMillis(0),
                            TimestampData.fromEpochMillis(1000),
                            TimestampData.fromEpochMillis(100),
                            null,
                            TimestampData.fromEpochMillis(10)),
                    Arrays.asList(null, null, null, null, null),
                    Arrays.asList(
                            null,
                            TimestampData.fromEpochMillis(1),
                            TimestampData.fromEpochMillis(1)));
        }

        @Override
        protected List<Long> getExpectedResults() {
            return Arrays.asList(4L, 0L, 1L);
        }
    }

    /** Test for {@link TimestampLtzApproxCountDistinctAggFunction}. */
    @Nested
    final class TimestampLtzApproxCountDistinctAggFunctionTest
            extends MergeTestBase<TimestampData> {

        @Override
        protected ApproxCountDistinctAggFunction<TimestampData> getTypedAggregator() {
            return new TimestampLtzApproxCountDistinctAggFunction(new LocalZonedTimestampType(6));
        }

        @Override
        protected List<List<TimestampData>> getInputValueSets() {
            return Arrays.asList(
                    Arrays.asList(
                            TimestampData.fromEpochMillis(0),
                            TimestampData.fromEpochMillis(1000),
                            TimestampData.fromEpochMillis(100),
                            null,
                            TimestampData.fromEpochMillis(10)),
                    Arrays.asList(null, null, null, null, null),
                    Arrays.asList(
                            null,
                            TimestampData.fromEpochMillis(1),
                            TimestampData.fromEpochMillis(1)));
        }

        @Override
        protected List<Long> getExpectedResults() {
            return Arrays.asList(4L, 0L, 1L);
        }
    }

    /** Test for {@link StringApproxCountDistinctAggFunction}. */
    @Nested
    final class StringApproxCountDistinctAggFunctionTest extends MergeTestBase<BinaryStringData> {

        @Override
        protected ApproxCountDistinctAggFunction<BinaryStringData> getTypedAggregator() {
            return new StringApproxCountDistinctAggFunction();
        }

        @Override
        protected List<List<BinaryStringData>> getInputValueSets() {
            return Arrays.asList(
                    Arrays.asList(
                            BinaryStringData.fromString("abc"),
                            BinaryStringData.fromString("def"),
                            BinaryStringData.fromString("ghi"),
                            null,
                            BinaryStringData.fromString("jkl"),
                            null,
                            BinaryStringData.fromString("zzz")),
                    Arrays.asList(null, null),
                    Arrays.asList(null, BinaryStringData.fromString("a")),
                    Arrays.asList(
                            BinaryStringData.fromString("x"),
                            null,
                            BinaryStringData.fromString("x")));
        }

        @Override
        protected List<Long> getExpectedResults() {
            return Arrays.asList(5L, 0L, 1L, 1L);
        }
    }
}
