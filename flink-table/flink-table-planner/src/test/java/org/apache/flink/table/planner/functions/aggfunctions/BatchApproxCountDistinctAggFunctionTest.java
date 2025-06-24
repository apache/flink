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
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.runtime.functions.aggregate.BatchApproxCountDistinctAggFunctions.ApproxCountDistinctAggFunction;
import org.apache.flink.table.runtime.functions.aggregate.BatchApproxCountDistinctAggFunctions.ByteApproxCountDistinctAggFunction;
import org.apache.flink.table.runtime.functions.aggregate.BatchApproxCountDistinctAggFunctions.DateApproxCountDistinctAggFunction;
import org.apache.flink.table.runtime.functions.aggregate.BatchApproxCountDistinctAggFunctions.DecimalApproxCountDistinctAggFunction;
import org.apache.flink.table.runtime.functions.aggregate.BatchApproxCountDistinctAggFunctions.DoubleApproxCountDistinctAggFunction;
import org.apache.flink.table.runtime.functions.aggregate.BatchApproxCountDistinctAggFunctions.FloatApproxCountDistinctAggFunction;
import org.apache.flink.table.runtime.functions.aggregate.BatchApproxCountDistinctAggFunctions.IntApproxCountDistinctAggFunction;
import org.apache.flink.table.runtime.functions.aggregate.BatchApproxCountDistinctAggFunctions.LongApproxCountDistinctAggFunction;
import org.apache.flink.table.runtime.functions.aggregate.BatchApproxCountDistinctAggFunctions.ShortApproxCountDistinctAggFunction;
import org.apache.flink.table.runtime.functions.aggregate.BatchApproxCountDistinctAggFunctions.StringApproxCountDistinctAggFunction;
import org.apache.flink.table.runtime.functions.aggregate.BatchApproxCountDistinctAggFunctions.TimeApproxCountDistinctAggFunction;
import org.apache.flink.table.runtime.functions.aggregate.BatchApproxCountDistinctAggFunctions.TimestampApproxCountDistinctAggFunction;
import org.apache.flink.table.runtime.functions.aggregate.BatchApproxCountDistinctAggFunctions.TimestampLtzApproxCountDistinctAggFunction;
import org.apache.flink.table.runtime.functions.aggregate.hyperloglog.HllBuffer;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.TimestampType;

import org.junit.jupiter.api.Nested;

import java.util.Arrays;
import java.util.List;

/** Test case for built-in APPROX_COUNT_DISTINCT aggregate function. */
final class BatchApproxCountDistinctAggFunctionTest {

    /** Base class for the test. */
    abstract static class ApproxCountDistinctAggFunctionTestBase<IN>
            extends AggFunctionTestBase<IN, Long, HllBuffer> {

        @Override
        protected Class<?> getAccClass() {
            return HllBuffer.class;
        }
    }

    /** Test for {@link ByteApproxCountDistinctAggFunction}. */
    @Nested
    final class ByteApproxCountDistinctAggFunctionTest
            extends NumberApproxCountDistinctAggFunctionTestBase<Byte> {

        @Override
        protected AggregateFunction<Long, HllBuffer> getAggregator() {
            return new ByteApproxCountDistinctAggFunction();
        }

        @Override
        protected Byte getValue(String v) {
            return Byte.valueOf(v);
        }
    }

    /** Test for {@link ShortApproxCountDistinctAggFunction}. */
    @Nested
    final class ShortApproxCountDistinctAggFunctionTest
            extends NumberApproxCountDistinctAggFunctionTestBase<Short> {

        @Override
        protected AggregateFunction<Long, HllBuffer> getAggregator() {
            return new ShortApproxCountDistinctAggFunction();
        }

        @Override
        protected Short getValue(String v) {
            return Short.valueOf(v);
        }
    }

    /** Test for {@link ShortApproxCountDistinctAggFunction}. */
    @Nested
    final class IntegerApproxCountDistinctAggFunctionTest
            extends NumberApproxCountDistinctAggFunctionTestBase<Integer> {

        @Override
        protected AggregateFunction<Long, HllBuffer> getAggregator() {
            return new IntApproxCountDistinctAggFunction();
        }

        @Override
        protected Integer getValue(String v) {
            return Integer.valueOf(v);
        }
    }

    /** Test for {@link LongApproxCountDistinctAggFunction}. */
    @Nested
    final class LongApproxCountDistinctAggFunctionTest
            extends NumberApproxCountDistinctAggFunctionTestBase<Long> {

        @Override
        protected AggregateFunction<Long, HllBuffer> getAggregator() {
            return new LongApproxCountDistinctAggFunction();
        }

        @Override
        protected Long getValue(String v) {
            return Long.valueOf(v);
        }
    }

    /** Test for {@link FloatApproxCountDistinctAggFunction}. */
    @Nested
    final class FloatApproxCountDistinctAggFunctionTest
            extends ApproxCountDistinctAggFunctionTestBase<Float> {

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

        @Override
        protected AggregateFunction<Long, HllBuffer> getAggregator() {
            return new FloatApproxCountDistinctAggFunction();
        }
    }

    /** Test for {@link DoubleApproxCountDistinctAggFunction}. */
    @Nested
    final class DoubleApproxCountDistinctAggFunctionTest
            extends ApproxCountDistinctAggFunctionTestBase<Double> {

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

        @Override
        protected AggregateFunction<Long, HllBuffer> getAggregator() {
            return new DoubleApproxCountDistinctAggFunction();
        }
    }

    /** Test for {@link DecimalApproxCountDistinctAggFunction}. */
    abstract static class DecimalApproxCountDistinctAggFunctionTestBase
            extends ApproxCountDistinctAggFunctionTestBase<DecimalData> {

        private final int precision;
        private final int scale;

        DecimalApproxCountDistinctAggFunctionTestBase(int precision, int scale) {
            this.precision = precision;
            this.scale = scale;
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

        @Override
        protected AggregateFunction<Long, HllBuffer> getAggregator() {
            return new DecimalApproxCountDistinctAggFunction(new DecimalType(precision, scale));
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
    final class DateApproxCountDistinctAggFunctionTest
            extends NumberApproxCountDistinctAggFunctionTestBase<Integer> {

        @Override
        protected AggregateFunction<Long, HllBuffer> getAggregator() {
            return new DateApproxCountDistinctAggFunction();
        }

        @Override
        protected Integer getValue(String v) {
            return Integer.valueOf(v);
        }
    }

    /** Test for {@link DateApproxCountDistinctAggFunction}. */
    @Nested
    final class TimeApproxCountDistinctAggFunctionTest
            extends NumberApproxCountDistinctAggFunctionTestBase<Integer> {

        @Override
        protected AggregateFunction<Long, HllBuffer> getAggregator() {
            return new TimeApproxCountDistinctAggFunction();
        }

        @Override
        protected Integer getValue(String v) {
            return Integer.valueOf(v);
        }
    }

    /** Test for {@link TimestampApproxCountDistinctAggFunction}. */
    @Nested
    final class TimestampApproxCountDistinctAggFunctionTest
            extends ApproxCountDistinctAggFunctionTestBase<TimestampData> {

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

        @Override
        protected AggregateFunction<Long, HllBuffer> getAggregator() {
            return new TimestampApproxCountDistinctAggFunction(new TimestampType(3));
        }
    }

    /** Test for {@link TimestampLtzApproxCountDistinctAggFunction}. */
    @Nested
    final class TimestampLtzApproxCountDistinctAggFunctionTest
            extends ApproxCountDistinctAggFunctionTestBase<TimestampData> {

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

        @Override
        protected AggregateFunction<Long, HllBuffer> getAggregator() {
            return new TimestampLtzApproxCountDistinctAggFunction(new LocalZonedTimestampType(6));
        }
    }

    /** Test for {@link TimestampLtzApproxCountDistinctAggFunction}. */
    @Nested
    final class StringApproxCountDistinctAggFunctionTest
            extends ApproxCountDistinctAggFunctionTestBase<StringData> {

        @Override
        protected List<List<StringData>> getInputValueSets() {
            return Arrays.asList(
                    Arrays.asList(
                            StringData.fromString("abc"),
                            StringData.fromString("def"),
                            StringData.fromString("ghi"),
                            null,
                            StringData.fromString("jkl"),
                            null,
                            StringData.fromString("zzz")),
                    Arrays.asList(null, null),
                    Arrays.asList(null, StringData.fromString("a")),
                    Arrays.asList(StringData.fromString("x"), null, StringData.fromString("x")));
        }

        @Override
        protected List<Long> getExpectedResults() {
            return Arrays.asList(5L, 0L, 1L, 1L);
        }

        @Override
        protected AggregateFunction<Long, HllBuffer> getAggregator() {
            return new StringApproxCountDistinctAggFunction();
        }
    }

    /** Test base for {@link ApproxCountDistinctAggFunction} of numeric types. */
    abstract static class NumberApproxCountDistinctAggFunctionTestBase<T>
            extends ApproxCountDistinctAggFunctionTestBase<T> {

        protected abstract T getValue(String v);

        @Override
        protected List<List<T>> getInputValueSets() {
            return Arrays.asList(
                    Arrays.asList(getValue("1"), getValue("1"), getValue("1")),
                    Arrays.asList(getValue("1"), getValue("2"), getValue("3")),
                    Arrays.asList(null, null, null, null, null, null),
                    Arrays.asList(null, getValue("10")));
        }

        @Override
        protected List<Long> getExpectedResults() {
            return Arrays.asList(1L, 3L, 0L, 1L);
        }
    }
}
