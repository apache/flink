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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.DecimalDataUtils;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.UserDefinedFunctionHelper;
import org.apache.flink.table.planner.functions.aggfunctions.MinWithRetractAggFunction.MinWithRetractAccumulator;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

/** Test case for built-in Min with retraction aggregate function. */
@RunWith(Enclosed.class)
public final class MinWithRetractAggFunctionTest {

    // --------------------------------------------------------------------------------------------
    // Test sets for a particular type being aggregated
    //
    // Actual tests are implemented in:
    //  - AggFunctionTestBase
    // --------------------------------------------------------------------------------------------

    /** Test for {@link TinyIntType}. */
    public static final class ByteMinWithRetractAggFunctionTest
            extends NumberMinWithRetractAggFunctionTestBase<Byte> {

        @Override
        protected Byte getMinValue() {
            return Byte.MIN_VALUE + 1;
        }

        @Override
        protected Byte getMaxValue() {
            return Byte.MAX_VALUE - 1;
        }

        @Override
        protected Byte getValue(String v) {
            return Byte.valueOf(v);
        }

        @Override
        protected AggregateFunction<Byte, MinWithRetractAccumulator<Byte>> getAggregator() {
            return new MinWithRetractAggFunction<>(DataTypes.TINYINT().getLogicalType());
        }
    }

    /** Test for {@link SmallIntType}. */
    public static final class ShortMinWithRetractAggFunctionTest
            extends NumberMinWithRetractAggFunctionTestBase<Short> {

        @Override
        protected Short getMinValue() {
            return Short.MIN_VALUE + 1;
        }

        @Override
        protected Short getMaxValue() {
            return Short.MAX_VALUE - 1;
        }

        @Override
        protected Short getValue(String v) {
            return Short.valueOf(v);
        }

        @Override
        protected AggregateFunction<Short, MinWithRetractAccumulator<Short>> getAggregator() {
            return new MinWithRetractAggFunction<>(DataTypes.SMALLINT().getLogicalType());
        }
    }

    /** Test for {@link IntType}. */
    public static final class IntMinWithRetractAggFunctionTest
            extends NumberMinWithRetractAggFunctionTestBase<Integer> {

        @Override
        protected Integer getMinValue() {
            return Integer.MIN_VALUE + 1;
        }

        @Override
        protected Integer getMaxValue() {
            return Integer.MAX_VALUE - 1;
        }

        @Override
        protected Integer getValue(String v) {
            return Integer.valueOf(v);
        }

        @Override
        protected AggregateFunction<Integer, MinWithRetractAccumulator<Integer>> getAggregator() {
            return new MinWithRetractAggFunction<>(DataTypes.INT().getLogicalType());
        }
    }

    /** Test for {@link BigIntType}. */
    public static final class LongMinWithRetractAggFunctionTest
            extends NumberMinWithRetractAggFunctionTestBase<Long> {

        @Override
        protected Long getMinValue() {
            return Long.MIN_VALUE + 1;
        }

        @Override
        protected Long getMaxValue() {
            return Long.MAX_VALUE - 1;
        }

        @Override
        protected Long getValue(String v) {
            return Long.valueOf(v);
        }

        @Override
        protected AggregateFunction<Long, MinWithRetractAccumulator<Long>> getAggregator() {
            return new MinWithRetractAggFunction<>(DataTypes.BIGINT().getLogicalType());
        }
    }

    /** Test for {@link FloatType}. */
    public static final class FloatMinWithRetractAggFunctionTest
            extends NumberMinWithRetractAggFunctionTestBase<Float> {

        @Override
        protected Float getMinValue() {
            return -Float.MAX_VALUE / 2;
        }

        @Override
        protected Float getMaxValue() {
            return Float.MAX_VALUE / 2;
        }

        @Override
        protected Float getValue(String v) {
            return Float.valueOf(v);
        }

        @Override
        protected AggregateFunction<Float, MinWithRetractAccumulator<Float>> getAggregator() {
            return new MinWithRetractAggFunction<>(DataTypes.FLOAT().getLogicalType());
        }
    }

    /** Test for {@link DoubleType}. */
    public static final class DoubleMinWithRetractAggFunctionTest
            extends NumberMinWithRetractAggFunctionTestBase<Double> {

        @Override
        protected Double getMinValue() {
            return -Double.MAX_VALUE / 2;
        }

        @Override
        protected Double getMaxValue() {
            return Double.MAX_VALUE / 2;
        }

        @Override
        protected Double getValue(String v) {
            return Double.valueOf(v);
        }

        @Override
        protected AggregateFunction<Double, MinWithRetractAccumulator<Double>> getAggregator() {
            return new MinWithRetractAggFunction<>(DataTypes.DOUBLE().getLogicalType());
        }
    }

    /** Test for {@link BooleanType}. */
    public static final class BooleanMinWithRetractAggFunctionTest
            extends MinWithRetractAggFunctionTestBase<Boolean> {

        @Override
        protected List<List<Boolean>> getInputValueSets() {
            return Arrays.asList(
                    Arrays.asList(false, false, false),
                    Arrays.asList(true, true, true),
                    Arrays.asList(true, false, null, true, false, true, null),
                    Arrays.asList(null, null, null),
                    Arrays.asList(null, true));
        }

        @Override
        protected List<Boolean> getExpectedResults() {
            return Arrays.asList(false, true, false, null, true);
        }

        @Override
        protected AggregateFunction<Boolean, MinWithRetractAccumulator<Boolean>> getAggregator() {
            return new MinWithRetractAggFunction<>(DataTypes.BOOLEAN().getLogicalType());
        }
    }

    /** Test for {@link DecimalType}. */
    public static final class DecimalMinWithRetractAggFunctionTest
            extends MinWithRetractAggFunctionTestBase<DecimalData> {

        private int precision = 20;
        private int scale = 6;

        @Override
        protected List<List<DecimalData>> getInputValueSets() {
            return Arrays.asList(
                    Arrays.asList(
                            DecimalDataUtils.castFrom("1", precision, scale),
                            DecimalDataUtils.castFrom("1000", precision, scale),
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
        protected List<DecimalData> getExpectedResults() {
            return Arrays.asList(
                    DecimalDataUtils.castFrom("-999.999", precision, scale),
                    null,
                    DecimalDataUtils.castFrom("0", precision, scale));
        }

        @Override
        protected AggregateFunction<DecimalData, MinWithRetractAccumulator<DecimalData>>
                getAggregator() {
            return new MinWithRetractAggFunction<>(
                    DataTypes.DECIMAL(precision, scale).getLogicalType());
        }
    }

    /** Test for {@link VarCharType}. */
    public static final class StringMinWithRetractAggFunctionTest
            extends MinWithRetractAggFunctionTestBase<StringData> {

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
                    Arrays.asList(StringData.fromString("x"), null, StringData.fromString("e")));
        }

        @Override
        protected List<StringData> getExpectedResults() {
            return Arrays.asList(
                    StringData.fromString("abc"),
                    null,
                    StringData.fromString("a"),
                    StringData.fromString("e"));
        }

        @Override
        protected AggregateFunction<StringData, MinWithRetractAccumulator<StringData>>
                getAggregator() {
            return new MinWithRetractAggFunction<>(DataTypes.STRING().getLogicalType());
        }
    }

    /** Test for {@link TimestampType}. */
    public static final class TimestampMinWithRetractAggFunctionTest
            extends MinWithRetractAggFunctionTestBase<TimestampData> {

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
                    Arrays.asList(null, TimestampData.fromEpochMillis(1)));
        }

        @Override
        protected List<TimestampData> getExpectedResults() {
            return Arrays.asList(
                    TimestampData.fromEpochMillis(0), null, TimestampData.fromEpochMillis(1));
        }

        @Override
        protected AggregateFunction<TimestampData, MinWithRetractAccumulator<TimestampData>>
                getAggregator() {
            return new MinWithRetractAggFunction<>(DataTypes.TIMESTAMP(3).getLogicalType());
        }
    }

    /** Test for {@link TimestampType} with precision 9. */
    public static final class Timestamp9MinWithRetractAggFunctionTest
            extends MinWithRetractAggFunctionTestBase<TimestampData> {

        @Override
        protected List<List<TimestampData>> getInputValueSets() {
            return Arrays.asList(
                    Arrays.asList(
                            TimestampData.fromEpochMillis(0, 1),
                            TimestampData.fromEpochMillis(0, 2),
                            TimestampData.fromEpochMillis(1000, 0),
                            TimestampData.fromEpochMillis(100, 0),
                            null,
                            TimestampData.fromEpochMillis(10, 0)),
                    Arrays.asList(null, null, null, null, null),
                    Arrays.asList(
                            null,
                            TimestampData.fromEpochMillis(1, 1),
                            TimestampData.fromEpochMillis(1, 2)));
        }

        @Override
        protected List<TimestampData> getExpectedResults() {
            return Arrays.asList(
                    TimestampData.fromEpochMillis(0, 1), null, TimestampData.fromEpochMillis(1, 1));
        }

        @Override
        protected AggregateFunction<TimestampData, MinWithRetractAccumulator<TimestampData>>
                getAggregator() {
            return new MinWithRetractAggFunction<>(DataTypes.TIMESTAMP(9).getLogicalType());
        }
    }

    /** Test for {@link DateType}. */
    public static final class DateMinWithRetractAggFunctionTest
            extends MinWithRetractAggFunctionTestBase<Integer> {

        @Override
        protected List<List<Integer>> getInputValueSets() {
            return Arrays.asList(
                    Arrays.asList(0, 1000, 100, null, 10),
                    Arrays.asList(null, null, null, null, null),
                    Arrays.asList(null, 1));
        }

        @Override
        protected List<Integer> getExpectedResults() {
            return Arrays.asList(0, null, 1);
        }

        @Override
        protected AggregateFunction<Integer, MinWithRetractAccumulator<Integer>> getAggregator() {
            return new MinWithRetractAggFunction<>(DataTypes.DATE().getLogicalType());
        }
    }

    /** Test for {@link TimeType}. */
    public static final class TimeMinWithRetractAggFunctionTest
            extends MinWithRetractAggFunctionTestBase<Integer> {

        @Override
        protected List<List<Integer>> getInputValueSets() {
            return Arrays.asList(
                    Arrays.asList(0, 1000, 100, null, 10),
                    Arrays.asList(null, null, null, null, null),
                    Arrays.asList(null, 1));
        }

        @Override
        protected List<Integer> getExpectedResults() {
            return Arrays.asList(0, null, 1);
        }

        @Override
        protected AggregateFunction<Integer, MinWithRetractAccumulator<Integer>> getAggregator() {
            return new MinWithRetractAggFunction<>(DataTypes.TIME(0).getLogicalType());
        }
    }

    // --------------------------------------------------------------------------------------------
    // This section contain base classes that provide:
    //  - common inputs
    //  - declare the accumulator class
    //  - accessor for retract function
    //  for tests declared above.
    // --------------------------------------------------------------------------------------------

    /** Test base for {@link MinWithRetractAggFunction}. */
    public abstract static class MinWithRetractAggFunctionTestBase<T>
            extends AggFunctionTestBase<T, MinWithRetractAccumulator<T>> {

        @Override
        protected Class<?> getAccClass() {
            return MinWithRetractAccumulator.class;
        }

        @Override
        protected Method getAccumulateFunc() throws NoSuchMethodException {
            return getAggregator()
                    .getClass()
                    .getMethod(
                            UserDefinedFunctionHelper.AGGREGATE_ACCUMULATE,
                            getAccClass(),
                            Comparable.class);
        }

        @Override
        protected Method getRetractFunc() throws NoSuchMethodException {
            return getAggregator()
                    .getClass()
                    .getMethod(
                            UserDefinedFunctionHelper.AGGREGATE_RETRACT,
                            getAccClass(),
                            Comparable.class);
        }
    }

    /** Test base for {@link MinWithRetractAggFunction} and numeric types. */
    public abstract static class NumberMinWithRetractAggFunctionTestBase<T>
            extends MinWithRetractAggFunctionTestBase<T> {
        protected abstract T getMinValue();

        protected abstract T getMaxValue();

        protected abstract T getValue(String v);

        @Override
        protected List<List<T>> getInputValueSets() {
            return Arrays.asList(
                    Arrays.asList(
                            getValue("1"),
                            null,
                            getMaxValue(),
                            getValue("-99"),
                            getValue("3"),
                            getValue("56"),
                            getValue("0"),
                            getMinValue(),
                            getValue("-20"),
                            getValue("17"),
                            null),
                    Arrays.asList(null, null, null, null, null, null),
                    Arrays.asList(null, getValue("10")));
        }

        @Override
        protected List<T> getExpectedResults() {
            return Arrays.asList(getMinValue(), null, getValue("10"));
        }
    }
}
