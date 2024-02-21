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
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.DecimalDataUtils;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.runtime.functions.aggregate.ArrayAggFunction;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.testutils.serialization.types.ShortType;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.Nested;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

/** Test case for built-in ARRAY_AGG with retraction aggregate function. */
final class ArrayAggFunctionTest {

    /** Test for {@link TinyIntType}. */
    @Nested
    final class ByteArrayAggFunctionTest extends NumberArrayAggFunctionTestBase<Byte> {

        @Override
        protected Byte getValue(String v) {
            return Byte.valueOf(v);
        }

        @Override
        protected AggregateFunction<ArrayData, ArrayAggFunction.ArrayAggAccumulator<Byte>>
                getAggregator() {
            return new ArrayAggFunction<>(DataTypes.TINYINT().getLogicalType(), true);
        }
    }

    /** Test for {@link ShortType}. */
    @Nested
    final class ShortArrayAggTest extends NumberArrayAggFunctionTestBase<Short> {

        @Override
        protected Short getValue(String v) {
            return Short.valueOf(v);
        }

        @Override
        protected AggregateFunction<ArrayData, ArrayAggFunction.ArrayAggAccumulator<Short>>
                getAggregator() {
            return new ArrayAggFunction<>(DataTypes.SMALLINT().getLogicalType(), true);
        }
    }

    /** Test for {@link IntType}. */
    @Nested
    final class IntArrayAggTest extends NumberArrayAggFunctionTestBase<Integer> {

        @Override
        protected Integer getValue(String v) {
            return Integer.valueOf(v);
        }

        @Override
        protected AggregateFunction<ArrayData, ArrayAggFunction.ArrayAggAccumulator<Integer>>
                getAggregator() {
            return new ArrayAggFunction<>(DataTypes.INT().getLogicalType(), true);
        }
    }

    /** Test for {@link BigIntType}. */
    @Nested
    final class LongArrayAggTest extends NumberArrayAggFunctionTestBase<Long> {

        @Override
        protected Long getValue(String v) {
            return Long.valueOf(v);
        }

        @Override
        protected AggregateFunction<ArrayData, ArrayAggFunction.ArrayAggAccumulator<Long>>
                getAggregator() {
            return new ArrayAggFunction<>(DataTypes.BIGINT().getLogicalType(), true);
        }
    }

    /** Test for {@link FloatType}. */
    @Nested
    final class FloatArrayAggTest extends NumberArrayAggFunctionTestBase<Float> {

        @Override
        protected Float getValue(String v) {
            return Float.valueOf(v);
        }

        @Override
        protected AggregateFunction<ArrayData, ArrayAggFunction.ArrayAggAccumulator<Float>>
                getAggregator() {
            return new ArrayAggFunction<>(DataTypes.FLOAT().getLogicalType(), true);
        }
    }

    /** Test for {@link DoubleType}. */
    @Nested
    final class DoubleArrayAggTest extends NumberArrayAggFunctionTestBase<Double> {

        @Override
        protected Double getValue(String v) {
            return Double.valueOf(v);
        }

        @Override
        protected AggregateFunction<ArrayData, ArrayAggFunction.ArrayAggAccumulator<Double>>
                getAggregator() {
            return new ArrayAggFunction<>(DataTypes.DOUBLE().getLogicalType(), true);
        }
    }

    /** Test for {@link BooleanType}. */
    @Nested
    final class BooleanArrayAggTest extends ArrayAggFunctionTestBase<Boolean> {

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
        protected List<ArrayData> getExpectedResults() {
            return Arrays.asList(
                    new GenericArrayData(new Object[] {false, false, false}),
                    new GenericArrayData(new Object[] {true, true, true}),
                    new GenericArrayData(new Object[] {true, false, true, false, true}),
                    null,
                    new GenericArrayData(new Object[] {true}));
        }

        @Override
        protected AggregateFunction<ArrayData, ArrayAggFunction.ArrayAggAccumulator<Boolean>>
                getAggregator() {
            return new ArrayAggFunction<>(DataTypes.BOOLEAN().getLogicalType(), true);
        }
    }

    /** Test for {@link DecimalType}. */
    @Nested
    final class DecimalArrayAggFunctionTest extends NumberArrayAggFunctionTestBase<DecimalData> {

        private final int precision = 20;
        private final int scale = 6;

        @Override
        protected DecimalData getValue(String v) {
            return DecimalDataUtils.castFrom(v, precision, scale);
        }

        @Override
        protected List<List<DecimalData>> getInputValueSets() {
            return Arrays.asList(
                    Arrays.asList(
                            getValue("1"),
                            getValue("1000.000001"),
                            getValue("-1"),
                            getValue("-999.998999"),
                            null,
                            getValue("0"),
                            getValue("-999.999"),
                            null,
                            getValue("999.999")),
                    Arrays.asList(null, null, null, null, null),
                    Arrays.asList(null, getValue("0")));
        }

        @Override
        protected List<ArrayData> getExpectedResults() {
            return Arrays.asList(
                    new GenericArrayData(
                            new Object[] {
                                getValue("1"),
                                getValue("1000.000001"),
                                getValue("-1"),
                                getValue("-999.998999"),
                                getValue("0"),
                                getValue("-999.999"),
                                getValue("999.999")
                            }),
                    null,
                    new GenericArrayData(new Object[] {getValue("0")}));
        }

        @Override
        protected AggregateFunction<ArrayData, ArrayAggFunction.ArrayAggAccumulator<DecimalData>>
                getAggregator() {
            return new ArrayAggFunction<>(
                    DataTypes.DECIMAL(precision, scale).getLogicalType(), true);
        }
    }

    /** Test for {@link VarCharType}. */
    @Nested
    final class StringArrayAggFunctionTest extends ArrayAggFunctionTestBase<StringData> {

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
        protected List<ArrayData> getExpectedResults() {
            return Arrays.asList(
                    new GenericArrayData(
                            new Object[] {
                                StringData.fromString("abc"),
                                StringData.fromString("def"),
                                StringData.fromString("ghi"),
                                StringData.fromString("jkl"),
                                StringData.fromString("zzz")
                            }),
                    null,
                    new GenericArrayData(new Object[] {StringData.fromString("a")}),
                    new GenericArrayData(
                            new Object[] {StringData.fromString("x"), StringData.fromString("e")}));
        }

        @Override
        protected AggregateFunction<ArrayData, ArrayAggFunction.ArrayAggAccumulator<StringData>>
                getAggregator() {
            return new ArrayAggFunction<>(DataTypes.STRING().getLogicalType(), true);
        }
    }

    /** Test for {@link RowType}. */
    @Nested
    final class RowDArrayAggFunctionTest extends ArrayAggFunctionTestBase<RowData> {

        private RowData getValue(Integer f0, String f1) {
            GenericRowData rowData = new GenericRowData(RowKind.INSERT, 2);
            rowData.setField(0, f0);
            rowData.setField(1, f1 == null ? null : StringData.fromString(f1));
            return rowData;
        }

        @Override
        protected List<List<RowData>> getInputValueSets() {
            return Arrays.asList(
                    Arrays.asList(
                            getValue(0, "abc"),
                            getValue(1, "def"),
                            getValue(2, "ghi"),
                            null,
                            getValue(3, "jkl"),
                            null,
                            getValue(4, "zzz")),
                    Arrays.asList(null, null),
                    Arrays.asList(null, getValue(null, "a")),
                    Arrays.asList(getValue(5, null), null, getValue(null, "e")));
        }

        @Override
        protected List<ArrayData> getExpectedResults() {
            return Arrays.asList(
                    new GenericArrayData(
                            new Object[] {
                                getValue(0, "abc"),
                                getValue(1, "def"),
                                getValue(2, "ghi"),
                                getValue(3, "jkl"),
                                getValue(4, "zzz")
                            }),
                    null,
                    new GenericArrayData(new Object[] {getValue(null, "a")}),
                    new GenericArrayData(new Object[] {getValue(5, null), getValue(null, "e")}));
        }

        @Override
        protected AggregateFunction<ArrayData, ArrayAggFunction.ArrayAggAccumulator<RowData>>
                getAggregator() {
            return new ArrayAggFunction<>(
                    DataTypes.ROW(DataTypes.INT(), DataTypes.STRING()).getLogicalType(), true);
        }
    }

    /** Test for {@link ArrayType}. */
    @Nested
    final class ArrayArrayAggFunctionTest extends ArrayAggFunctionTestBase<ArrayData> {

        private ArrayData getValue(Integer... elements) {
            return new GenericArrayData(elements);
        }

        @Override
        protected List<List<ArrayData>> getInputValueSets() {
            return Arrays.asList(
                    Arrays.asList(
                            getValue(0, 1, 2),
                            getValue(1, null),
                            getValue(5, 3, 4, 5),
                            null,
                            getValue(6, null, 7)),
                    Arrays.asList(null, null));
        }

        @Override
        protected List<ArrayData> getExpectedResults() {
            return Arrays.asList(
                    new GenericArrayData(
                            new Object[] {
                                getValue(0, 1, 2),
                                getValue(1, null),
                                getValue(5, 3, 4, 5),
                                getValue(6, null, 7)
                            }),
                    null);
        }

        @Override
        protected AggregateFunction<ArrayData, ArrayAggFunction.ArrayAggAccumulator<ArrayData>>
                getAggregator() {
            return new ArrayAggFunction<>(DataTypes.ARRAY(DataTypes.INT()).getLogicalType(), true);
        }
    }

    /** Test base for {@link ArrayAggFunction}. */
    abstract static class ArrayAggFunctionTestBase<T>
            extends AggFunctionTestBase<T, ArrayData, ArrayAggFunction.ArrayAggAccumulator<T>> {

        @Override
        protected Class<?> getAccClass() {
            return ArrayAggFunction.ArrayAggAccumulator.class;
        }

        @Override
        protected Method getAccumulateFunc() throws NoSuchMethodException {
            return getAggregator().getClass().getMethod("accumulate", getAccClass(), Object.class);
        }

        @Override
        protected Method getRetractFunc() throws NoSuchMethodException {
            return getAggregator().getClass().getMethod("retract", getAccClass(), Object.class);
        }
    }

    /** Test base for {@link ArrayAggFunction} with number types. */
    abstract static class NumberArrayAggFunctionTestBase<T> extends ArrayAggFunctionTestBase<T> {

        protected abstract T getValue(String v);

        @Override
        protected List<List<T>> getInputValueSets() {
            return Arrays.asList(
                    Arrays.asList(getValue("1"), null, getValue("-99"), getValue("3"), null),
                    Arrays.asList(null, null, null, null),
                    Arrays.asList(null, getValue("10"), null, getValue("3")));
        }

        @Override
        protected List<ArrayData> getExpectedResults() {
            return Arrays.asList(
                    new GenericArrayData(
                            new Object[] {getValue("1"), getValue("-99"), getValue("3")}),
                    null,
                    new GenericArrayData(new Object[] {getValue("10"), getValue("3")}));
        }
    }
}
