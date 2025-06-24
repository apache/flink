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

package org.apache.flink.table.runtime.functions.aggregate;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.DecimalDataUtils;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.runtime.functions.aggregate.hyperloglog.HllBuffer;
import org.apache.flink.table.runtime.functions.aggregate.hyperloglog.HyperLogLogPlusPlus;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarCharType;

import java.util.Collections;
import java.util.List;

import static org.apache.flink.table.runtime.functions.aggregate.hyperloglog.XXH64.DEFAULT_SEED;
import static org.apache.flink.table.runtime.functions.aggregate.hyperloglog.XXH64.hashInt;
import static org.apache.flink.table.runtime.functions.aggregate.hyperloglog.XXH64.hashLong;
import static org.apache.flink.table.runtime.functions.aggregate.hyperloglog.XXH64.hashUnsafeBytes;
import static org.apache.flink.table.types.utils.DataTypeUtils.toInternalDataType;

/** Built-in APPROX_COUNT_DISTINCT aggregate function for Batch sql. */
public class BatchApproxCountDistinctAggFunctions {

    /** Base function for APPROX_COUNT_DISTINCT aggregate. */
    public abstract static class ApproxCountDistinctAggFunction<T>
            extends BuiltInAggregateFunction<Long, HllBuffer> {

        /**
         * The maximum relative standard deviation allowed. the precision for '0.01' is 14, which is
         * enough, see
         * https://docs.google.com/document/d/1gyjfMHy43U9OWBXxfaeG-3MjGzejW1dlpyMwEYAAWEI/view?fullscreen#.
         */
        private static final Double RELATIVE_SD = 0.01;

        private transient HyperLogLogPlusPlus hll;

        private final transient DataType valueDataType;

        public ApproxCountDistinctAggFunction(LogicalType valueType) {
            this.valueDataType = toInternalDataType(valueType);
        }

        @Override
        public HllBuffer createAccumulator() {
            hll = new HyperLogLogPlusPlus(RELATIVE_SD);
            HllBuffer buffer = new HllBuffer();
            buffer.array = new long[hll.getNumWords()];
            resetAccumulator(buffer);
            return buffer;
        }

        public void accumulate(HllBuffer buffer, Object input) throws Exception {
            if (input != null) {
                hll.updateByHashcode(buffer, getHashcode((T) input));
            }
        }

        abstract long getHashcode(T t);

        public void merge(HllBuffer buffer, Iterable<HllBuffer> it) throws Exception {
            for (HllBuffer tmpBuffer : it) {
                hll.merge(buffer, tmpBuffer);
            }
        }

        public void resetAccumulator(HllBuffer buffer) {
            int word = 0;
            while (word < hll.getNumWords()) {
                buffer.array[word] = 0;
                word++;
            }
        }

        @Override
        public Long getValue(HllBuffer buffer) {
            return hll.query(buffer);
        }

        @Override
        public List<DataType> getArgumentDataTypes() {
            return Collections.singletonList(valueDataType);
        }

        @Override
        public DataType getAccumulatorDataType() {
            return DataTypes.STRUCTURED(
                    HllBuffer.class,
                    DataTypes.FIELD(
                            "array",
                            DataTypes.ARRAY(DataTypes.BIGINT().notNull())
                                    .notNull()
                                    .bridgedTo(long[].class)));
        }

        @Override
        public DataType getOutputDataType() {
            return DataTypes.BIGINT();
        }
    }

    /** Built-in byte APPROX_COUNT_DISTINCT aggregate function. */
    public static class ByteApproxCountDistinctAggFunction
            extends ApproxCountDistinctAggFunction<Byte> {

        public ByteApproxCountDistinctAggFunction() {
            super(new TinyIntType());
        }

        @Override
        long getHashcode(Byte value) {
            return hashInt(value, DEFAULT_SEED);
        }
    }

    /** Built-in decimal APPROX_COUNT_DISTINCT aggregate function. */
    public static class DecimalApproxCountDistinctAggFunction
            extends ApproxCountDistinctAggFunction<DecimalData> {

        private final int precision;

        public DecimalApproxCountDistinctAggFunction(DecimalType decimalType) {
            super(decimalType);
            this.precision = decimalType.getPrecision();
        }

        @Override
        long getHashcode(DecimalData d) {
            if (DecimalDataUtils.isByteArrayDecimal(precision)) {
                byte[] bytes = d.toBigDecimal().unscaledValue().toByteArray();
                MemorySegment segment = MemorySegmentFactory.wrap(bytes);
                return hashUnsafeBytes(segment, 0, segment.size(), DEFAULT_SEED);
            } else {
                return hashLong(d.toUnscaledLong(), DEFAULT_SEED);
            }
        }
    }

    /** Built-in double APPROX_COUNT_DISTINCT aggregate function. */
    public static class DoubleApproxCountDistinctAggFunction
            extends ApproxCountDistinctAggFunction<Double> {

        public DoubleApproxCountDistinctAggFunction() {
            super(new DoubleType());
        }

        @Override
        long getHashcode(Double value) {
            return hashLong(Double.doubleToLongBits(normalizeDouble(value)), DEFAULT_SEED);
        }

        private Double normalizeDouble(Double value) {
            if (value.isNaN()) {
                return Double.NaN;
            } else if (value == -0.0d) {
                return 0.0d;
            } else {
                return value;
            }
        }
    }

    /** Built-in float APPROX_COUNT_DISTINCT aggregate function. */
    public static class FloatApproxCountDistinctAggFunction
            extends ApproxCountDistinctAggFunction<Float> {

        public FloatApproxCountDistinctAggFunction() {
            super(new FloatType());
        }

        @Override
        long getHashcode(Float value) {
            return hashInt(Float.floatToIntBits(normalizeFloat(value)), DEFAULT_SEED);
        }

        private Float normalizeFloat(Float value) {
            if (value.isNaN()) {
                return Float.NaN;
            } else if (value == -0.0f) {
                return 0.0f;
            } else {
                return value;
            }
        }
    }

    /** Built-in int APPROX_COUNT_DISTINCT aggregate function. */
    public static class IntApproxCountDistinctAggFunction
            extends ApproxCountDistinctAggFunction<Integer> {

        public IntApproxCountDistinctAggFunction() {
            super(new IntType());
        }

        @Override
        long getHashcode(Integer value) {
            return hashInt(value, DEFAULT_SEED);
        }
    }

    /** Built-in long APPROX_COUNT_DISTINCT aggregate function. */
    public static class LongApproxCountDistinctAggFunction
            extends ApproxCountDistinctAggFunction<Long> {

        public LongApproxCountDistinctAggFunction() {
            super(new BigIntType());
        }

        @Override
        long getHashcode(Long value) {
            return hashLong(value, DEFAULT_SEED);
        }
    }

    /** Built-in Short APPROX_COUNT_DISTINCT aggregate function. */
    public static class ShortApproxCountDistinctAggFunction
            extends ApproxCountDistinctAggFunction<Short> {

        public ShortApproxCountDistinctAggFunction() {
            super(new SmallIntType());
        }

        @Override
        long getHashcode(Short value) {
            return hashInt(value, DEFAULT_SEED);
        }
    }

    /** Built-in Date APPROX_COUNT_DISTINCT aggregate function. */
    public static class DateApproxCountDistinctAggFunction
            extends ApproxCountDistinctAggFunction<Integer> {

        public DateApproxCountDistinctAggFunction() {
            super(new DateType());
        }

        @Override
        long getHashcode(Integer value) {
            return hashLong(value, DEFAULT_SEED);
        }
    }

    /** Built-in Time APPROX_COUNT_DISTINCT aggregate function. */
    public static class TimeApproxCountDistinctAggFunction
            extends ApproxCountDistinctAggFunction<Integer> {

        public TimeApproxCountDistinctAggFunction() {
            super(new TimeType());
        }

        @Override
        long getHashcode(Integer value) {
            return hashLong(value, DEFAULT_SEED);
        }
    }

    /** Built-in Timestamp APPROX_COUNT_DISTINCT aggregate function. */
    public static class TimestampApproxCountDistinctAggFunction
            extends ApproxCountDistinctAggFunction<TimestampData> {

        public TimestampApproxCountDistinctAggFunction(TimestampType type) {
            super(type);
        }

        @Override
        long getHashcode(TimestampData value) {
            return hashLong(value.getMillisecond(), DEFAULT_SEED);
        }
    }

    /** Built-in TimestampLtz APPROX_COUNT_DISTINCT aggregate function. */
    public static class TimestampLtzApproxCountDistinctAggFunction
            extends ApproxCountDistinctAggFunction<TimestampData> {

        public TimestampLtzApproxCountDistinctAggFunction(LocalZonedTimestampType type) {
            super(type);
        }

        @Override
        long getHashcode(TimestampData value) {
            return hashLong(value.getMillisecond(), DEFAULT_SEED);
        }
    }

    /** Built-in string APPROX_COUNT_DISTINCT aggregate function. */
    public static class StringApproxCountDistinctAggFunction
            extends ApproxCountDistinctAggFunction<BinaryStringData> {

        public StringApproxCountDistinctAggFunction() {
            super(new VarCharType());
        }

        @Override
        long getHashcode(BinaryStringData s) {
            MemorySegment[] segments = s.getSegments();
            if (segments.length == 1) {
                return hashUnsafeBytes(
                        segments[0], s.getOffset(), s.getSizeInBytes(), DEFAULT_SEED);
            } else {
                return hashUnsafeBytes(
                        MemorySegmentFactory.wrap(s.toBytes()),
                        0,
                        s.getSizeInBytes(),
                        DEFAULT_SEED);
            }
        }
    }
}
