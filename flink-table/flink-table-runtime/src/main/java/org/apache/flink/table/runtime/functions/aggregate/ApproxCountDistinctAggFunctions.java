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

import org.apache.flink.annotation.Internal;
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

/**
 * Built-in APPROX_COUNT_DISTINCT aggregate functions supporting both Batch and Streaming modes.
 *
 * <p>This implementation uses the HyperLogLog++ algorithm for approximate distinct counting, which
 * provides a relative standard error of approximately 1%.
 *
 * <p>Key features:
 *
 * <ul>
 *   <li>Supports both batch and streaming SQL
 *   <li>Supports Window TVF (TUMBLE, HOP, CUMULATE) through the merge() method
 *   <li>Uses Flink's internal HyperLogLog++ implementation
 *   <li>Space complexity: O(m) where m is related to precision (approximately 16KB for 1% error)
 * </ul>
 *
 * <p>Comparison with COUNT(DISTINCT x):
 *
 * <ul>
 *   <li>COUNT(DISTINCT x) provides exact results but requires O(n) memory
 *   <li>APPROX_COUNT_DISTINCT provides approximate results with bounded error (~1%) using O(1)
 *       memory
 *   <li>Use APPROX_COUNT_DISTINCT when exact counts are not required and memory is a concern
 * </ul>
 *
 * <p>Note: This function does NOT support retraction operations in non-windowed streaming
 * aggregations because HyperLogLog is a probabilistic data structure that cannot remove elements
 * once added.
 */
@Internal
public class ApproxCountDistinctAggFunctions {

    /** Base function for APPROX_COUNT_DISTINCT aggregate. */
    public abstract static class ApproxCountDistinctAggFunction<T>
            extends BuiltInAggregateFunction<Long, HllBuffer> {

        private static final long serialVersionUID = 1L;

        /**
         * The maximum relative standard deviation allowed. The precision for '0.01' is 14, which is
         * enough. See
         * https://docs.google.com/document/d/1gyjfMHy43U9OWBXxfaeG-3MjGzejW1dlpyMwEYAAWEI/view?fullscreen#
         */
        private static final Double RELATIVE_SD = 0.01;

        private transient HyperLogLogPlusPlus hll;

        private final transient DataType valueDataType;

        public ApproxCountDistinctAggFunction(LogicalType valueType) {
            this.valueDataType = toInternalDataType(valueType);
        }

        private HyperLogLogPlusPlus getOrCreateHll() {
            if (hll == null) {
                hll = new HyperLogLogPlusPlus(RELATIVE_SD);
            }
            return hll;
        }

        @Override
        public HllBuffer createAccumulator() {
            HyperLogLogPlusPlus hllInstance = getOrCreateHll();
            HllBuffer buffer = new HllBuffer();
            buffer.array = new long[hllInstance.getNumWords()];
            resetAccumulator(buffer);
            return buffer;
        }

        /**
         * Accumulates a value into the HyperLogLog buffer.
         *
         * @param buffer the accumulator buffer
         * @param input the input value to accumulate
         */
        public void accumulate(HllBuffer buffer, Object input) {
            if (input != null) {
                @SuppressWarnings("unchecked")
                T typedInput = (T) input;
                getOrCreateHll().updateByHashcode(buffer, getHashcode(typedInput));
            }
        }

        /**
         * Computes the hash code for the given value.
         *
         * @param value the input value
         * @return the hash code
         */
        abstract long getHashcode(T value);

        /**
         * Merges multiple HyperLogLog buffers into one. This method is essential for supporting
         * Window TVF aggregations (TUMBLE, HOP, CUMULATE).
         *
         * @param buffer the target accumulator buffer
         * @param it iterable of source buffers to merge
         */
        public void merge(HllBuffer buffer, Iterable<HllBuffer> it) {
            HyperLogLogPlusPlus hllInstance = getOrCreateHll();
            for (HllBuffer tmpBuffer : it) {
                if (tmpBuffer != null && tmpBuffer.array != null) {
                    hllInstance.merge(buffer, tmpBuffer);
                }
            }
        }

        /**
         * Resets the accumulator to its initial state.
         *
         * @param buffer the accumulator buffer to reset
         */
        public void resetAccumulator(HllBuffer buffer) {
            HyperLogLogPlusPlus hllInstance = getOrCreateHll();
            int numWords = hllInstance.getNumWords();
            for (int word = 0; word < numWords; word++) {
                buffer.array[word] = 0;
            }
        }

        @Override
        public Long getValue(HllBuffer buffer) {
            return getOrCreateHll().query(buffer);
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

    /** Built-in TINYINT (byte) APPROX_COUNT_DISTINCT aggregate function. */
    public static class ByteApproxCountDistinctAggFunction
            extends ApproxCountDistinctAggFunction<Byte> {

        private static final long serialVersionUID = 1L;

        public ByteApproxCountDistinctAggFunction() {
            super(new TinyIntType());
        }

        @Override
        long getHashcode(Byte value) {
            return hashInt(value, DEFAULT_SEED);
        }
    }

    /** Built-in SMALLINT (short) APPROX_COUNT_DISTINCT aggregate function. */
    public static class ShortApproxCountDistinctAggFunction
            extends ApproxCountDistinctAggFunction<Short> {

        private static final long serialVersionUID = 1L;

        public ShortApproxCountDistinctAggFunction() {
            super(new SmallIntType());
        }

        @Override
        long getHashcode(Short value) {
            return hashInt(value, DEFAULT_SEED);
        }
    }

    /** Built-in INT (integer) APPROX_COUNT_DISTINCT aggregate function. */
    public static class IntApproxCountDistinctAggFunction
            extends ApproxCountDistinctAggFunction<Integer> {

        private static final long serialVersionUID = 1L;

        public IntApproxCountDistinctAggFunction() {
            super(new IntType());
        }

        @Override
        long getHashcode(Integer value) {
            return hashInt(value, DEFAULT_SEED);
        }
    }

    /** Built-in BIGINT (long) APPROX_COUNT_DISTINCT aggregate function. */
    public static class LongApproxCountDistinctAggFunction
            extends ApproxCountDistinctAggFunction<Long> {

        private static final long serialVersionUID = 1L;

        public LongApproxCountDistinctAggFunction() {
            super(new BigIntType());
        }

        @Override
        long getHashcode(Long value) {
            return hashLong(value, DEFAULT_SEED);
        }
    }

    /** Built-in FLOAT APPROX_COUNT_DISTINCT aggregate function. */
    public static class FloatApproxCountDistinctAggFunction
            extends ApproxCountDistinctAggFunction<Float> {

        private static final long serialVersionUID = 1L;

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

    /** Built-in DOUBLE APPROX_COUNT_DISTINCT aggregate function. */
    public static class DoubleApproxCountDistinctAggFunction
            extends ApproxCountDistinctAggFunction<Double> {

        private static final long serialVersionUID = 1L;

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

    /** Built-in DECIMAL APPROX_COUNT_DISTINCT aggregate function. */
    public static class DecimalApproxCountDistinctAggFunction
            extends ApproxCountDistinctAggFunction<DecimalData> {

        private static final long serialVersionUID = 1L;

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

    /** Built-in DATE APPROX_COUNT_DISTINCT aggregate function. */
    public static class DateApproxCountDistinctAggFunction
            extends ApproxCountDistinctAggFunction<Integer> {

        private static final long serialVersionUID = 1L;

        public DateApproxCountDistinctAggFunction() {
            super(new DateType());
        }

        @Override
        long getHashcode(Integer value) {
            return hashLong(value, DEFAULT_SEED);
        }
    }

    /** Built-in TIME APPROX_COUNT_DISTINCT aggregate function. */
    public static class TimeApproxCountDistinctAggFunction
            extends ApproxCountDistinctAggFunction<Integer> {

        private static final long serialVersionUID = 1L;

        public TimeApproxCountDistinctAggFunction() {
            super(new TimeType());
        }

        @Override
        long getHashcode(Integer value) {
            return hashLong(value, DEFAULT_SEED);
        }
    }

    /** Built-in TIMESTAMP APPROX_COUNT_DISTINCT aggregate function. */
    public static class TimestampApproxCountDistinctAggFunction
            extends ApproxCountDistinctAggFunction<TimestampData> {

        private static final long serialVersionUID = 1L;

        public TimestampApproxCountDistinctAggFunction(TimestampType type) {
            super(type);
        }

        @Override
        long getHashcode(TimestampData value) {
            // Combine milliseconds and nanoseconds for high-precision timestamps
            // This ensures TIMESTAMP(6) and higher precision values are distinguished correctly
            long combinedValue = value.getMillisecond() * 1_000_000L + value.getNanoOfMillisecond();
            return hashLong(combinedValue, DEFAULT_SEED);
        }
    }

    /** Built-in TIMESTAMP WITH LOCAL TIME ZONE APPROX_COUNT_DISTINCT aggregate function. */
    public static class TimestampLtzApproxCountDistinctAggFunction
            extends ApproxCountDistinctAggFunction<TimestampData> {

        private static final long serialVersionUID = 1L;

        public TimestampLtzApproxCountDistinctAggFunction(LocalZonedTimestampType type) {
            super(type);
        }

        @Override
        long getHashcode(TimestampData value) {
            // Combine milliseconds and nanoseconds for high-precision timestamps
            // This ensures TIMESTAMP_LTZ(6) and higher precision values are distinguished correctly
            long combinedValue = value.getMillisecond() * 1_000_000L + value.getNanoOfMillisecond();
            return hashLong(combinedValue, DEFAULT_SEED);
        }
    }

    /** Built-in VARCHAR (string) APPROX_COUNT_DISTINCT aggregate function. */
    public static class StringApproxCountDistinctAggFunction
            extends ApproxCountDistinctAggFunction<BinaryStringData> {

        private static final long serialVersionUID = 1L;

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
