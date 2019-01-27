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

package org.apache.flink.table.runtime.functions.aggfunctions;

import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.table.api.functions.AggregateFunction;
import org.apache.flink.table.api.types.DataType;
import org.apache.flink.table.api.types.DataTypes;
import org.apache.flink.table.api.types.DecimalType;
import org.apache.flink.table.api.types.GenericType;
import org.apache.flink.table.api.types.TypeInfoWrappedDataType;
import org.apache.flink.table.runtime.functions.aggfunctions.hyperloglog.HyperLogLogPlusPlus;
import org.apache.flink.table.typeutils.BinaryStringTypeInfo;

/**
 * Built-in approximate count distinct aggregate function.
 */
public class ApproximateCountDistinct {

	/**
	 * Base function for approximate count distinct aggregate.
	 */
	public abstract static class ApproximateCountDistinctAggFunction extends AggregateFunction<Long, HllBuffer> {

		private static final Double RELATIVE_SD = 0.01;
		private HyperLogLogPlusPlus hyperLogLogPlusPlus;

		public abstract DataType getValueTypeInfo();

		@Override
		public HllBuffer createAccumulator() {
			hyperLogLogPlusPlus = new HyperLogLogPlusPlus(RELATIVE_SD);
			HllBuffer buffer = new HllBuffer();
			buffer.array = new long[hyperLogLogPlusPlus.numWords()];
			resetAccumulator(buffer);
			return buffer;
		}

		public void accumulate(HllBuffer buffer, Object input) throws Exception {
			if (input != null) {
				hyperLogLogPlusPlus.update(buffer, input);
			}
		}

		public void merge(HllBuffer buffer, Iterable<HllBuffer> it) throws Exception {
			for (HllBuffer tmpBuffer : it) {
				hyperLogLogPlusPlus.merge(buffer, tmpBuffer);
			}
		}

		public void resetAccumulator(HllBuffer buffer) {
			int word = 0;
			while (word < hyperLogLogPlusPlus.numWords()) {
				buffer.array[word] = 0;
				word++;
			}
		}

		@Override
		public Long getValue(HllBuffer buffer) {
			return hyperLogLogPlusPlus.query(buffer);
		}

		@Override
		public DataType[] getUserDefinedInputTypes(Class[] signature) {
			if (signature.length == 1) {
				return new DataType[]{getValueTypeInfo()};
			} else if (signature.length == 0) {
				return new DataType[0];
			} else {
				throw new UnsupportedOperationException();
			}
		}

		@Override
		public DataType getAccumulatorType() {
			return new GenericType<>(new GenericTypeInfo<>(HllBuffer.class));
		}
	}

	/**
	 * Buffer used by HLL++.
	 */
	public static class HllBuffer {
		public long[] array;
	}

	/**
	 * Built-in byte approximate count distinct aggregate function.
	 */
	public static class ByteApproximateCountDistinctAggFunction extends ApproximateCountDistinctAggFunction {

		@Override
		public DataType getValueTypeInfo() {
			return DataTypes.BYTE;
		}
	}

	/**
	 * Built-in decimal approximate count distinct aggregate function.
	 */
	public static class DecimalApproximateCountDistinctAggFunction extends ApproximateCountDistinctAggFunction {

		public final DecimalType decimalType;

		public DecimalApproximateCountDistinctAggFunction(DecimalType decimalType) {
			this.decimalType = decimalType;
		}

		@Override
		public DataType getValueTypeInfo() {
			return decimalType;
		}
	}

	/**
	 * Built-in double approximate count distinct aggregate function.
	 */
	public static class DoubleApproximateCountDistinctAggFunction extends ApproximateCountDistinctAggFunction {

		@Override
		public DataType getValueTypeInfo() {
			return DataTypes.DOUBLE;
		}
	}

	/**
	 * Built-in float approximate count distinct aggregate function.
	 */
	public static class FloatApproximateCountDistinctAggFunction extends ApproximateCountDistinctAggFunction {

		@Override
		public DataType getValueTypeInfo() {
			return DataTypes.FLOAT;
		}
	}

	/**
	 * Built-in int approximate count distinct aggregate function.
	 */
	public static class IntApproximateCountDistinctAggFunction extends ApproximateCountDistinctAggFunction {

		@Override
		public DataType getValueTypeInfo() {
			return DataTypes.INT;
		}
	}

	/**
	 * Built-in long approximate count distinct aggregate function.
	 */
	public static class LongApproximateCountDistinctAggFunction extends ApproximateCountDistinctAggFunction {

		@Override
		public DataType getValueTypeInfo() {
			return DataTypes.LONG;
		}
	}

	/**
	 * Built-in short approximate count distinct aggregate function.
	 */
	public static class ShortApproximateCountDistinctAggFunction extends ApproximateCountDistinctAggFunction {

		@Override
		public DataType getValueTypeInfo() {
			return DataTypes.SHORT;
		}
	}

	/**
	 * Built-in boolean approximate count distinct aggregate function.
	 */
	public static class BooleanApproximateCountDistinctAggFunction extends ApproximateCountDistinctAggFunction {

		@Override
		public DataType getValueTypeInfo() {
			return DataTypes.BOOLEAN;
		}
	}

	/**
	 * Built-in date approximate count distinct aggregate function.
	 */
	public static class DateApproximateCountDistinctAggFunction extends ApproximateCountDistinctAggFunction {

		@Override
		public DataType getValueTypeInfo() {
			return DataTypes.DATE;
		}
	}

	/**
	 * Built-in time approximate count distinct aggregate function.
	 */
	public static class TimeApproximateCountDistinctAggFunction extends ApproximateCountDistinctAggFunction {

		@Override
		public DataType getValueTypeInfo() {
			return DataTypes.TIME;
		}
	}

	/**
	 * Built-in timestamp approximate count distinct aggregate function.
	 */
	public static class TimestampApproximateCountDistinctAggFunction extends ApproximateCountDistinctAggFunction {

		@Override
		public DataType getValueTypeInfo() {
			return DataTypes.TIMESTAMP;
		}
	}

	/**
	 * Built-in string approximate count distinct aggregate function.
	 */
	public static class StringApproximateCountDistinctAggFunction extends ApproximateCountDistinctAggFunction {

		@Override
		public DataType getValueTypeInfo() {
			return new TypeInfoWrappedDataType(BinaryStringTypeInfo.INSTANCE);
		}
	}
}
