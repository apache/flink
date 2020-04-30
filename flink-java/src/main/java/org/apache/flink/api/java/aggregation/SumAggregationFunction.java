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

package org.apache.flink.api.java.aggregation;

import org.apache.flink.annotation.Internal;
import org.apache.flink.types.ByteValue;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.FloatValue;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.ShortValue;

/**
 * Definitions of sum functions for different numerical types.
 * @param <T> type of elements being summed
 */
@Internal
public abstract class SumAggregationFunction<T> extends AggregationFunction<T> {

	private static final long serialVersionUID = 1L;

	@Override
	public String toString() {
		return "SUM";
	}

	// --------------------------------------------------------------------------------------------

	private static final class ByteSumAgg extends SumAggregationFunction<Byte> {
		private static final long serialVersionUID = 1L;

		private long agg;

		@Override
		public void initializeAggregate() {
			agg = 0;
		}

		@Override
		public void aggregate(Byte value) {
			agg += value;
		}

		@Override
		public Byte getAggregate() {
			return (byte) agg;
		}
	}

	private static final class ByteValueSumAgg extends SumAggregationFunction<ByteValue> {
		private static final long serialVersionUID = 1L;

		private long agg;

		@Override
		public void initializeAggregate() {
			agg = 0;
		}

		@Override
		public void aggregate(ByteValue value) {
			agg += value.getValue();
		}

		@Override
		public ByteValue getAggregate() {
			return new ByteValue((byte) agg);
		}
	}

	private static final class ShortSumAgg extends SumAggregationFunction<Short> {
		private static final long serialVersionUID = 1L;

		private long agg;

		@Override
		public void initializeAggregate() {
			agg = 0;
		}

		@Override
		public void aggregate(Short value) {
			agg += value;
		}

		@Override
		public Short getAggregate() {
			return (short) agg;
		}
	}

	private static final class ShortValueSumAgg extends SumAggregationFunction<ShortValue> {
		private static final long serialVersionUID = 1L;

		private long agg;

		@Override
		public void initializeAggregate() {
			agg = 0;
		}

		@Override
		public void aggregate(ShortValue value) {
			agg += value.getValue();
		}

		@Override
		public ShortValue getAggregate() {
			return new ShortValue((short) agg);
		}
	}

	private static final class IntSumAgg extends SumAggregationFunction<Integer> {
		private static final long serialVersionUID = 1L;

		private long agg;

		@Override
		public void initializeAggregate() {
			agg = 0;
		}

		@Override
		public void aggregate(Integer value) {
			agg += value;
		}

		@Override
		public Integer getAggregate() {
			return (int) agg;
		}
	}

	private static final class IntValueSumAgg extends SumAggregationFunction<IntValue> {
		private static final long serialVersionUID = 1L;

		private long agg;

		@Override
		public void initializeAggregate() {
			agg = 0;
		}

		@Override
		public void aggregate(IntValue value) {
			agg += value.getValue();
		}

		@Override
		public IntValue getAggregate() {
			return new IntValue((int) agg);
		}
	}

	private static final class LongSumAgg extends SumAggregationFunction<Long> {
		private static final long serialVersionUID = 1L;

		private long agg;

		@Override
		public void initializeAggregate() {
			agg = 0L;
		}

		@Override
		public void aggregate(Long value) {
			agg += value;
		}

		@Override
		public Long getAggregate() {
			return agg;
		}
	}

	private static final class LongValueSumAgg extends SumAggregationFunction<LongValue> {
		private static final long serialVersionUID = 1L;

		private long agg;

		@Override
		public void initializeAggregate() {
			agg = 0L;
		}

		@Override
		public void aggregate(LongValue value) {
			agg += value.getValue();
		}

		@Override
		public LongValue getAggregate() {
			return new LongValue(agg);
		}
	}

	private static final class FloatSumAgg extends SumAggregationFunction<Float> {
		private static final long serialVersionUID = 1L;

		private double agg;

		@Override
		public void initializeAggregate() {
			agg = 0.0f;
		}

		@Override
		public void aggregate(Float value) {
			agg += value;
		}

		@Override
		public Float getAggregate() {
			return (float) agg;
		}
	}

	private static final class FloatValueSumAgg extends SumAggregationFunction<FloatValue> {
		private static final long serialVersionUID = 1L;

		private double agg;

		@Override
		public void initializeAggregate() {
			agg = 0.0f;
		}

		@Override
		public void aggregate(FloatValue value) {
			agg += value.getValue();
		}

		@Override
		public FloatValue getAggregate() {
			return new FloatValue((float) agg);
		}
	}

	private static final class DoubleSumAgg extends SumAggregationFunction<Double> {
		private static final long serialVersionUID = 1L;

		private double agg;

		@Override
		public void initializeAggregate() {
			agg = 0.0;
		}

		@Override
		public void aggregate(Double value) {
			agg += value;
		}

		@Override
		public Double getAggregate() {
			return agg;
		}
	}

	private static final class DoubleValueSumAgg extends SumAggregationFunction<DoubleValue> {
		private static final long serialVersionUID = 1L;

		private double agg;

		@Override
		public void initializeAggregate() {
			agg = 0.0;
		}

		@Override
		public void aggregate(DoubleValue value) {
			agg += value.getValue();
		}

		@Override
		public DoubleValue getAggregate() {
			return new DoubleValue(agg);
		}
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Factory for {@link SumAggregationFunction}.
	 */
	public static final class SumAggregationFunctionFactory implements AggregationFunctionFactory {
		private static final long serialVersionUID = 1L;

		@SuppressWarnings("unchecked")
		@Override
		public <T> AggregationFunction<T> createAggregationFunction(Class<T> type) {
			if (type == Long.class) {
				return (AggregationFunction<T>) new LongSumAgg();
			}
			else if (type == LongValue.class) {
				return (AggregationFunction<T>) new LongValueSumAgg();
			}
			else if (type == Integer.class) {
				return (AggregationFunction<T>) new IntSumAgg();
			}
			else if (type == IntValue.class) {
				return (AggregationFunction<T>) new IntValueSumAgg();
			}
			else if (type == Double.class) {
				return (AggregationFunction<T>) new DoubleSumAgg();
			}
			else if (type == DoubleValue.class) {
				return (AggregationFunction<T>) new DoubleValueSumAgg();
			}
			else if (type == Float.class) {
				return (AggregationFunction<T>) new FloatSumAgg();
			}
			else if (type == FloatValue.class) {
				return (AggregationFunction<T>) new FloatValueSumAgg();
			}
			else if (type == Byte.class) {
				return (AggregationFunction<T>) new ByteSumAgg();
			}
			else if (type == ByteValue.class) {
				return (AggregationFunction<T>) new ByteValueSumAgg();
			}
			else if (type == Short.class) {
				return (AggregationFunction<T>) new ShortSumAgg();
			}
			else if (type == ShortValue.class) {
				return (AggregationFunction<T>) new ShortValueSumAgg();
			}
			else {
				throw new UnsupportedAggregationTypeException("The type " + type.getName() +
					" is currently not supported for built-in sum aggregations.");
			}
		}
	}
}
