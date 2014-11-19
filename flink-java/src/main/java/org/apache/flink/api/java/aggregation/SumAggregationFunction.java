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


public abstract class SumAggregationFunction<T> extends AggregationFunction<T> {
	
	private static final long serialVersionUID = 1L;

	@Override
	public String toString() {
		return "SUM";
	}
	
	// --------------------------------------------------------------------------------------------
	
	public static final class ByteSumAgg extends SumAggregationFunction<Byte> {
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
	
	public static final class ShortSumAgg extends SumAggregationFunction<Short> {
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
	
	public static final class IntSumAgg extends SumAggregationFunction<Integer> {
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
	
	public static final class LongSumAgg extends SumAggregationFunction<Long> {
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
	
	public static final class FloatSumAgg extends SumAggregationFunction<Float> {
		private static final long serialVersionUID = 1L;
		
		private float agg;

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
			return agg;
		}
	}
	
	public static final class DoubleSumAgg extends SumAggregationFunction<Double> {
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
	
	// --------------------------------------------------------------------------------------------
	
	public static final class SumAggregationFunctionFactory implements AggregationFunctionFactory {
		private static final long serialVersionUID = 1L;
		
		@SuppressWarnings("unchecked")
		@Override
		public <T> AggregationFunction<T> createAggregationFunction(Class<T> type) {
			if (type == Long.class) {
				return (AggregationFunction<T>) new LongSumAgg();
			}
			else if (type == Integer.class) {
				return (AggregationFunction<T>) new IntSumAgg();
			}
			else if (type == Double.class) {
				return (AggregationFunction<T>) new DoubleSumAgg();
			}
			else if (type == Float.class) {
				return (AggregationFunction<T>) new FloatSumAgg();
			}
			else if (type == Byte.class) {
				return (AggregationFunction<T>) new ByteSumAgg();
			}
			else if (type == Short.class) {
				return (AggregationFunction<T>) new ShortSumAgg();
			}
			else {
				throw new UnsupportedAggregationTypeException("The type " + type.getName() + 
					" has currently not supported for built-in sum aggregations.");
			}
		}
	}
}
