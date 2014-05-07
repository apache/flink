/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.api.java.aggregation;


public abstract class AvgAggregationFunction<T> extends AggregationFunction<T> {
	private static final long serialVersionUID = 1L;


	@Override
	public String toString() {
		return "AVG";
	}
	
	// --------------------------------------------------------------------------------------------
	
	public static final class ByteAvgAgg extends AvgAggregationFunction<Byte> {
		private static final long serialVersionUID = 1L;
		
		private long sum;
		private long count;

		@Override
		public void initializeAggregate() {
			sum = 0;
			count = 0;
		}

		@Override
		public void aggregate(Byte value) {
			sum += value.byteValue();
			count++;
		}

		@Override
		public Byte getAggregate() {
			return (byte) (sum / count);
		}
	}
	
	public static final class ShortAvgAgg extends AvgAggregationFunction<Short> {
		private static final long serialVersionUID = 1L;
		
		private long sum;
		private long count;

		@Override
		public void initializeAggregate() {
			sum = 0;
			count = 0;
		}

		@Override
		public void aggregate(Short value) {
			sum += value.shortValue();
			count++;
		}

		@Override
		public Short getAggregate() {
			return (short) (sum / count);
		}
	}
	
	public static final class IntAvgAgg extends AvgAggregationFunction<Integer> {
		private static final long serialVersionUID = 1L;
		
		private long sum;
		private long count;

		@Override
		public void initializeAggregate() {
			sum = 0;
			count = 0;
		}

		@Override
		public void aggregate(Integer value) {
			sum += value.intValue();
			count++;
		}

		@Override
		public Integer getAggregate() {
			return (int) (sum / count);
		}
	}
	
	public static final class LongAvgAgg extends AvgAggregationFunction<Long> {
		private static final long serialVersionUID = 1L;
		
		private long sum;
		private long count;

		@Override
		public void initializeAggregate() {
			sum = 0;
			count = 0;
		}

		@Override
		public void aggregate(Long value) {
			sum += value.longValue();
			count++;
		}

		@Override
		public Long getAggregate() {
			return sum / count;
		}
	}
	
	public static final class FloatAvgAgg extends AvgAggregationFunction<Float> {
		private static final long serialVersionUID = 1L;
		
		private float sum;
		private long count;

		@Override
		public void initializeAggregate() {
			sum = 0.0f;
			count = 0;
		}

		@Override
		public void aggregate(Float value) {
			sum += value.floatValue();
			count++;
		}

		@Override
		public Float getAggregate() {
			return sum / count;
		}
	}
	
	public static final class DoubleAvgAgg extends AvgAggregationFunction<Double> {
		private static final long serialVersionUID = 1L;
		
		private double sum;
		private long count;

		@Override
		public void initializeAggregate() {
			sum = 0.0;
			count = 0;
		}

		@Override
		public void aggregate(Double value) {
			sum += value.doubleValue();
			count++;
		}

		@Override
		public Double getAggregate() {
			return sum / count;
		}
	}

	
	// --------------------------------------------------------------------------------------------
	
	public static final class AvgAggregationFunctionFactory implements AggregationFunctionFactory {
		private static final long serialVersionUID = 1L;

		@SuppressWarnings("unchecked")
		@Override
		public <T> AggregationFunction<T> createAggregationFunction(Class<T> type) {
			if (type == Long.class) {
				return (AggregationFunction<T>) new LongAvgAgg();
			}
			else if (type == Integer.class) {
				return (AggregationFunction<T>) new IntAvgAgg();
			}
			else if (type == Double.class) {
				return (AggregationFunction<T>) new DoubleAvgAgg();
			}
			else if (type == Float.class) {
				return (AggregationFunction<T>) new FloatAvgAgg();
			}
			else if (type == Byte.class) {
				return (AggregationFunction<T>) new ByteAvgAgg();
			}
			else if (type == Short.class) {
				return (AggregationFunction<T>) new ShortAvgAgg();
			}
			else {
				throw new UnsupportedAggregationTypeException("The type " + type.getName() + 
					" has currently not supported for built-in average aggregations.");
			}
		}
	}
}
