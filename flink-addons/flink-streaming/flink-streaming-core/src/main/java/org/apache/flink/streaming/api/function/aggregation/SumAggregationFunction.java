/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.function.aggregation;

import org.apache.flink.api.java.tuple.Tuple;

public abstract class SumAggregationFunction<T> extends AggregationFunction<T> {

	private static final long serialVersionUID = 1L;

	public SumAggregationFunction(int pos) {
		super(pos);
	}

	@SuppressWarnings("unchecked")
	@Override
	public T reduce(T value1, T value2) throws Exception {
		if (value1 instanceof Tuple) {
			Tuple tuple1 = (Tuple) value1;
			Tuple tuple2 = (Tuple) value2;

			returnTuple = tuple2;
			returnTuple.setField(add(tuple1.getField(position), tuple2.getField(position)),
					position);

			return (T) returnTuple;
		} else {
			return (T) add(value1, value2);
		}
	}

	protected abstract Object add(Object value1, Object value2);

	@SuppressWarnings("rawtypes")
	public static <T> SumAggregationFunction getSumFunction(int pos, Class<T> type) {

		if (type == Integer.class) {
			return new IntSum<T>(pos);
		} else if (type == Long.class) {
			return new LongSum<T>(pos);
		} else if (type == Short.class) {
			return new ShortSum<T>(pos);
		} else if (type == Double.class) {
			return new DoubleSum<T>(pos);
		} else if (type == Float.class) {
			return new FloatSum<T>(pos);
		} else if (type == Byte.class) {
			return new ByteSum<T>(pos);
		} else {
			throw new RuntimeException("DataStream cannot be summed because the class "
					+ type.getSimpleName() + " does not support the + operator.");
		}

	}

	private static class IntSum<T> extends SumAggregationFunction<T> {
		private static final long serialVersionUID = 1L;

		public IntSum(int pos) {
			super(pos);
		}

		@Override
		protected Object add(Object value1, Object value2) {
			return (Integer) value1 + (Integer) value2;
		}
	}

	private static class LongSum<T> extends SumAggregationFunction<T> {
		private static final long serialVersionUID = 1L;

		public LongSum(int pos) {
			super(pos);
		}

		@Override
		protected Object add(Object value1, Object value2) {
			return (Long) value1 + (Long) value2;
		}
	}

	private static class DoubleSum<T> extends SumAggregationFunction<T> {

		private static final long serialVersionUID = 1L;

		public DoubleSum(int pos) {
			super(pos);
		}

		@Override
		protected Object add(Object value1, Object value2) {
			return (Double) value1 + (Double) value2;
		}
	}

	private static class ShortSum<T> extends SumAggregationFunction<T> {
		private static final long serialVersionUID = 1L;

		public ShortSum(int pos) {
			super(pos);
		}

		@Override
		protected Object add(Object value1, Object value2) {
			return (Short) value1 + (Short) value2;
		}
	}

	private static class FloatSum<T> extends SumAggregationFunction<T> {
		private static final long serialVersionUID = 1L;

		public FloatSum(int pos) {
			super(pos);
		}

		@Override
		protected Object add(Object value1, Object value2) {
			return (Float) value1 + (Float) value2;
		}
	}

	private static class ByteSum<T> extends SumAggregationFunction<T> {
		private static final long serialVersionUID = 1L;

		public ByteSum(int pos) {
			super(pos);
		}

		@Override
		protected Object add(Object value1, Object value2) {
			return (Byte) value1 + (Byte) value2;
		}
	}

}
