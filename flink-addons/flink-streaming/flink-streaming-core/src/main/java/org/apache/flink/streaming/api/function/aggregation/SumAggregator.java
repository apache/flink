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

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.List;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.common.typeutils.CompositeType.FlatFieldDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.runtime.PojoComparator;

public abstract class SumAggregator {

	public static <T> ReduceFunction<T> getSumFunction(int pos, Class<?> clazz,
			TypeInformation<T> typeInfo) {

		if (typeInfo.isTupleType()) {
			return new TupleSumAggregator<T>(pos, SumFunction.getForClass(clazz));
		} else if (typeInfo instanceof BasicArrayTypeInfo
				|| typeInfo instanceof PrimitiveArrayTypeInfo) {
			return new ArraySumAggregator<T>(pos, SumFunction.getForClass(clazz));
		} else {
			return new SimpleSumAggregator<T>(SumFunction.getForClass(clazz));
		}

	}

	public static <T> ReduceFunction<T> getSumFunction(String field, TypeInformation<T> typeInfo) {

		return new PojoSumAggregator<T>(field, typeInfo);
	}

	private static class TupleSumAggregator<T> extends AggregationFunction<T> {

		private static final long serialVersionUID = 1L;

		SumFunction adder;

		public TupleSumAggregator(int pos, SumFunction adder) {
			super(pos);
			this.adder = adder;
		}

		@SuppressWarnings("unchecked")
		@Override
		public T reduce(T value1, T value2) throws Exception {

			Tuple tuple1 = (Tuple) value1;
			Tuple tuple2 = (Tuple) value2;

			tuple2.setField(adder.add(tuple1.getField(position), tuple2.getField(position)),
					position);

			return (T) tuple2;
		}

	}

	private static class ArraySumAggregator<T> extends AggregationFunction<T> {

		private static final long serialVersionUID = 1L;

		SumFunction adder;

		public ArraySumAggregator(int pos, SumFunction adder) {
			super(pos);
			this.adder = adder;
		}

		@Override
		public T reduce(T value1, T value2) throws Exception {

			Object v1 = Array.get(value1, position);
			Object v2 = Array.get(value2, position);
			Array.set(value2, position, adder.add(v1, v2));
			return value2;
		}

	}

	private static class SimpleSumAggregator<T> extends AggregationFunction<T> {

		private static final long serialVersionUID = 1L;

		SumFunction adder;

		public SimpleSumAggregator(SumFunction adder) {
			super(0);
			this.adder = adder;
		}

		@SuppressWarnings("unchecked")
		@Override
		public T reduce(T value1, T value2) throws Exception {

			return (T) adder.add(value1, value2);
		}

	}

	private static class PojoSumAggregator<T> extends AggregationFunction<T> {

		private static final long serialVersionUID = 1L;
		SumFunction adder;
		PojoComparator<T> comparator;

		public PojoSumAggregator(String field, TypeInformation<?> type) {
			super(0);
			if (!(type instanceof CompositeType<?>)) {
				throw new IllegalArgumentException(
						"Key expressions are only supported on POJO types and Tuples. "
								+ "A type is considered a POJO if all its fields are public, or have both getters and setters defined");
			}

			@SuppressWarnings("unchecked")
			CompositeType<T> cType = (CompositeType<T>) type;

			List<FlatFieldDescriptor> fieldDescriptors = cType.getFlatFields(field);

			int logicalKeyPosition = fieldDescriptors.get(0).getPosition();
			Class<?> keyClass = fieldDescriptors.get(0).getType().getTypeClass();

			adder = SumFunction.getForClass(keyClass);

			if (cType instanceof PojoTypeInfo) {
				comparator = (PojoComparator<T>) cType.createComparator(
						new int[] { logicalKeyPosition }, new boolean[] { false }, 0);
			} else {
				throw new IllegalArgumentException(
						"Key expressions are only supported on POJO types. "
								+ "A type is considered a POJO if all its fields are public, or have both getters and setters defined");
			}
		}

		@Override
		public T reduce(T value1, T value2) throws Exception {

			Field[] keyFields = comparator.getKeyFields();
			Object field1 = comparator.accessField(keyFields[0], value1);
			Object field2 = comparator.accessField(keyFields[0], value2);

			keyFields[0].set(value2, adder.add(field1, field2));

			return value2;
		}

	}

}
