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

import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.common.typeutils.CompositeType.FlatFieldDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.runtime.PojoComparator;

public abstract class ComparableAggregator<T> extends AggregationFunction<T> {

	private static final long serialVersionUID = 1L;

	public Comparator comparator;
	public boolean byAggregate;
	public boolean first;

	public ComparableAggregator(int pos, AggregationType aggregationType, boolean first) {
		super(pos);
		this.comparator = Comparator.getForAggregation(aggregationType);
		this.byAggregate = (aggregationType == AggregationType.MAXBY)
				|| (aggregationType == AggregationType.MINBY);
		this.first = first;
	}

	public static <R> AggregationFunction<R> getAggregator(int positionToAggregate,
			TypeInformation<R> typeInfo, AggregationType aggregationType) {
		return getAggregator(positionToAggregate, typeInfo, aggregationType, false);
	}

	public static <R> AggregationFunction<R> getAggregator(int positionToAggregate,
			TypeInformation<R> typeInfo, AggregationType aggregationType, boolean first) {

		if (typeInfo.isTupleType()) {
			return new TupleComparableAggregator<R>(positionToAggregate, aggregationType, first);
		} else if (typeInfo instanceof BasicArrayTypeInfo
				|| typeInfo instanceof PrimitiveArrayTypeInfo) {
			return new ArrayComparableAggregator<R>(positionToAggregate, aggregationType, first);
		} else {
			return new SimpleComparableAggregator<R>(aggregationType);
		}
	}

	public static <R> AggregationFunction<R> getAggregator(String field,
			TypeInformation<R> typeInfo, AggregationType aggregationType, boolean first) {

		return new PojoComparableAggregator<R>(field, typeInfo, aggregationType, first);
	}

	private static class TupleComparableAggregator<T> extends ComparableAggregator<T> {

		private static final long serialVersionUID = 1L;

		public TupleComparableAggregator(int pos, AggregationType aggregationType, boolean first) {
			super(pos, aggregationType, first);
		}

		@SuppressWarnings("unchecked")
		@Override
		public T reduce(T value1, T value2) throws Exception {
			Tuple tuple1 = (Tuple) value1;
			Tuple tuple2 = (Tuple) value2;

			Comparable<Object> o1 = tuple1.getField(position);
			Object o2 = tuple2.getField(position);

			int c = comparator.isExtremal(o1, o2);

			if (byAggregate) {
				if (c == 1) {
					return (T) tuple1;
				}
				if (first) {
					if (c == 0) {
						return (T) tuple1;
					}
				}

				return (T) tuple2;

			} else {
				if (c == 1) {
					tuple2.setField(o1, position);
				}
				return (T) tuple2;
			}

		}
	}

	private static class ArrayComparableAggregator<T> extends ComparableAggregator<T> {

		private static final long serialVersionUID = 1L;

		public ArrayComparableAggregator(int pos, AggregationType aggregationType, boolean first) {
			super(pos, aggregationType, first);
		}

		@SuppressWarnings("unchecked")
		@Override
		public T reduce(T array1, T array2) throws Exception {

			Object v1 = Array.get(array1, position);
			Object v2 = Array.get(array2, position);

			int c = comparator.isExtremal((Comparable<Object>) v1, v2);

			if (byAggregate) {
				if (c == 1) {
					return array1;
				}
				if (first) {
					if (c == 0) {
						return array1;
					}
				}

				return array2;
			} else {
				if (c == 1) {
					Array.set(array2, position, v1);
				}

				return array2;
			}
		}

	}

	private static class SimpleComparableAggregator<T> extends ComparableAggregator<T> {

		private static final long serialVersionUID = 1L;

		public SimpleComparableAggregator(AggregationType aggregationType) {
			super(0, aggregationType, false);
		}

		@SuppressWarnings("unchecked")
		@Override
		public T reduce(T value1, T value2) throws Exception {

			if (comparator.isExtremal((Comparable<Object>) value1, value2) == 1) {
				return value1;
			} else {
				return value2;
			}
		}

	}

	private static class PojoComparableAggregator<T> extends ComparableAggregator<T> {

		private static final long serialVersionUID = 1L;
		PojoComparator<T> pojoComparator;

		public PojoComparableAggregator(String field, TypeInformation<?> typeInfo,
				AggregationType aggregationType, boolean first) {
			super(0, aggregationType, first);
			if (!(typeInfo instanceof CompositeType<?>)) {
				throw new IllegalArgumentException(
						"Key expressions are only supported on POJO types and Tuples. "
								+ "A type is considered a POJO if all its fields are public, or have both getters and setters defined");
			}

			@SuppressWarnings("unchecked")
			CompositeType<T> cType = (CompositeType<T>) typeInfo;

			List<FlatFieldDescriptor> fieldDescriptors = cType.getFlatFields(field);
			int logicalKeyPosition = fieldDescriptors.get(0).getPosition();

			if (cType instanceof PojoTypeInfo) {
				pojoComparator = (PojoComparator<T>) cType.createComparator(
						new int[] { logicalKeyPosition }, new boolean[] { false }, 0, getRuntimeContext().getExecutionConfig());
			} else {
				throw new IllegalArgumentException(
						"Key expressions are only supported on POJO types. "
								+ "A type is considered a POJO if all its fields are public, or have both getters and setters defined");
			}
		}

		@Override
		public T reduce(T value1, T value2) throws Exception {

			Field[] keyFields = pojoComparator.getKeyFields();
			Object field1 = pojoComparator.accessField(keyFields[0], value1);
			Object field2 = pojoComparator.accessField(keyFields[0], value2);

			@SuppressWarnings("unchecked")
			int c = comparator.isExtremal((Comparable<Object>) field1, field2);

			if (byAggregate) {
				if (c == 1) {
					return value1;
				}
				if (first) {
					if (c == 0) {
						return value1;
					}
				}

				return value2;
			} else {
				if (c == 1) {
					keyFields[0].set(value2, field1);
				} 
				
				return value2;
			}
		}

	}

}
