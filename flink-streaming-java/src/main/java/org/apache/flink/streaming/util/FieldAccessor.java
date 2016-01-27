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

package org.apache.flink.streaming.util;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.runtime.PojoComparator;
import org.apache.flink.api.java.typeutils.TupleTypeInfoBase;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializerBase;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.List;

import scala.Product;


/**
 * These classes encapsulate the logic of accessing a field specified by the user as either an index
 * or a field expression string. TypeInformation can also be requested for the field.
 * The position index might specify a field of a Tuple, an array, or a simple type (only "0th field").
 */
public abstract class FieldAccessor<R, F> implements Serializable {

	private static final long serialVersionUID = 1L;

	TypeInformation fieldType;

	// Note: Returns the corresponding basic type for array of a primitive type (Integer for int[]).
	@SuppressWarnings("unchecked")
	public TypeInformation<F> getFieldType() {
		return fieldType;
	}


	public abstract F get(R record);

	// Note: This has to return the result, because the SimpleFieldAccessor might not be able to modify the
	// record in place. (for example, when R is simply Double) (Unfortunately there is no passing by reference in Java.)
	public abstract R set(R record, F fieldValue);



	@SuppressWarnings("unchecked")
	public static <R, F> FieldAccessor<R, F> create(int pos, TypeInformation<R> typeInfo, ExecutionConfig config) {
		if (typeInfo.isTupleType() && ((TupleTypeInfoBase)typeInfo).isCaseClass()) {
			return new ProductFieldAccessor<R, F>(pos, typeInfo, config);
		} else if (typeInfo.isTupleType()) {
			return new TupleFieldAccessor<R, F>(pos, typeInfo);
		} else if (typeInfo instanceof BasicArrayTypeInfo || typeInfo instanceof PrimitiveArrayTypeInfo) {
			return new ArrayFieldAccessor<R, F>(pos, typeInfo);
		} else {
			if(pos != 0) {
				throw new IndexOutOfBoundsException("Not 0th field selected for a simple type (non-tuple, non-array).");
			}
			return (FieldAccessor<R, F>) new SimpleFieldAccessor<R>(typeInfo);
		}
	}

	public static <R, F> FieldAccessor<R, F> create(String field, TypeInformation<R> typeInfo, ExecutionConfig config) {
		if (typeInfo.isTupleType() && ((TupleTypeInfoBase)typeInfo).isCaseClass()) {
			int pos = ((TupleTypeInfoBase)typeInfo).getFieldIndex(field);
			if(pos == -2) {
				throw new RuntimeException("Invalid field selected: " + field);
			}
			return new ProductFieldAccessor<R, F>(pos, typeInfo, config);
		} else if(typeInfo.isTupleType()) {
			return new TupleFieldAccessor<R, F>(((TupleTypeInfo)typeInfo).getFieldIndex(field), typeInfo);
		} else {
			return new PojoFieldAccessor<R, F>(field, typeInfo, config);
		}
	}



	public static class SimpleFieldAccessor<R> extends FieldAccessor<R, R> {

		private static final long serialVersionUID = 1L;

		SimpleFieldAccessor(TypeInformation<R> typeInfo) {
			this.fieldType = typeInfo;
		}

		@Override
		public R get(R record) {
			return record;
		}

		@Override
		public R set(R record, R fieldValue) {
			return fieldValue;
		}
	}

	public static class ArrayFieldAccessor<R, F> extends FieldAccessor<R, F> {

		private static final long serialVersionUID = 1L;

		int pos;

		ArrayFieldAccessor(int pos, TypeInformation typeInfo) {
			this.pos = pos;
			this.fieldType = BasicTypeInfo.getInfoFor(typeInfo.getTypeClass().getComponentType());
		}

		@SuppressWarnings("unchecked")
		@Override
		public F get(R record) {
			return (F) Array.get(record, pos);
		}

		@Override
		public R set(R record, F fieldValue) {
			Array.set(record, pos, fieldValue);
			return record;
		}
	}

	public static class TupleFieldAccessor<R, F> extends FieldAccessor<R, F> {

		private static final long serialVersionUID = 1L;

		int pos;

		TupleFieldAccessor(int pos, TypeInformation<R> typeInfo) {
			this.pos = pos;
			this.fieldType = ((TupleTypeInfo)typeInfo).getTypeAt(pos);
		}

		@SuppressWarnings("unchecked")
		@Override
		public F get(R record) {
			Tuple tuple = (Tuple) record;
			return (F)tuple.getField(pos);
		}

		@Override
		public R set(R record, F fieldValue) {
			Tuple tuple = (Tuple) record;
			tuple.setField(fieldValue, pos);
			return record;
		}
	}

	public static class PojoFieldAccessor<R, F> extends FieldAccessor<R, F> {

		private static final long serialVersionUID = 1L;

		PojoComparator comparator;

		PojoFieldAccessor(String field, TypeInformation<R> type, ExecutionConfig config) {
			if (!(type instanceof CompositeType<?>)) {
				throw new IllegalArgumentException(
						"Key expressions are only supported on POJO types and Tuples. "
								+ "A type is considered a POJO if all its fields are public, or have both getters and setters defined");
			}

			@SuppressWarnings("unchecked")
			CompositeType<R> cType = (CompositeType<R>) type;

			List<CompositeType.FlatFieldDescriptor> fieldDescriptors = cType.getFlatFields(field);

			int logicalKeyPosition = fieldDescriptors.get(0).getPosition();
			this.fieldType = fieldDescriptors.get(0).getType();
			Class<?> keyClass = fieldType.getTypeClass();

			if (cType instanceof PojoTypeInfo) {
				comparator = (PojoComparator<R>) cType.createComparator(
						new int[] { logicalKeyPosition }, new boolean[] { false }, 0, config);
			} else {
				throw new IllegalArgumentException(
						"Key expressions are only supported on POJO types. "
								+ "A type is considered a POJO if all its fields are public, or have both getters and setters defined");
			}
		}

		@SuppressWarnings("unchecked")
		@Override
		public F get(R record) {
			return (F) comparator.accessField(comparator.getKeyFields()[0], record);
		}

		@Override
		public R set(R record, F fieldValue) {
			try {
				comparator.getKeyFields()[0].set(record, fieldValue);
			} catch (IllegalAccessException e) {
				throw new RuntimeException("Could not modify the specified field.", e);
			}
			return record;
		}
	}

	public static class ProductFieldAccessor<R, F> extends FieldAccessor<R, F> {

		private static final long serialVersionUID = 1L;

		int pos;
		TupleSerializerBase<R> serializer;
		Object[] fields;
		int length;

		ProductFieldAccessor(int pos, TypeInformation<R> typeInfo, ExecutionConfig config) {
			this.pos = pos;
			this.fieldType = ((TupleTypeInfoBase<R>)typeInfo).getTypeAt(pos);
			this.serializer = (TupleSerializerBase<R>)typeInfo.createSerializer(config);
			this.length = this.serializer.getArity();
			this.fields = new Object[this.length];
		}

		@SuppressWarnings("unchecked")
		@Override
		public F get(R record) {
			return (F)((Product)record).productElement(pos);
		}

		@Override
		public R set(R record, F fieldValue) {
			Product prod = (Product)record;
			for (int i = 0; i < length; i++) {
				fields[i] = prod.productElement(i);
			}
			fields[pos] = fieldValue;
			return serializer.createInstance(fields);
		}
	}
}
