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

package org.apache.flink.table.data.conversion;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.binary.BinaryArrayData;
import org.apache.flink.table.data.writer.BinaryArrayWriter;
import org.apache.flink.table.data.writer.BinaryWriter;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.commons.lang3.ArrayUtils;

import java.io.Serializable;
import java.lang.reflect.Array;

import static org.apache.commons.lang3.ClassUtils.primitiveToWrapper;

/**
 * Converter for {@link ArrayType} of nested primitive or object arrays external types.
 */
@Internal
@SuppressWarnings("unchecked")
public class ArrayObjectArrayConverter<E> implements DataStructureConverter<ArrayData, E[]> {

	private static final long serialVersionUID = 1L;

	private final Class<E> elementClass;

	private final int elementSize;

	private final BinaryArrayWriter.NullSetter  writerNullSetter;

	private final BinaryWriter.ValueSetter writerValueSetter;

	private final GenericToJavaArrayConverter<E> genericToJavaArrayConverter;

	private transient BinaryArrayData reuseArray;

	private transient BinaryArrayWriter reuseWriter;

	final boolean hasInternalElements;

	final ArrayData.ElementGetter elementGetter;

	final DataStructureConverter<Object, E> elementConverter;

	private ArrayObjectArrayConverter(
			Class<E> elementClass,
			int elementSize,
			BinaryArrayWriter.NullSetter writerNullSetter,
			BinaryWriter.ValueSetter writerValueSetter,
			GenericToJavaArrayConverter<E> genericToJavaArrayConverter,
			ArrayData.ElementGetter elementGetter,
			DataStructureConverter<Object, E> elementConverter) {
		this.elementClass = elementClass;
		this.elementSize = elementSize;
		this.writerNullSetter = writerNullSetter;
		this.writerValueSetter = writerValueSetter;
		this.genericToJavaArrayConverter = genericToJavaArrayConverter;
		this.hasInternalElements = elementConverter.isIdentityConversion();
		this.elementGetter = elementGetter;
		this.elementConverter = elementConverter;
	}

	@Override
	public void open(ClassLoader classLoader) {
		reuseArray = new BinaryArrayData();
		reuseWriter = new BinaryArrayWriter(reuseArray, 0, elementSize);
		elementConverter.open(classLoader);
	}

	@Override
	public ArrayData toInternal(E[] external) {
		return hasInternalElements ? new GenericArrayData(external) : toBinaryArrayData(external);
	}

	@Override
	public E[] toExternal(ArrayData internal) {
		if (hasInternalElements && internal instanceof GenericArrayData) {
			final GenericArrayData genericArray = (GenericArrayData) internal;
			if (genericArray.isPrimitiveArray()) {
				return genericToJavaArrayConverter.convert((GenericArrayData) internal);
			}
			return (E[]) genericArray.toObjectArray();
		}
		return toJavaArray(internal);
	}

	// --------------------------------------------------------------------------------------------
	// Runtime helper methods
	// --------------------------------------------------------------------------------------------

	private ArrayData toBinaryArrayData(E[] external) {
		final int length = external.length;
		allocateWriter(length);
		for (int pos = 0; pos < length; pos++) {
			writeElement(pos, external[pos]);
		}
		return completeWriter().copy();
	}

	private E[] toJavaArray(ArrayData internal) {
		final int size = internal.size();
		final E[] values = (E[]) Array.newInstance(elementClass, size);
		for (int pos = 0; pos < size; pos++) {
			final Object value = elementGetter.getElementOrNull(internal, pos);
			values[pos] = elementConverter.toExternalOrNull(value);
		}
		return values;
	}

	interface GenericToJavaArrayConverter<E> extends Serializable {
		E[] convert(GenericArrayData internal);
	}

	// --------------------------------------------------------------------------------------------
	// Shared code
	// --------------------------------------------------------------------------------------------

	void allocateWriter(int length) {
		if (reuseWriter.getNumElements() != length) {
			reuseWriter = new BinaryArrayWriter(reuseArray, length, elementSize);
		} else {
			reuseWriter.reset();
		}
	}

	void writeElement(int pos, E element) {
		if (element == null) {
			writerNullSetter.setNull(reuseWriter, pos);
		} else {
			writerValueSetter.setValue(reuseWriter, pos, elementConverter.toInternal(element));
		}
	}

	BinaryArrayData completeWriter() {
		reuseWriter.complete();
		return reuseArray;
	}

	// --------------------------------------------------------------------------------------------
	// Factory method
	// --------------------------------------------------------------------------------------------

	public static ArrayObjectArrayConverter<?> create(DataType dataType) {
		return createForElement(dataType.getChildren().get(0));
	}

	public static <E> ArrayObjectArrayConverter<E> createForElement(DataType elementDataType) {
		final LogicalType elementType = elementDataType.getLogicalType();
		return new ArrayObjectArrayConverter<>(
			(Class<E>) primitiveToWrapper(elementDataType.getConversionClass()),
			BinaryArrayData.calculateFixLengthPartSize(elementType),
			BinaryArrayWriter.createNullSetter(elementType),
			BinaryWriter.createValueSetter(elementType),
			createGenericToJavaArrayConverter(elementType),
			ArrayData.createElementGetter(elementType),
			(DataStructureConverter<Object, E>) DataStructureConverters.getConverter(elementDataType)
		);
	}

	@SuppressWarnings("unchecked")
	private static <E> GenericToJavaArrayConverter<E> createGenericToJavaArrayConverter(LogicalType elementType) {
		switch (elementType.getTypeRoot()) {
			case BOOLEAN:
				return internal -> (E[]) ArrayUtils.toObject(internal.toBooleanArray());
			case TINYINT:
				return internal -> (E[]) ArrayUtils.toObject(internal.toByteArray());
			case SMALLINT:
				return internal -> (E[]) ArrayUtils.toObject(internal.toShortArray());
			case INTEGER:
				return internal -> (E[]) ArrayUtils.toObject(internal.toIntArray());
			case BIGINT:
				return internal -> (E[]) ArrayUtils.toObject(internal.toLongArray());
			case FLOAT:
				return internal -> (E[]) ArrayUtils.toObject(internal.toFloatArray());
			case DOUBLE:
				return internal -> (E[]) ArrayUtils.toObject(internal.toDoubleArray());
			case DISTINCT_TYPE:
				return createGenericToJavaArrayConverter(((DistinctType) elementType).getSourceType());
			default:
				return internal -> {
					throw new IllegalStateException();
				};
		}
	}
}
