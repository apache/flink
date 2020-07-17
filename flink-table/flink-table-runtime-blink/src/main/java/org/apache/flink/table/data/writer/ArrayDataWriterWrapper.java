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

package org.apache.flink.table.data.writer;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.binary.BinaryArrayData;
import org.apache.flink.table.types.logical.LogicalType;

/**
 * A utility class for writing array data in array and map data serializer implementations.
 */
public class ArrayDataWriterWrapper {

	private final LogicalType type;

	private final BinaryArrayData reuseArray;
	private BinaryArrayWriter reuseWriter;
	private final BinaryWriter.NullSetter reuseNullSetter;
	private final BinaryWriter.ValueSetter reuseValueSetter;

	public ArrayDataWriterWrapper(LogicalType type) {
		this(type, new BinaryArrayData());
	}

	public ArrayDataWriterWrapper(LogicalType type, BinaryArrayData reuseArray) {
		this.type = type;

		this.reuseArray = reuseArray;
		reuseNullSetter = BinaryWriter.createNullSetter(type);
		reuseValueSetter = BinaryWriter.createValueSetter(type);
	}

	public BinaryArrayData write(ArrayData array) {
		return write(
			array.size(),
			array::isNullAt,
			idx -> ArrayData.get(array, idx, type));
	}

	public <E extends Throwable> BinaryArrayData write(
			int numElements,
			ThrowingFunction<Integer, Boolean, E> nullChecker,
			ThrowingFunction<Integer, Object, E> elementProvider) throws E {
		if (reuseWriter == null || reuseWriter.getNumElements() != numElements) {
			reuseWriter = new BinaryArrayWriter(
				reuseArray, numElements, BinaryArrayData.calculateFixLengthPartSize(type));
		} else {
			reuseWriter.reset();
		}

		for (int i = 0; i < numElements; i++) {
			if (nullChecker.apply(i)) {
				reuseNullSetter.setNull(reuseWriter, i);
			} else {
				reuseValueSetter.setValue(reuseWriter, i, elementProvider.apply(i));
			}
		}
		reuseWriter.complete();

		return reuseArray;
	}

	public interface ThrowingFunction<T, R, E extends Throwable> {
		R apply(T t) throws E;
	}
}
