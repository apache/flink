/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.dataformat.vector.heap;

import org.apache.flink.table.dataformat.Decimal;
import org.apache.flink.table.dataformat.vector.AbstractColumnVector;
import org.apache.flink.table.type.DecimalType;
import org.apache.flink.table.type.InternalType;
import org.apache.flink.table.type.InternalTypes;

import java.util.Arrays;

/**
 * Heap vector that nullable shared structure.
 */
public abstract class AbstractHeapVector extends AbstractColumnVector {

	/*
	 * If hasNulls is true, then this array contains true if the value
	 * is null, otherwise false. The array is always allocated, so a batch can be re-used
	 * later and nulls added.
	 */
	public boolean[] isNull;

	/**
	 * Reusable column for ids of dictionary.
	 */
	protected HeapIntVector dictionaryIds;

	public AbstractHeapVector(int len) {
		isNull = new boolean[len];
	}

	/**
	 * Resets the column to default state.
	 * - fills the isNull array with false.
	 * - sets noNulls to true.
	 */
	@Override
	public void reset() {
		if (!noNulls) {
			Arrays.fill(isNull, false);
		}
		noNulls = true;
	}

	@Override
	public boolean isNullAt(int i) {
		return !noNulls && isNull[i];
	}

	/**
	 * Reserve a integer column for ids of dictionary.
	 */
	public HeapIntVector reserveDictionaryIds(int capacity) {
		if (dictionaryIds == null) {
			dictionaryIds = new HeapIntVector(capacity);
		} else {
			dictionaryIds.reset();
		}
		return dictionaryIds;
	}

	/**
	 * Returns the underlying integer column for ids of dictionary.
	 */
	public HeapIntVector getDictionaryIds() {
		return dictionaryIds;
	}

	public static AbstractHeapVector[] allocateHeapVectors(InternalType[] fieldTypes, int maxRows) {
		AbstractHeapVector[] columns = new AbstractHeapVector[fieldTypes.length];
		for (int i = 0; i < fieldTypes.length; i++) {
			columns[i] = createHeapColumn(fieldTypes[i], maxRows);
		}
		return columns;
	}

	public static AbstractHeapVector createHeapColumn(InternalType fieldType, int maxRows) {
		if (fieldType.equals(InternalTypes.BOOLEAN)) {
			return new HeapBooleanVector(maxRows);
		} else if (fieldType.equals(InternalTypes.BYTE)) {
			return new HeapByteVector(maxRows);
		} else if (fieldType.equals(InternalTypes.DOUBLE)) {
			return new HeapDoubleVector(maxRows);
		} else if (fieldType.equals(InternalTypes.FLOAT)) {
			return new HeapFloatVector(maxRows);
		} else if (fieldType.equals(InternalTypes.INT) || (fieldType instanceof DecimalType &&
				Decimal.is32BitDecimal(((DecimalType) fieldType).precision()))) {
			return new HeapIntVector(maxRows);
		} else if (fieldType.equals(InternalTypes.LONG) || (fieldType instanceof DecimalType &&
				Decimal.is64BitDecimal(((DecimalType) fieldType).precision()))) {
			return new HeapLongVector(maxRows);
		} else if (fieldType.equals(InternalTypes.SHORT)) {
			return new HeapShortVector(maxRows);
		} else if (fieldType.equals(InternalTypes.STRING)) {
			return new HeapBytesVector(maxRows);
		} else if (fieldType.equals(InternalTypes.BINARY) || (fieldType instanceof DecimalType &&
				Decimal.isByteArrayDecimal(((DecimalType) fieldType).precision()))) {
			return new HeapBytesVector(maxRows);
		} else if (fieldType.equals(InternalTypes.DATE)) {
			return new HeapIntVector(maxRows);
		} else if (fieldType.equals(InternalTypes.TIME)) {
			return new HeapIntVector(maxRows);
		} else if (fieldType.equals(InternalTypes.TIMESTAMP)) {
			return new HeapLongVector(maxRows);
		} else {
			throw new UnsupportedOperationException(fieldType  + " is not supported now.");
		}
	}
}
