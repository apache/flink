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
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;

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
	protected boolean[] isNull;

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

	public void setNullAt(int i) {
		isNull[i] = true;
		noNulls = false;
	}

	@Override
	public boolean isNullAt(int i) {
		return !noNulls && isNull[i];
	}

	@Override
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

	public static AbstractHeapVector[] allocateHeapVectors(LogicalType[] fieldTypes, int maxRows) {
		AbstractHeapVector[] columns = new AbstractHeapVector[fieldTypes.length];
		for (int i = 0; i < fieldTypes.length; i++) {
			columns[i] = createHeapColumn(fieldTypes[i], maxRows);
		}
		return columns;
	}

	public static AbstractHeapVector createHeapColumn(LogicalType fieldType, int maxRows) {
		switch (fieldType.getTypeRoot()) {
			case BOOLEAN:
				return new HeapBooleanVector(maxRows);
			case TINYINT:
				return new HeapByteVector(maxRows);
			case DOUBLE:
				return new HeapDoubleVector(maxRows);
			case FLOAT:
				return new HeapFloatVector(maxRows);
			case INTEGER:
			case DATE:
			case TIME_WITHOUT_TIME_ZONE:
				return new HeapIntVector(maxRows);
			case BIGINT:
			case TIMESTAMP_WITHOUT_TIME_ZONE:
			case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
				return new HeapLongVector(maxRows);
			case DECIMAL:
				DecimalType decimalType = (DecimalType) fieldType;
				if (Decimal.is32BitDecimal(decimalType.getPrecision())) {
					return new HeapIntVector(maxRows);
				} else if (Decimal.is64BitDecimal(decimalType.getPrecision())) {
					return new HeapLongVector(maxRows);
				} else {
					return new HeapBytesVector(maxRows);
				}
			case SMALLINT:
				return new HeapShortVector(maxRows);
			case CHAR:
			case VARCHAR:
			case BINARY:
			case VARBINARY:
				return new HeapBytesVector(maxRows);
			default:
				throw new UnsupportedOperationException(fieldType  + " is not supported now.");
		}
	}
}
