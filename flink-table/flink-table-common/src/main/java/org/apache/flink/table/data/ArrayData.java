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

package org.apache.flink.table.data;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;

import javax.annotation.Nullable;

import java.io.Serializable;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getFieldCount;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getPrecision;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getScale;

/**
 * Base interface of an internal data structure representing data of {@link ArrayType}.
 *
 * <p>Note: All elements of this data structure must be internal data structures and must be of the
 * same type. See {@link RowData} for more information about internal data structures.
 *
 * <p>Use {@link GenericArrayData} to construct instances of this interface from regular Java arrays.
 */
@PublicEvolving
public interface ArrayData {

	/**
	 * Returns the number of elements in this array.
	 */
	int size();

	// ------------------------------------------------------------------------------------------
	// Read-only accessor methods
	// ------------------------------------------------------------------------------------------

	/**
	 * Returns true if the element is null at the given position.
	 */
	boolean isNullAt(int pos);

	/**
	 * Returns the boolean value at the given position.
	 */
	boolean getBoolean(int pos);

	/**
	 * Returns the byte value at the given position.
	 */
	byte getByte(int pos);

	/**
	 * Returns the short value at the given position.
	 */
	short getShort(int pos);

	/**
	 * Returns the integer value at the given position.
	 */
	int getInt(int pos);

	/**
	 * Returns the long value at the given position.
	 */
	long getLong(int pos);

	/**
	 * Returns the float value at the given position.
	 */
	float getFloat(int pos);

	/**
	 * Returns the double value at the given position.
	 */
	double getDouble(int pos);

	/**
	 * Returns the string value at the given position.
	 */
	StringData getString(int pos);

	/**
	 * Returns the decimal value at the given position.
	 *
	 * <p>The precision and scale are required to determine whether the decimal value was stored in a
	 * compact representation (see {@link DecimalData}).
	 */
	DecimalData getDecimal(int pos, int precision, int scale);

	/**
	 * Returns the timestamp value at the given position.
	 *
	 * <p>The precision is required to determine whether the timestamp value was stored in a compact
	 * representation (see {@link TimestampData}).
	 */
	TimestampData getTimestamp(int pos, int precision);

	/**
	 * Returns the raw value at the given position.
	 */
	<T> RawValueData<T> getRawValue(int pos);

	/**
	 * Returns the binary value at the given position.
	 */
	byte[] getBinary(int pos);

	/**
	 * Returns the array value at the given position.
	 */
	ArrayData getArray(int pos);

	/**
	 * Returns the map value at the given position.
	 */
	MapData getMap(int pos);

	/**
	 * Returns the row value at the given position.
	 *
	 * <p>The number of fields is required to correctly extract the row.
	 */
	RowData getRow(int pos, int numFields);

	// ------------------------------------------------------------------------------------------
	// Conversion Utilities
	// ------------------------------------------------------------------------------------------

	boolean[] toBooleanArray();

	byte[] toByteArray();

	short[] toShortArray();

	int[] toIntArray();

	long[] toLongArray();

	float[] toFloatArray();

	double[] toDoubleArray();

	// ------------------------------------------------------------------------------------------
	// Access Utilities
	// ------------------------------------------------------------------------------------------

	/**
	 * Returns the element object in the internal array data structure at the given position.
	 *
	 * @param array the internal array data
	 * @param pos position of the element to return
	 * @param elementType the element type of the array
	 * @return the element object at the specified position in this array data
	 * @deprecated Use {@link #createElementGetter(LogicalType)} for avoiding logical types during runtime.
	 */
	@Deprecated
	static Object get(ArrayData array, int pos, LogicalType elementType) {
		if (array.isNullAt(pos)) {
			return null;
		}
		switch (elementType.getTypeRoot()) {
			case BOOLEAN:
				return array.getBoolean(pos);
			case TINYINT:
				return array.getByte(pos);
			case SMALLINT:
				return array.getShort(pos);
			case INTEGER:
			case DATE:
			case TIME_WITHOUT_TIME_ZONE:
			case INTERVAL_YEAR_MONTH:
				return array.getInt(pos);
			case BIGINT:
			case INTERVAL_DAY_TIME:
				return array.getLong(pos);
			case TIMESTAMP_WITHOUT_TIME_ZONE:
				TimestampType timestampType = (TimestampType) elementType;
				return array.getTimestamp(pos, timestampType.getPrecision());
			case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
				LocalZonedTimestampType lzTs = (LocalZonedTimestampType) elementType;
				return array.getTimestamp(pos, lzTs.getPrecision());
			case FLOAT:
				return array.getFloat(pos);
			case DOUBLE:
				return array.getDouble(pos);
			case CHAR:
			case VARCHAR:
				return array.getString(pos);
			case DECIMAL:
				DecimalType decimalType = (DecimalType) elementType;
				return array.getDecimal(pos, decimalType.getPrecision(), decimalType.getScale());
			case ARRAY:
				return array.getArray(pos);
			case MAP:
			case MULTISET:
				return array.getMap(pos);
			case ROW:
				return array.getRow(pos, ((RowType) elementType).getFieldCount());
			case STRUCTURED_TYPE:
				// not the most efficient code but ok for a deprecated method
				return array.getRow(pos, getFieldCount(elementType));
			case BINARY:
			case VARBINARY:
				return array.getBinary(pos);
			case RAW:
				return array.getRawValue(pos);
			default:
				throw new UnsupportedOperationException("Unsupported type: " + elementType);
		}
	}

	/**
	 * Creates an accessor for getting elements in an internal array data structure at the
	 * given position.
	 *
	 * @param elementType the element type of the array
	 */
	static ElementGetter createElementGetter(LogicalType elementType) {
		final ElementGetter elementGetter;
		// ordered by type root definition
		switch (elementType.getTypeRoot()) {
			case CHAR:
			case VARCHAR:
				elementGetter = ArrayData::getString;
				break;
			case BOOLEAN:
				elementGetter = ArrayData::getBoolean;
				break;
			case BINARY:
			case VARBINARY:
				elementGetter = ArrayData::getBinary;
				break;
			case DECIMAL:
				final int decimalPrecision = getPrecision(elementType);
				final int decimalScale = getScale(elementType);
				elementGetter = (array, pos) -> array.getDecimal(pos, decimalPrecision, decimalScale);
				break;
			case TINYINT:
				elementGetter = ArrayData::getByte;
				break;
			case SMALLINT:
				elementGetter = ArrayData::getShort;
				break;
			case INTEGER:
			case DATE:
			case TIME_WITHOUT_TIME_ZONE:
			case INTERVAL_YEAR_MONTH:
				elementGetter = ArrayData::getInt;
				break;
			case BIGINT:
			case INTERVAL_DAY_TIME:
				elementGetter = ArrayData::getLong;
				break;
			case FLOAT:
				elementGetter = ArrayData::getFloat;
				break;
			case DOUBLE:
				elementGetter = ArrayData::getDouble;
				break;
			case TIMESTAMP_WITHOUT_TIME_ZONE:
			case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
				final int timestampPrecision = getPrecision(elementType);
				elementGetter = (array, pos) -> array.getTimestamp(pos, timestampPrecision);
				break;
			case TIMESTAMP_WITH_TIME_ZONE:
				throw new UnsupportedOperationException();
			case ARRAY:
				elementGetter = ArrayData::getArray;
				break;
			case MULTISET:
			case MAP:
				elementGetter = ArrayData::getMap;
				break;
			case ROW:
			case STRUCTURED_TYPE:
				final int rowFieldCount = getFieldCount(elementType);
				elementGetter = (array, pos) -> array.getRow(pos, rowFieldCount);
				break;
			case DISTINCT_TYPE:
				elementGetter = createElementGetter(((DistinctType) elementType).getSourceType());
				break;
			case RAW:
				elementGetter = ArrayData::getRawValue;
				break;
			case NULL:
			case SYMBOL:
			case UNRESOLVED:
			default:
				throw new IllegalArgumentException();
		}
		if (!elementType.isNullable()) {
			return elementGetter;
		}
		return (array, pos) -> {
			if (array.isNullAt(pos)) {
				return null;
			}
			return elementGetter.getElementOrNull(array, pos);
		};
	}

	/**
	 * Accessor for getting the elements of an array during runtime.
	 *
	 * @see #createElementGetter(LogicalType)
	 */
	interface ElementGetter extends Serializable {
		@Nullable Object getElementOrNull(ArrayData array, int pos);
	}
}
