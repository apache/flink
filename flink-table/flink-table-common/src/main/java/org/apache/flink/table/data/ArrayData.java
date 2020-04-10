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

}
