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
 * {@link ArrayData} is an internal data structure representing data of {@link ArrayType}
 * in Flink Table/SQL, which only contains elements of the internal data structures.
 */
@PublicEvolving
public interface ArrayData {

	/**
	 * Returns the number of elements in this array.
	 */
	int size();

	// ------------------------------------------------------------------------------------------

	/**
	 * Returns true if the specific ordinal field is null.
	 */
	boolean isNullAt(int ordinal);

	/**
	 * Gets boolean value from the specific ordinal.
	 */
	boolean getBoolean(int ordinal);

	/**
	 * Gets byte value from the specific ordinal.
	 */
	byte getByte(int ordinal);

	/**
	 * Gets short value from the specific ordinal.
	 */
	short getShort(int ordinal);

	/**
	 * Get int value from the specific ordinal.
	 */
	int getInt(int ordinal);

	/**
	 * Get long value from the specific ordinal.
	 */
	long getLong(int ordinal);

	/**
	 * Get float value from the specific ordinal.
	 */
	float getFloat(int ordinal);

	/**
	 * Get double value from the specific ordinal.
	 */
	double getDouble(int ordinal);

	/**
	 * Get string value from the specific ordinal.
	 */
	StringData getString(int ordinal);

	/**
	 * Get decimal value from the specific ordinal.
	 */
	DecimalData getDecimal(int ordinal, int precision, int scale);

	/**
	 * Get timestamp value from the specific ordinal.
	 */
	TimestampData getTimestamp(int ordinal, int precision);

	/**
	 * Get raw value from the specific ordinal.
	 */
	<T> RawValueData<T> getRawValue(int ordinal);

	/**
	 * Get binary value from the specific ordinal.
	 */
	byte[] getBinary(int ordinal);

	/**
	 * Get array value from the specific ordinal.
	 */
	ArrayData getArray(int ordinal);

	/**
	 * Get map value from the specific ordinal.
	 */
	MapData getMap(int ordinal);

	/**
	 * Get row value from the specific ordinal.
	 */
	RowData getRow(int ordinal, int numFields);

}
