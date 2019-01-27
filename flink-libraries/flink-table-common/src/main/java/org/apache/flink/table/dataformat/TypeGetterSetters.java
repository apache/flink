/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.	See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.dataformat;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.api.types.ArrayType;
import org.apache.flink.table.api.types.DecimalType;
import org.apache.flink.table.api.types.GenericType;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.api.types.MapType;
import org.apache.flink.table.api.types.RowType;
import org.apache.flink.table.api.types.Types;

/**
 * Provide type specialized getters and setters to reduce if/else and eliminate box and unbox.
 *
 * <p>There is only setter for the fixed-length type field, because the variable-length type
 * cannot be set to the binary format such as {@link BinaryRow}.</p>
 *
 * <p>All the {@code getXxx(int)} methods do not guarantee new object returned when every
 * time called.</p>
 */
public interface TypeGetterSetters {

	/**
	 * Get boolean value.
	 */
	boolean getBoolean(int ordinal);

	/**
	 * Get byte value.
	 */
	byte getByte(int ordinal);

	/**
	 * Get short value.
	 */
	short getShort(int ordinal);

	/**
	 * Get int value.
	 */
	int getInt(int ordinal);

	/**
	 * Get long value.
	 */
	long getLong(int ordinal);

	/**
	 * Get float value.
	 */
	float getFloat(int ordinal);

	/**
	 * Get double value.
	 */
	double getDouble(int ordinal);

	/**
	 * Get char value.
	 */
	char getChar(int ordinal);

	/**
	 * Get byte[] value.
	 */
	byte[] getByteArray(int ordinal);

	/**
	 * Get BinaryString(intenral String/Varchar) value.
	 */
	BinaryString getBinaryString(int ordinal);

	/**
	 * Get BinaryString(intenral String/Varchar) value by reuse object. The underlying bytes
	 * array is not copied.
	 */
	BinaryString getBinaryString(int ordinal, BinaryString reuseRef);

	/**
	 * Get String value.
	 */
	default String getString(int ordinal) {
		return getBinaryString(ordinal).toString();
	}

	/**
	 * Get value of DECIMAL(precision,scale) from `ordinal`..
	 */
	Decimal getDecimal(int ordinal, int precision, int scale);

	/**
	 * Get generic value by serializer.
	 */
	@Deprecated
	<T> T getGeneric(int ordinal, TypeSerializer<T> serializer);

	/**
	 * Get generic value by GenericType.
	 */
	<T> T getGeneric(int ordinal, GenericType<T> type);

	/**
	 * Get nested row value.
	 */
	BaseRow getBaseRow(int ordinal, int numFields);

	/**
	 * Get array value.
	 */
	BaseArray getBaseArray(int ordinal);

	/**
	 * Get map value.
	 */
	BaseMap getBaseMap(int ordinal);

	/**
	 * Set boolean value.
	 */
	void setBoolean(int ordinal, boolean value);

	/**
	 * Set byte value.
	 */
	void setByte(int ordinal, byte value);

	/**
	 * Set short value.
	 */
	void setShort(int ordinal, short value);

	/**
	 * Set int value.
	 */
	void setInt(int ordinal, int value);

	/**
	 * Set long value.
	 */
	void setLong(int ordinal, long value);

	/**
	 * Set float value.
	 */
	void setFloat(int ordinal, float value);

	/**
	 * Set double value.
	 */
	void setDouble(int ordinal, double value);

	/**
	 * Set char value.
	 */
	void setChar(int ordinal, char value);

	/**
	 * Set decimal value.
	 */
	void setDecimal(int ordinal, Decimal value, int precision, int scale);

	static Object get(TypeGetterSetters row, int ordinal, InternalType type) {
		if (type.equals(Types.BOOLEAN)) {
			return row.getBoolean(ordinal);
		} else if (type.equals(Types.BYTE)) {
			return row.getByte(ordinal);
		} else if (type.equals(Types.SHORT)) {
			return row.getShort(ordinal);
		} else if (type.equals(Types.INT)) {
			return row.getInt(ordinal);
		} else if (type.equals(Types.LONG)) {
			return row.getLong(ordinal);
		} else if (type.equals(Types.FLOAT)) {
			return row.getFloat(ordinal);
		} else if (type.equals(Types.DOUBLE)) {
			return row.getDouble(ordinal);
		} else if (type instanceof DecimalType) {
			DecimalType dt = (DecimalType) type;
			return row.getDecimal(ordinal, dt.precision(), dt.scale());
		} else if (type.equals(Types.STRING)) {
			return row.getBinaryString(ordinal);
		} else if (type.equals(Types.CHAR)) {
			return row.getChar(ordinal);
		} else if (type.equals(Types.ROWTIME_INDICATOR)) {
			return row.getLong(ordinal);
		} else if (type.equals(Types.DATE)) {
			return row.getInt(ordinal);
		} else if (type.equals(Types.TIME)) {
			return row.getInt(ordinal);
		} else if (type.equals(Types.TIMESTAMP)) {
			return row.getLong(ordinal);
		} else if (type.equals(Types.BYTE_ARRAY)) {
			return row.getByteArray(ordinal);
		} else if (type instanceof ArrayType) {
			return row.getBaseArray(ordinal);
		} else if (type instanceof MapType) {
			return row.getBaseMap(ordinal);
		} else if (type instanceof RowType) {
			return row.getBaseRow(ordinal, ((RowType) type).getArity());
		} else if (type instanceof GenericType) {
			return row.getGeneric(ordinal, (GenericType) type);
		} else {
			throw new RuntimeException("Not support type: " + type);
		}
	}
}
