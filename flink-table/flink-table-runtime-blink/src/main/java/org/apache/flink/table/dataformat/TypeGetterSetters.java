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

import org.apache.flink.table.type.ArrayType;
import org.apache.flink.table.type.DateType;
import org.apache.flink.table.type.DecimalType;
import org.apache.flink.table.type.GenericType;
import org.apache.flink.table.type.InternalType;
import org.apache.flink.table.type.InternalTypes;
import org.apache.flink.table.type.MapType;
import org.apache.flink.table.type.RowType;
import org.apache.flink.table.type.TimestampType;

/**
 * Provide type specialized getters and setters to reduce if/else and eliminate box and unbox.
 *
 * <p>There is only setter for the fixed-length type field, because the variable-length type
 * cannot be set to the binary format such as {@link BinaryFormat}.</p>
 *
 * <p>All the {@code getXxx(int)} methods do not guarantee new object returned when every
 * time called.</p>
 */
public interface TypeGetterSetters {

	/**
	 * Because the specific row implementation such as BinaryRow uses the binary format. We must
	 * first determine if it is null, and then make a specific get.
	 *
	 * @return true if this field is null.
	 */
	boolean isNullAt(int ordinal);

	/**
	 * Set null to this field.
	 */
	void setNullAt(int ordinal);

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
	 * Get string value, internal format is BinaryString.
	 */
	BinaryString getString(int ordinal);

	/**
	 * Get decimal value, internal format is Decimal.
	 */
	Decimal getDecimal(int ordinal, int precision, int scale);

	/**
	 * Get generic value, internal format is BinaryGeneric.
	 */
	<T> BinaryGeneric<T> getGeneric(int ordinal);

	/**
	 * Get binary value, internal format is byte[].
	 */
	byte[] getBinary(int ordinal);

	/**
	 * Get array value, internal format is BinaryArray.
	 */
	BinaryArray getArray(int ordinal);

	/**
	 * Get map value, internal format is BinaryMap.
	 */
	BinaryMap getMap(int ordinal);

	/**
	 * Get row value, internal format is BaseRow.
	 */
	BaseRow getRow(int ordinal, int numFields);

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
	 * Set the decimal column value.
	 *
	 * <p>Note:
	 * Precision is compact: can call setNullAt when decimal is null.
	 * Precision is not compact: can not call setNullAt when decimal is null, must call
	 * setDecimal(i, null, precision) because we need update var-length-part.
	 */
	void setDecimal(int i, Decimal value, int precision);

	static Object get(TypeGetterSetters row, int ordinal, InternalType type) {
		if (type.equals(InternalTypes.BOOLEAN)) {
			return row.getBoolean(ordinal);
		} else if (type.equals(InternalTypes.BYTE)) {
			return row.getByte(ordinal);
		} else if (type.equals(InternalTypes.SHORT)) {
			return row.getShort(ordinal);
		} else if (type.equals(InternalTypes.INT)) {
			return row.getInt(ordinal);
		} else if (type.equals(InternalTypes.LONG)) {
			return row.getLong(ordinal);
		} else if (type.equals(InternalTypes.FLOAT)) {
			return row.getFloat(ordinal);
		} else if (type.equals(InternalTypes.DOUBLE)) {
			return row.getDouble(ordinal);
		} else if (type.equals(InternalTypes.STRING)) {
			return row.getString(ordinal);
		} else if (type instanceof DateType) {
			return row.getInt(ordinal);
		} else if (type.equals(InternalTypes.TIME)) {
			return row.getInt(ordinal);
		} else if (type instanceof TimestampType) {
			return row.getLong(ordinal);
		} else if (type instanceof DecimalType) {
			DecimalType decimalType = (DecimalType) type;
			return row.getDecimal(ordinal, decimalType.precision(), decimalType.scale());
		} else if (type instanceof ArrayType) {
			return row.getArray(ordinal);
		} else if (type instanceof MapType) {
			return row.getMap(ordinal);
		} else if (type instanceof RowType) {
			return row.getRow(ordinal, ((RowType) type).getArity());
		} else if (type instanceof GenericType) {
			return row.getGeneric(ordinal);
		} else if (type.equals(InternalTypes.BINARY)) {
			return row.getBinary(ordinal);
		} else {
			throw new RuntimeException("Not support type: " + type);
		}
	}
}
