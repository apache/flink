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

import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;

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
	 * Get Timestamp value, internal format is SqlTimestamp.
	 */
	SqlTimestamp getTimestamp(int ordinal, int precision);

	/**
	 * Get generic value, internal format is BinaryGeneric.
	 */
	<T> BinaryGeneric<T> getGeneric(int ordinal);

	/**
	 * Get binary value, internal format is byte[].
	 */
	byte[] getBinary(int ordinal);

	/**
	 * Get array value, internal format is BaseArray.
	 */
	BaseArray getArray(int ordinal);

	/**
	 * Get map value, internal format is BaseMap.
	 */
	BaseMap getMap(int ordinal);

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

	/**
	 * Set Timestamp value.
	 *
	 * <p>Note:
	 * If precision is compact: can call setNullAt when SqlTimestamp value is null.
	 * Otherwise: can not call setNullAt when SqlTimestamp value is null, must call
	 * setTimestamp(ordinal, null, precision) because we need to update var-length-part.
	 */
	void setTimestamp(int ordinal, SqlTimestamp value, int precision);

	static Object get(TypeGetterSetters row, int ordinal, LogicalType type) {
		switch (type.getTypeRoot()) {
			case BOOLEAN:
				return row.getBoolean(ordinal);
			case TINYINT:
				return row.getByte(ordinal);
			case SMALLINT:
				return row.getShort(ordinal);
			case INTEGER:
			case DATE:
			case TIME_WITHOUT_TIME_ZONE:
			case INTERVAL_YEAR_MONTH:
				return row.getInt(ordinal);
			case BIGINT:
			case INTERVAL_DAY_TIME:
				return row.getLong(ordinal);
			case TIMESTAMP_WITHOUT_TIME_ZONE:
				TimestampType timestampType = (TimestampType) type;
				return row.getTimestamp(ordinal, timestampType.getPrecision());
			case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
				LocalZonedTimestampType lzTs = (LocalZonedTimestampType) type;
				return row.getTimestamp(ordinal, lzTs.getPrecision());
			case FLOAT:
				return row.getFloat(ordinal);
			case DOUBLE:
				return row.getDouble(ordinal);
			case CHAR:
			case VARCHAR:
				return row.getString(ordinal);
			case DECIMAL:
				DecimalType decimalType = (DecimalType) type;
				return row.getDecimal(ordinal, decimalType.getPrecision(), decimalType.getScale());
			case ARRAY:
				return row.getArray(ordinal);
			case MAP:
			case MULTISET:
				return row.getMap(ordinal);
			case ROW:
				return row.getRow(ordinal, ((RowType) type).getFieldCount());
			case BINARY:
			case VARBINARY:
				return row.getBinary(ordinal);
			case RAW:
				return row.getGeneric(ordinal);
			default:
				throw new RuntimeException("Not support type: " + type);
		}
	}
}
