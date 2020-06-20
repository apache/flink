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

package org.apache.flink.table.data.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.types.InternalSerializers;
import org.apache.flink.table.runtime.typeutils.ArrayDataSerializer;
import org.apache.flink.table.runtime.typeutils.MapDataSerializer;
import org.apache.flink.table.runtime.typeutils.RawValueDataSerializer;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimestampType;

import java.io.Serializable;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getPrecision;

/**
 * Writer to write a composite data format, like row, array.
 * 1. Invoke {@link #reset()}.
 * 2. Write each field by writeXX or setNullAt. (Same field can not be written repeatedly.)
 * 3. Invoke {@link #complete()}.
 */
@Internal
public interface BinaryWriter {

	/**
	 * Reset writer to prepare next write.
	 */
	void reset();

	/**
	 * Set null to this field.
	 */
	void setNullAt(int pos);

	void writeBoolean(int pos, boolean value);

	void writeByte(int pos, byte value);

	void writeShort(int pos, short value);

	void writeInt(int pos, int value);

	void writeLong(int pos, long value);

	void writeFloat(int pos, float value);

	void writeDouble(int pos, double value);

	void writeString(int pos, StringData value);

	void writeBinary(int pos, byte[] bytes);

	void writeDecimal(int pos, DecimalData value, int precision);

	void writeTimestamp(int pos, TimestampData value, int precision);

	void writeArray(int pos, ArrayData value, ArrayDataSerializer serializer);

	void writeMap(int pos, MapData value, MapDataSerializer serializer);

	void writeRow(int pos, RowData value, RowDataSerializer serializer);

	void writeRawValue(int pos, RawValueData<?> value, RawValueDataSerializer<?> serializer);

	/**
	 * Finally, complete write to set real size to binary.
	 */
	void complete();

	// --------------------------------------------------------------------------------------------

	/**
	 * @deprecated Use {@link #createValueSetter(LogicalType)} for avoiding logical types during runtime.
	 */
	@Deprecated
	static void write(
			BinaryWriter writer, int pos, Object o, LogicalType type, TypeSerializer<?> serializer) {
		switch (type.getTypeRoot()) {
			case BOOLEAN:
				writer.writeBoolean(pos, (boolean) o);
				break;
			case TINYINT:
				writer.writeByte(pos, (byte) o);
				break;
			case SMALLINT:
				writer.writeShort(pos, (short) o);
				break;
			case INTEGER:
			case DATE:
			case TIME_WITHOUT_TIME_ZONE:
			case INTERVAL_YEAR_MONTH:
				writer.writeInt(pos, (int) o);
				break;
			case BIGINT:
			case INTERVAL_DAY_TIME:
				writer.writeLong(pos, (long) o);
				break;
			case TIMESTAMP_WITHOUT_TIME_ZONE:
				TimestampType timestampType = (TimestampType) type;
				writer.writeTimestamp(pos, (TimestampData) o, timestampType.getPrecision());
				break;
			case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
				LocalZonedTimestampType lzTs = (LocalZonedTimestampType) type;
				writer.writeTimestamp(pos, (TimestampData) o, lzTs.getPrecision());
				break;
			case FLOAT:
				writer.writeFloat(pos, (float) o);
				break;
			case DOUBLE:
				writer.writeDouble(pos, (double) o);
				break;
			case CHAR:
			case VARCHAR:
				writer.writeString(pos, (StringData) o);
				break;
			case DECIMAL:
				DecimalType decimalType = (DecimalType) type;
				writer.writeDecimal(pos, (DecimalData) o, decimalType.getPrecision());
				break;
			case ARRAY:
				writer.writeArray(pos, (ArrayData) o, (ArrayDataSerializer) serializer);
				break;
			case MAP:
			case MULTISET:
				writer.writeMap(pos, (MapData) o, (MapDataSerializer) serializer);
				break;
			case ROW:
			case STRUCTURED_TYPE:
				writer.writeRow(pos, (RowData) o, (RowDataSerializer) serializer);
				break;
			case RAW:
				writer.writeRawValue(pos, (RawValueData<?>) o, (RawValueDataSerializer<?>) serializer);
				break;
			case BINARY:
			case VARBINARY:
				writer.writeBinary(pos, (byte[]) o);
				break;
			default:
				throw new UnsupportedOperationException("Not support type: " + type);
		}
	}

	/**
	 * Creates an accessor for setting the elements of an array writer during runtime.
	 *
	 * @param elementType the element type of the array
	 */
	static ValueSetter createValueSetter(LogicalType elementType) {
		// ordered by type root definition
		switch (elementType.getTypeRoot()) {
			case CHAR:
			case VARCHAR:
				return (writer, pos, value) -> writer.writeString(pos, (StringData) value);
			case BOOLEAN:
				return (writer, pos, value) -> writer.writeBoolean(pos, (boolean) value);
			case BINARY:
			case VARBINARY:
				return (writer, pos, value) -> writer.writeBinary(pos, (byte[]) value);
			case DECIMAL:
				final int decimalPrecision = getPrecision(elementType);
				return (writer, pos, value) -> writer.writeDecimal(pos, (DecimalData) value, decimalPrecision);
			case TINYINT:
				return (writer, pos, value) -> writer.writeByte(pos, (byte) value);
			case SMALLINT:
				return (writer, pos, value) -> writer.writeShort(pos, (short) value);
			case INTEGER:
			case DATE:
			case TIME_WITHOUT_TIME_ZONE:
			case INTERVAL_YEAR_MONTH:
				return (writer, pos, value) -> writer.writeInt(pos, (int) value);
			case BIGINT:
			case INTERVAL_DAY_TIME:
				return (writer, pos, value) -> writer.writeLong(pos, (long) value);
			case FLOAT:
				return (writer, pos, value) -> writer.writeFloat(pos, (float) value);
			case DOUBLE:
				return (writer, pos, value) -> writer.writeDouble(pos, (double) value);
			case TIMESTAMP_WITHOUT_TIME_ZONE:
			case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
				final int timestampPrecision = getPrecision(elementType);
				return (writer, pos, value) -> writer.writeTimestamp(pos, (TimestampData) value, timestampPrecision);
			case TIMESTAMP_WITH_TIME_ZONE:
				throw new UnsupportedOperationException();
			case ARRAY:
				final ArrayDataSerializer arraySerializer = (ArrayDataSerializer) InternalSerializers.create(elementType);
				return (writer, pos, value) -> writer.writeArray(pos, (ArrayData) value, arraySerializer);
			case MULTISET:
			case MAP:
				final MapDataSerializer mapSerializer = (MapDataSerializer) InternalSerializers.create(elementType);
				return (writer, pos, value) -> writer.writeMap(pos, (MapData) value, mapSerializer);
			case ROW:
			case STRUCTURED_TYPE:
				final RowDataSerializer rowSerializer = (RowDataSerializer) InternalSerializers.create(elementType);
				return (writer, pos, value) -> writer.writeRow(pos, (RowData) value, rowSerializer);
			case DISTINCT_TYPE:
				return createValueSetter(((DistinctType) elementType).getSourceType());
			case RAW:
				final RawValueDataSerializer<?> rawSerializer = (RawValueDataSerializer<?>) InternalSerializers.create(elementType);
				return (writer, pos, value) -> writer.writeRawValue(pos, (RawValueData<?>) value, rawSerializer);
			case NULL:
			case SYMBOL:
			case UNRESOLVED:
			default:
				throw new IllegalArgumentException();
		}
	}

	/**
	 * Accessor for setting the elements of an array writer during runtime.
	 */
	interface ValueSetter extends Serializable {
		void setValue(BinaryArrayWriter writer, int pos, Object value);
	}
}
