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
import org.apache.flink.table.runtime.typeutils.BaseArraySerializer;
import org.apache.flink.table.runtime.typeutils.BaseMapSerializer;
import org.apache.flink.table.runtime.typeutils.BaseRowSerializer;
import org.apache.flink.table.runtime.typeutils.BinaryGenericSerializer;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimestampType;

/**
 * Writer to write a composite data format, like row, array.
 * 1. Invoke {@link #reset()}.
 * 2. Write each field by writeXX or setNullAt. (Same field can not be written repeatedly.)
 * 3. Invoke {@link #complete()}.
 */
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

	void writeString(int pos, BinaryString value);

	void writeBinary(int pos, byte[] bytes);

	void writeDecimal(int pos, Decimal value, int precision);

	void writeTimestamp(int pos, SqlTimestamp value, int precision);

	void writeArray(int pos, BaseArray value, BaseArraySerializer serializer);

	void writeMap(int pos, BaseMap value, BaseMapSerializer serializer);

	void writeRow(int pos, BaseRow value, BaseRowSerializer type);

	void writeGeneric(int pos, BinaryGeneric value, BinaryGenericSerializer serializer);

	/**
	 * Finally, complete write to set real size to binary.
	 */
	void complete();

	static void write(BinaryWriter writer, int pos,
			Object o, LogicalType type, TypeSerializer serializer) {
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
				writer.writeTimestamp(pos, (SqlTimestamp) o, timestampType.getPrecision());
				break;
			case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
				LocalZonedTimestampType lzTs = (LocalZonedTimestampType) type;
				writer.writeTimestamp(pos, (SqlTimestamp) o, lzTs.getPrecision());
				break;
			case FLOAT:
				writer.writeFloat(pos, (float) o);
				break;
			case DOUBLE:
				writer.writeDouble(pos, (double) o);
				break;
			case CHAR:
			case VARCHAR:
				writer.writeString(pos, (BinaryString) o);
				break;
			case DECIMAL:
				DecimalType decimalType = (DecimalType) type;
				writer.writeDecimal(pos, (Decimal) o, decimalType.getPrecision());
				break;
			case ARRAY:
				writer.writeArray(pos, (BaseArray) o, (BaseArraySerializer) serializer);
				break;
			case MAP:
			case MULTISET:
				writer.writeMap(pos, (BaseMap) o, (BaseMapSerializer) serializer);
				break;
			case ROW:
				writer.writeRow(pos, (BaseRow) o, (BaseRowSerializer) serializer);
				break;
			case RAW:
				writer.writeGeneric(pos, (BinaryGeneric) o, (BinaryGenericSerializer) serializer);
				break;
			case BINARY:
			case VARBINARY:
				writer.writeBinary(pos, (byte[]) o);
				break;
			default:
				throw new RuntimeException("Not support type: " + type);
		}
	}
}
