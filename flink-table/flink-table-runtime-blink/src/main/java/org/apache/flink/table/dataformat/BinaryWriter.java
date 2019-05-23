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
import org.apache.flink.table.typeutils.BaseRowSerializer;

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

	void writeArray(int pos, BinaryArray value);

	void writeMap(int pos, BinaryMap value);

	void writeRow(int pos, BaseRow value, BaseRowSerializer serializer);

	void writeGeneric(int pos, BinaryGeneric value);

	/**
	 * Finally, complete write to set real size to binary.
	 */
	void complete();

	static void write(BinaryWriter writer, int pos, Object o, InternalType type) {
		if (type.equals(InternalTypes.BOOLEAN)) {
			writer.writeBoolean(pos, (boolean) o);
		} else if (type.equals(InternalTypes.BYTE)) {
			writer.writeByte(pos, (byte) o);
		} else if (type.equals(InternalTypes.SHORT)) {
			writer.writeShort(pos, (short) o);
		} else if (type.equals(InternalTypes.INT)) {
			writer.writeInt(pos, (int) o);
		} else if (type.equals(InternalTypes.LONG)) {
			writer.writeLong(pos, (long) o);
		} else if (type.equals(InternalTypes.FLOAT)) {
			writer.writeFloat(pos, (float) o);
		} else if (type.equals(InternalTypes.DOUBLE)) {
			writer.writeDouble(pos, (double) o);
		} else if (type.equals(InternalTypes.STRING)) {
			writer.writeString(pos, (BinaryString) o);
		} else if (type instanceof DateType) {
			writer.writeInt(pos, (int) o);
		} else if (type.equals(InternalTypes.TIME)) {
			writer.writeInt(pos, (int) o);
		} else if (type instanceof TimestampType) {
			writer.writeLong(pos, (long) o);
		} else if (type instanceof DecimalType) {
			DecimalType decimalType = (DecimalType) type;
			writer.writeDecimal(pos, (Decimal) o, decimalType.precision());
		} else if (type instanceof ArrayType) {
			writer.writeArray(pos, (BinaryArray) o);
		} else if (type instanceof MapType) {
			writer.writeMap(pos, (BinaryMap) o);
		} else if (type instanceof RowType) {
			RowType rowType = (RowType) type;
			writer.writeRow(pos, (BaseRow) o, rowType.getBaseRowSerializer());
		} else if (type instanceof GenericType) {
			writer.writeGeneric(pos, (BinaryGeneric) o);
		} else if (type.equals(InternalTypes.BINARY)) {
			writer.writeBinary(pos, (byte[]) o);
		} else {
			throw new RuntimeException("Not support type: " + type);
		}
	}
}
