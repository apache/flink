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

package org.apache.flink.table.dataformat.util;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.BigDecimalTypeInfo;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.table.api.types.ArrayType;
import org.apache.flink.table.api.types.DataType;
import org.apache.flink.table.api.types.DataTypes;
import org.apache.flink.table.api.types.DateType;
import org.apache.flink.table.api.types.DecimalType;
import org.apache.flink.table.api.types.GenericType;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.api.types.MapType;
import org.apache.flink.table.api.types.RowType;
import org.apache.flink.table.api.types.TimestampType;
import org.apache.flink.table.api.types.TypeInfoWrappedDataType;
import org.apache.flink.table.dataformat.BaseArray;
import org.apache.flink.table.dataformat.BaseMap;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryArray;
import org.apache.flink.table.dataformat.BinaryMap;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.dataformat.BinaryString;
import org.apache.flink.table.dataformat.BinaryWriter;
import org.apache.flink.table.dataformat.Decimal;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.dataformat.NestedRow;
import org.apache.flink.table.dataformat.TypeGetterSetters;
import org.apache.flink.table.typeutils.BaseArraySerializer;
import org.apache.flink.table.typeutils.BaseMapSerializer;
import org.apache.flink.table.typeutils.BaseRowSerializer;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;
import org.apache.flink.table.typeutils.TypeUtils;

import java.io.IOException;

import static org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Util for base row.
 */
public final class BaseRowUtil {

	/**
	 * Indicates the row as an accumulate message.
	 */
	public static final byte ACCUMULATE_MSG = 0;

	/**
	 * Indicates the row as a retraction message.
	 */
	public static final byte RETRACT_MSG = 1;

	public static boolean isAccumulateMsg(BaseRow baseRow) {
		return baseRow.getHeader() == ACCUMULATE_MSG;
	}

	public static boolean isRetractMsg(BaseRow baseRow) {
		return baseRow.getHeader() == RETRACT_MSG;
	}

	public static BaseRow setAccumulate(BaseRow baseRow) {
		baseRow.setHeader(ACCUMULATE_MSG);
		return baseRow;
	}

	public static BaseRow setRetract(BaseRow baseRow) {
		baseRow.setHeader(RETRACT_MSG);
		return baseRow;
	}

	public static GenericRow toGenericRow(
		BaseRow baseRow,
		TypeInformation[] typeInfos,
		TypeSerializer[] typeSerializers) {
		if (baseRow instanceof GenericRow) {
			return (GenericRow) baseRow;
		} else {
			GenericRow row = new GenericRow(baseRow.getArity());
			row.setHeader(baseRow.getHeader());
			for (int i = 0; i < row.getArity(); i++) {
				if (baseRow.isNullAt(i)) {
					row.update(i, null);
				} else {
					row.update(i, get(baseRow, i, typeInfos[i], typeSerializers[i]));
				}
			}
			return row;
		}
	}

	public static GenericRow toGenericRow(
			BaseRow baseRow,
			InternalType[] types) {
		if (baseRow instanceof GenericRow) {
			return (GenericRow) baseRow;
		} else {
			GenericRow row = new GenericRow(baseRow.getArity());
			row.setHeader(baseRow.getHeader());
			for (int i = 0; i < row.getArity(); i++) {
				if (baseRow.isNullAt(i)) {
					row.update(i, null);
				} else {
					row.update(i, TypeGetterSetters.get(baseRow, i, types[i]));
				}
			}
			return row;
		}
	}

	public static Object get(BaseRow row, int ordinal, TypeInformation type, TypeSerializer serializer) {
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
		} else if (type instanceof BigDecimalTypeInfo) {
			BigDecimalTypeInfo dt = (BigDecimalTypeInfo) type;
			return row.getDecimal(ordinal, dt.precision(), dt.scale());
		} else if (type.equals(Types.STRING)) {
			return row.getBinaryString(ordinal);
		} else if (type.equals(BasicTypeInfo.CHAR_TYPE_INFO)) {
			return row.getChar(ordinal);
		} else if (type.equals(TimeIndicatorTypeInfo.ROWTIME_INDICATOR())) {
			return row.getLong(ordinal);
		} else if (type.equals(SqlTimeTypeInfo.DATE)) {
			return row.getInt(ordinal);
		} else if (type.equals(SqlTimeTypeInfo.TIME)) {
			return row.getInt(ordinal);
		} else if (type.equals(SqlTimeTypeInfo.TIMESTAMP)) {
			return row.getLong(ordinal);
		} else if (type.equals(BYTE_PRIMITIVE_ARRAY_TYPE_INFO)) {
			return row.getByteArray(ordinal);
		} else if (TypeUtils.isInternalArrayType(type)) {
			return row.getBaseArray(ordinal);
		} else if (type instanceof MapTypeInfo) {
			return row.getBaseMap(ordinal);
		} else if (TypeUtils.isInternalCompositeType(type)) {
			return row.getBaseRow(ordinal, type.getArity());
		} else {
			return row.getGeneric(ordinal, serializer);
		}
	}

	public static Object get(BaseRow row, int ordinal, DataType type) {
		if (type.equals(DataTypes.BOOLEAN)) {
			return row.getBoolean(ordinal);
		} else if (type.equals(DataTypes.BYTE)) {
			return row.getByte(ordinal);
		} else if (type.equals(DataTypes.SHORT)) {
			return row.getShort(ordinal);
		} else if (type.equals(DataTypes.INT)) {
			return row.getInt(ordinal);
		} else if (type.equals(DataTypes.LONG)) {
			return row.getLong(ordinal);
		} else if (type.equals(DataTypes.FLOAT)) {
			return row.getFloat(ordinal);
		} else if (type.equals(DataTypes.DOUBLE)) {
			return row.getDouble(ordinal);
		} else if (type instanceof DecimalType) {
			DecimalType dt = (DecimalType) type;
			return row.getDecimal(ordinal, dt.precision(), dt.scale());
		} else if (type.equals(DataTypes.STRING)) {
			return row.getBinaryString(ordinal);
		} else if (type.equals(DataTypes.CHAR)) {
			return row.getChar(ordinal);
		} else if (type.equals(DataTypes.ROWTIME_INDICATOR)) {
			return row.getLong(ordinal);
		} else if (type.equals(DataTypes.DATE)) {
			return row.getInt(ordinal);
		} else if (type.equals(DataTypes.TIME)) {
			return row.getInt(ordinal);
		} else if (type.equals(DataTypes.TIMESTAMP)) {
			return row.getLong(ordinal);
		} else if (type.equals(DataTypes.BYTE_ARRAY)) {
			return row.getByteArray(ordinal);
		} else if (type instanceof ArrayType) {
			return row.getBaseArray(ordinal);
		} else if (type instanceof MapType) {
			return row.getBaseMap(ordinal);
		} else if (type instanceof RowType) {
			return row.getBaseRow(ordinal, ((RowType) type).getArity());
		} else if (type instanceof GenericType) {
			return row.getGeneric(ordinal, (GenericType) type);
		} else if (type instanceof TypeInfoWrappedDataType) {
			return TypeGetterSetters.get(row, ordinal, type.toInternalType());
		} else {
			throw new RuntimeException("Not support type: " + type);
		}
	}

	public static String toOriginString(BaseRow row, TypeInformation[] types, TypeSerializer[] serializers) {
		checkArgument(types.length == row.getArity());
		StringBuilder build = new StringBuilder("[");
		build.append(row.getHeader());
		for (int i = 0; i < row.getArity(); i++) {
			build.append(',');
			if (row.isNullAt(i)) {
				build.append("null");
			} else {
				TypeSerializer serializer = serializers != null ? serializers[i] : null;
				build.append(get(row, i, types[i], serializer));
			}
		}
		build.append(']');
		return build.toString();
	}

	public static void write(BinaryWriter writer, int pos, Object o, InternalType type, TypeSerializer serializer) {
		if (type.equals(DataTypes.BOOLEAN)) {
			writer.writeBoolean(pos, (boolean) o);
		} else if (type.equals(DataTypes.BYTE)) {
			writer.writeByte(pos, (byte) o);
		} else if (type.equals(DataTypes.SHORT)) {
			writer.writeShort(pos, (short) o);
		} else if (type.equals(DataTypes.INT)) {
			writer.writeInt(pos, (int) o);
		} else if (type.equals(DataTypes.LONG)) {
			writer.writeLong(pos, (long) o);
		} else if (type.equals(DataTypes.FLOAT)) {
			writer.writeFloat(pos, (float) o);
		} else if (type.equals(DataTypes.DOUBLE)) {
			writer.writeDouble(pos, (double) o);
		} else if (type.equals(DataTypes.STRING)) {
			writer.writeBinaryString(pos, (BinaryString) o);
		} else if (type.equals(DataTypes.CHAR)) {
			writer.writeChar(pos, (char) o);
		} else if (type instanceof DecimalType) {
			DecimalType t = (DecimalType) type;
			writer.writeDecimal(pos, (Decimal) o, t.precision(), t.scale());
		} else if (type instanceof DateType) {
			writer.writeInt(pos, (int) o);
		} else if (type.equals(DataTypes.TIME)) {
			writer.writeInt(pos, (int) o);
		} else if (type instanceof TimestampType) {
			writer.writeLong(pos, (long) o);
		} else if (type.equals(DataTypes.BYTE_ARRAY)) {
			writer.writeByteArray(pos, (byte[]) o);
		} else if (type instanceof ArrayType) {
			writeBaseArray(writer, pos, (BaseArray) o, (BaseArraySerializer) serializer);
		} else if (type instanceof MapType) {
			writeBaseMap(writer, pos, (BaseMap) o, (BaseMapSerializer) serializer);
		} else if (type instanceof RowType) {
			writeBaseRow(writer, pos, (BaseRow) o, (BaseRowSerializer) serializer);
		} else {
			writer.writeGeneric(pos, o, (GenericType) type);
		}
	}

	public static void writeBaseArray(BinaryWriter writer, int pos, BaseArray input, BaseArraySerializer serializer) {
		BinaryArray binaryArray;
		if (input instanceof BinaryArray) {
			binaryArray = (BinaryArray) input;
		} else {
			binaryArray = serializer.baseArrayToBinary(input);
		}

		writer.writeBinaryArray(pos, binaryArray);
	}

	public static void writeBaseMap(BinaryWriter writer, int pos, BaseMap input, BaseMapSerializer serializer) {
		BinaryMap binaryMap;
		if (input instanceof BinaryMap) {
			binaryMap = (BinaryMap) input;
		} else {
			binaryMap = serializer.baseMapToBinary(input);
		}

		writer.writeBinaryMap(pos, binaryMap);
	}

	public static void writeBaseRow(BinaryWriter writer, int pos, BaseRow input, BaseRowSerializer serializer) {
		if (input instanceof BinaryRow) {
			BinaryRow row = (BinaryRow) input;
			writer.writeSegments(pos, row.getAllSegments(), row.getBaseOffset(), row.getSizeInBytes());
		} else if (input instanceof NestedRow) {
			NestedRow row = (NestedRow) input;
			writer.writeNestedRow(pos, row);
		} else {
			try {
				BinaryRow row = serializer.baseRowToBinary(input);
				writer.writeBinaryRow(pos, row);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}
}
