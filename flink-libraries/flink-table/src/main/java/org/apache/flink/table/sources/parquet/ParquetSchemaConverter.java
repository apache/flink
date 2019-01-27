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

package org.apache.flink.table.sources.parquet;

import org.apache.flink.table.api.types.ArrayType;
import org.apache.flink.table.api.types.DataTypes;
import org.apache.flink.table.api.types.DecimalType;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.dataformat.Decimal;

import org.apache.parquet.schema.DecimalMetadata;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@link ParquetSchemaConverter} is used to convert Parquet {@link MessageType} to
 * Flink field-name and {@link InternalType} pairs, and vice versa.
 *
 * <p>TODO supports more Flink type and Parquet MessageType
 */
public class ParquetSchemaConverter {

	/**
	 * Converts Parquet {@link MessageType} to Flink field-name and {@link InternalType} pairs.
	 */
	public Map<String, InternalType> convertToInternalType(MessageType parquetSchema) {
		List<Type> types = parquetSchema.asGroupType().getFields();
		Map<String, InternalType> result = new HashMap<>();
		for (Type type : types) {
			String name = type.getName();
			switch (type.getRepetition()) {
				case OPTIONAL:
				case REQUIRED:
					result.put(name, convertType(type));
					break;
				default:
					throw new UnsupportedOperationException(type + " is not supported");
			}
		}
		return result;
	}

	/**
	 * Converts a Parquet {@link Type} to a Flink {@link InternalType}.
	 */
	private InternalType convertType(Type parquetType) {
		if (parquetType.isPrimitive()) {
			return convertPrimitiveType(parquetType.asPrimitiveType());
		} else {
			return convertGroupType(parquetType.asGroupType());
		}
	}

	/**
	 * Converts a primitive Parquet {@link Type} to a Flink {@link InternalType}.
	 */
	private InternalType convertPrimitiveType(PrimitiveType primitiveType) {
		PrimitiveType.PrimitiveTypeName typeName = primitiveType.getPrimitiveTypeName();
		OriginalType originalType = primitiveType.getOriginalType();
		DecimalMetadata decimalMetadata = primitiveType.getDecimalMetadata();

		switch (typeName) {
			case BOOLEAN:
				return DataTypes.BOOLEAN;
			case FLOAT:
				return DataTypes.FLOAT;
			case DOUBLE:
				return DataTypes.DOUBLE;
			case INT32:
				return convertOriginalType_INT32(originalType, decimalMetadata);
			case INT64:
				return convertOriginalType_INT64(originalType, decimalMetadata);
			case BINARY:
				return convertOriginalType_BINARY(originalType, decimalMetadata);
			case INT96:
			case FIXED_LEN_BYTE_ARRAY:
			default:
				throw new UnsupportedOperationException(typeName + " is not supported");
		}
	}

	private InternalType convertOriginalType_INT32(OriginalType originalType, DecimalMetadata decimalMetadata) {
		if (originalType == null) {
			return DataTypes.INT;
		}
		switch (originalType) {
			case INT_8:
				return DataTypes.BYTE;
			case INT_16:
				return DataTypes.SHORT;
			case INT_32:
				return DataTypes.INT;
			case DATE:
				return DataTypes.DATE;
			case DECIMAL:
				return DecimalType.of(decimalMetadata.getPrecision(), decimalMetadata.getScale());
			case UINT_8:
			case UINT_16:
			case UINT_32:
			case TIME_MILLIS:
			default:
				throw new UnsupportedOperationException(originalType + " is not supported");
		}
	}

	private InternalType convertOriginalType_INT64(OriginalType originalType, DecimalMetadata decimalMetadata) {
		if (originalType == null) {
			return DataTypes.LONG;
		}
		switch (originalType) {
			case INT_64:
				return DataTypes.LONG;
			case DECIMAL:
				return DecimalType.of(decimalMetadata.getPrecision(), decimalMetadata.getScale());
			case TIMESTAMP_MILLIS:
			case UINT_64:
			default:
				throw new UnsupportedOperationException(originalType + " is not supported");
		}
	}

	private InternalType convertOriginalType_BINARY(OriginalType originalType, DecimalMetadata decimalMetadata) {
		if (originalType == null) {
			return DataTypes.BYTE_ARRAY;
		}
		switch (originalType) {
			case UTF8:
			case ENUM:
			case JSON:
				return DataTypes.STRING;
			case BSON:
				return DataTypes.BYTE_ARRAY;
			case DECIMAL:
				return DecimalType.of(decimalMetadata.getPrecision(), decimalMetadata.getScale());
			default:
				throw new UnsupportedOperationException(originalType + " is not supported");
		}
	}

	/**
	 * Converts a combined Parquet {@link Type} to a Flink {@link InternalType}.
	 */
	private InternalType convertGroupType(GroupType groupType) {
		throw new UnsupportedOperationException(groupType + " is not supported");
	}

	public static MessageType convert(final String[] columnNames, final InternalType... types) {
		return new MessageType("flink_schema", convertTypes(columnNames, types));
	}

	private static Type[] convertTypes(final String[] columnNames, final InternalType... internalTypes) {
		if (columnNames.length != internalTypes.length) {
			throw new IllegalStateException("Mismatched Flink columns and types. Flink columns names" +
			" found : " + Arrays.toString(columnNames) +
					" . And Flink types found : " + Arrays.toString(internalTypes));
		}
		final Type[] types = new Type[internalTypes.length];
		for (int i = 0; i < internalTypes.length; ++i) {
			InternalType flinkType = internalTypes[i];
			types[i] = convertType(columnNames[i], flinkType);
		}
		return types;
	}

	private static Type convertType(final String name, final InternalType flinkType) {
		return convertType(name, flinkType, Type.Repetition.OPTIONAL);
	}

	private static Type convertType(
		final String name, final InternalType type, final Type.Repetition repetition) {
		if (DataTypes.INT.equals(type)) {
			return Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition).named(name);
		} else if (DataTypes.SHORT.equals(type)) {
			return Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition)
				.as(OriginalType.INT_16)
				.named(name);
		} else if (DataTypes.BOOLEAN.equals(type)) {
			return Types.primitive(PrimitiveType.PrimitiveTypeName.BOOLEAN, repetition).named(name);
		} else if (DataTypes.BYTE.equals(type)) {
			return Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition)
				.as(OriginalType.INT_8)
				.named(name);
		} else if (DataTypes.DOUBLE.equals(type)) {
			return Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, repetition).named(name);
		} else if (DataTypes.FLOAT.equals(type)) {
			return Types.primitive(PrimitiveType.PrimitiveTypeName.FLOAT, repetition).named(name);
		} else if (DataTypes.LONG.equals(type)) {
			return Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, repetition).named(name);
		} else if (DataTypes.STRING.equals(type)) {
			return Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, repetition).as(OriginalType.UTF8)
				.named(name);
		} else if (DataTypes.DATE.equals(type)) {
			return Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition).as(OriginalType.DATE).named(name);
		} else if (DataTypes.TIME.equals(type)) {
			return Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition)
				.as(OriginalType.TIME_MILLIS)
				.named(name);
		} else if (DataTypes.TIMESTAMP.equals(type)) {
			return Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, repetition)
				.as(OriginalType.TIMESTAMP_MILLIS)
				.named(name);
		} else if (type instanceof DecimalType) {
			int precision = ((DecimalType) type).precision();
			int scale = ((DecimalType) type).scale();
			if (Decimal.is32BitDecimal(precision)) {
				return Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition)
					.precision(precision)
					.scale(scale)
					.as(OriginalType.DECIMAL)
					.named(name);
			} else if (Decimal.is64BitDecimal(precision)) {
				return Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, repetition)
					.precision(precision)
					.scale(scale)
					.as(OriginalType.DECIMAL)
					.named(name);
			} else {
				int numBytes = computeMinBytesForPrecision(precision);
				return Types.primitive(
					PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, repetition)
					.precision(precision)
					.scale(scale)
					.length(numBytes)
					.as(OriginalType.DECIMAL)
					.named(name);
			}
		} else if (type instanceof ArrayType) {
			return convertListType(name, (ArrayType) type);
		} else {
			throw new RuntimeException("Unsupported category " + type);
		}
	}

	private static GroupType convertListType(final String name, final ArrayType arrayType) {
		final InternalType subType = arrayType.getElementInternalType();

		return Types
			.buildGroup(Type.Repetition.OPTIONAL).as(OriginalType.LIST)
			.addField(convertType("array", subType))
			.named(name);
	}

	public static int computeMinBytesForPrecision(int precision) {
		int numBytes = 1;
		while (Math.pow(2.0, 8 * numBytes - 1) < Math.pow(10.0, precision)) {
			numBytes += 1;
		}
		return numBytes;
	}
}
