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

package org.apache.flink.api.io.parquet;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@link ParquetSchemaConverter} is used to convert Parquet {@link MessageType} to
 * Flink field-names and {@link TypeInformation}s, and vice versa.
 */
public class ParquetSchemaConverter {

	/**
	 * Converts Parquet {@link MessageType} to Flink field-name and {@link TypeInformation}.
	 */
	public Map<String, TypeInformation<?>> convertToTypeInformation(MessageType parquetSchema) {
		List<Type> types = parquetSchema.asGroupType().getFields();
		Map<String, TypeInformation<?>> result = new HashMap<>();
		for (Type type : types) {
			String name = type.getName();
			switch (type.getRepetition()) {
				case OPTIONAL:
				case REQUIRED:
					result.put(name, convertType(type));
					break;
				case REPEATED:
					throw new UnsupportedOperationException(type + " is not supported");
			}
		}
		return result;
	}

	/**
	 * Converts a Parquet {@link Type} to a Flink {@link TypeInformation}.
	 */
	private TypeInformation<?> convertType(Type parquetType) {
		if (parquetType.isPrimitive()) {
			return convertPrimitiveType(parquetType.asPrimitiveType());
		} else {
			return convertGroupType(parquetType.asGroupType());
		}
	}

	/**
	 * Converts a primitive Parquet {@link Type} to a Flink {@link TypeInformation}.
	 */
	private TypeInformation<?> convertPrimitiveType(PrimitiveType primitiveType) {
		PrimitiveType.PrimitiveTypeName typeName = primitiveType.getPrimitiveTypeName();
		OriginalType originalType = primitiveType.getOriginalType();

		switch (typeName) {
			case BOOLEAN:
				return BasicTypeInfo.BOOLEAN_TYPE_INFO;
			case FLOAT:
				return BasicTypeInfo.FLOAT_TYPE_INFO;
			case DOUBLE:
				return BasicTypeInfo.DOUBLE_TYPE_INFO;
			case INT32:
				return convertOriginalType_INT32(originalType);
			case INT64:
				return convertOriginalType_INT64(originalType);
			case BINARY:
				return convertOriginalType_BINARY(originalType);
			case INT96:
			case FIXED_LEN_BYTE_ARRAY:
			default:
				throw new UnsupportedOperationException(typeName + " is not supported");
		}
	}

	private TypeInformation<?> convertOriginalType_INT32(OriginalType originalType) {
		if (originalType == null) {
			return BasicTypeInfo.INT_TYPE_INFO;
		}
		switch (originalType) {
			case INT_8:
				return BasicTypeInfo.BYTE_TYPE_INFO;
			case INT_16:
				return BasicTypeInfo.SHORT_TYPE_INFO;
			case INT_32:
				return BasicTypeInfo.INT_TYPE_INFO;
			case DATE:
				return BasicTypeInfo.DATE_TYPE_INFO;
			case DECIMAL:
				return BasicTypeInfo.BIG_DEC_TYPE_INFO;
			case UINT_8:
			case UINT_16:
			case UINT_32:
			case TIME_MILLIS:
			default:
				throw new UnsupportedOperationException(originalType + " is not supported");
		}
	}

	private TypeInformation<?> convertOriginalType_INT64(OriginalType originalType) {
		if (originalType == null) {
			return BasicTypeInfo.LONG_TYPE_INFO;
		}
		switch (originalType) {
			case INT_64:
				return BasicTypeInfo.LONG_TYPE_INFO;
			case DECIMAL:
				return BasicTypeInfo.BIG_DEC_TYPE_INFO;
			case TIMESTAMP_MILLIS:
			case UINT_64:
			default:
				throw new UnsupportedOperationException(originalType + " is not supported");
		}
	}

	private TypeInformation<?> convertOriginalType_BINARY(OriginalType originalType) {
		if (originalType == null) {
			return PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO;
		}
		switch (originalType) {
			case UTF8:
			case ENUM:
			case JSON:
				return BasicTypeInfo.STRING_TYPE_INFO;
			case BSON:
				return PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO;
			case DECIMAL:
				return BasicTypeInfo.BIG_DEC_TYPE_INFO;
			default:
				throw new UnsupportedOperationException(originalType + " is not supported");
		}
	}

	/**
	 * Converts a combined Parquet {@link Type} to a Flink {@link TypeInformation}.
	 */
	private TypeInformation<?> convertGroupType(GroupType groupType) {
		throw new UnsupportedOperationException(groupType + " is not supported");
	}

}
