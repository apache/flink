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

package org.apache.flink.table.catalog.hive.util;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

/**
 * Utils to convert data types between Flink and Hive.
 */
public class HiveTypeUtil {

	private HiveTypeUtil() {
	}

	/**
	 * Convert Flink data type to Hive data type name.
	 * TODO: the following Hive types are not supported in Flink yet, including CHAR, VARCHAR, DECIMAL, MAP, STRUCT
	 * 		[FLINK-12386] Support complete mapping between Flink and Hive data types
	 *
	 * @param type a Flink data type
	 * @return the corresponding Hive data type name
	 */
	public static String toHiveTypeName(DataType type) {
		return toHiveTypeInfo(type).getTypeName();
	}

	/**
	 * Convert Flink data type to Hive data type.
	 *
	 * @param type a Flink data type
	 * @return the corresponding Hive data type
	 */
	public static TypeInfo toHiveTypeInfo(DataType type) {
		if (type.equals(DataTypes.BOOLEAN())) {
			return TypeInfoFactory.booleanTypeInfo;
		} else if (type.equals(DataTypes.TINYINT())) {
			return TypeInfoFactory.byteTypeInfo;
		} else if (type.equals(DataTypes.SMALLINT())) {
			return TypeInfoFactory.shortTypeInfo;
		} else if (type.equals(DataTypes.INT())) {
			return TypeInfoFactory.intTypeInfo;
		} else if (type.equals(DataTypes.BIGINT())) {
			return TypeInfoFactory.longTypeInfo;
		} else if (type.equals(DataTypes.FLOAT())) {
			return TypeInfoFactory.floatTypeInfo;
		} else if (type.equals(DataTypes.DOUBLE())) {
			return TypeInfoFactory.doubleTypeInfo;
		} else if (type.equals(DataTypes.STRING())) {
			return TypeInfoFactory.stringTypeInfo;
		} else if (type.equals(DataTypes.DATE())) {
			return TypeInfoFactory.dateTypeInfo;
		} else if (type.equals(DataTypes.BYTES())) {
			return TypeInfoFactory.binaryTypeInfo;
		} else if (type.equals(DataTypes.TIMESTAMP())) {
			return TypeInfoFactory.timestampTypeInfo;
		} else {
			throw new UnsupportedOperationException(
				String.format("Flink doesn't support converting type %s to Hive type yet.", type.toString()));
		}
	}

	/**
	 * Convert Hive data type to a Flink data type.
	 * TODO: the following Hive types are not supported in Flink yet, including CHAR, VARCHAR, DECIMAL, MAP, STRUCT
	 *      [FLINK-12386] Support complete mapping between Flink and Hive data types
	 *
	 * @param hiveType a Hive data type
	 * @return the corresponding Flink data type
	 */
	public static DataType toFlinkType(TypeInfo hiveType) {
		switch (hiveType.getCategory()) {
			case PRIMITIVE:
				return toFlinkPrimitiveType((PrimitiveTypeInfo) hiveType);
			case LIST:
				ListTypeInfo listTypeInfo = (ListTypeInfo) hiveType;
				return DataTypes.ARRAY(toFlinkType(listTypeInfo.getListElementTypeInfo()));
			default:
				throw new UnsupportedOperationException(
					String.format("Flink doesn't support Hive data type %s yet.", hiveType));
		}
	}

	// TODO: the following Hive types are not supported in Flink yet, including CHAR, VARCHAR, DECIMAL, MAP, STRUCT
	//    [FLINK-12386] Support complete mapping between Flink and Hive data types
	private static DataType toFlinkPrimitiveType(PrimitiveTypeInfo hiveType) {
		switch (hiveType.getPrimitiveCategory()) {
			// For CHAR(p) and VARCHAR(p) types, map them to String for now because Flink doesn't yet support them.
			case CHAR:
			case VARCHAR:
			case STRING:
				return DataTypes.STRING();
			case BOOLEAN:
				return DataTypes.BOOLEAN();
			case BYTE:
				return DataTypes.TINYINT();
			case SHORT:
				return DataTypes.SMALLINT();
			case INT:
				return DataTypes.INT();
			case LONG:
				return DataTypes.BIGINT();
			case FLOAT:
				return DataTypes.FLOAT();
			case DOUBLE:
				return DataTypes.DOUBLE();
			case DATE:
				return DataTypes.DATE();
			case TIMESTAMP:
				return DataTypes.TIMESTAMP();
			case BINARY:
				return DataTypes.BYTES();
			default:
				throw new UnsupportedOperationException(
					String.format("Flink doesn't support Hive primitive type %s yet", hiveType));
		}
	}
}
