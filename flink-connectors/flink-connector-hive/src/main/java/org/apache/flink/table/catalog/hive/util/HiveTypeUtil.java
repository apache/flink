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

import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * Utils to convert data types between Flink and Hive.
 */
public class HiveTypeUtil {

	// Note: Need to keep this in sync with BaseSemanticAnalyzer::getTypeStringFromAST
	private static final String HIVE_ARRAY_TYPE_NAME_FORMAT = serdeConstants.LIST_TYPE_NAME + "<%s>";

	private HiveTypeUtil() {
	}

	/**
	 * Convert Flink data type to Hive data type.
	 * TODO: the following Hive types are not supported in Flink yet, including CHAR, VARCHAR, DECIMAL, MAP, STRUCT
	 * 		[FLINK-12386] Support complete mapping between Flink and Hive data types
	 *
	 * @param type a Flink data type
	 * @return the corresponding Hive data type
	 */
	public static String toHiveType(TypeInformation type) {
		if (type == BasicTypeInfo.BOOLEAN_TYPE_INFO) {
			return serdeConstants.BOOLEAN_TYPE_NAME;
		} else if (type == BasicTypeInfo.BYTE_TYPE_INFO) {
			return serdeConstants.TINYINT_TYPE_NAME;
		} else if (type == BasicTypeInfo.SHORT_TYPE_INFO) {
			return serdeConstants.SMALLINT_TYPE_NAME;
		} else if (type == BasicTypeInfo.INT_TYPE_INFO) {
			return serdeConstants.INT_TYPE_NAME;
		} else if (type == BasicTypeInfo.LONG_TYPE_INFO) {
			return serdeConstants.BIGINT_TYPE_NAME;
		} else if (type == BasicTypeInfo.FLOAT_TYPE_INFO) {
			return serdeConstants.FLOAT_TYPE_NAME;
		} else if (type == BasicTypeInfo.DOUBLE_TYPE_INFO) {
			return serdeConstants.DOUBLE_TYPE_NAME;
		} else if (type == BasicTypeInfo.STRING_TYPE_INFO) {
			return serdeConstants.STRING_TYPE_NAME;
		} else if (type == BasicTypeInfo.DATE_TYPE_INFO) {
			return serdeConstants.DATE_TYPE_NAME;
		} else if (type == BasicArrayTypeInfo.BYTE_ARRAY_TYPE_INFO) {
			return serdeConstants.BINARY_TYPE_NAME;
		} else if (type instanceof SqlTimeTypeInfo) {
			return serdeConstants.TIMESTAMP_TYPE_NAME;
		} else if (type instanceof BasicArrayTypeInfo) {
			return toHiveArrayType((BasicArrayTypeInfo) type);
		} else {
			throw new UnsupportedOperationException(
				String.format("Flink doesn't support converting type %s to Hive type yet.", type.toString()));
		}
	}

	private static String toHiveArrayType(BasicArrayTypeInfo arrayTypeInfo) {
		return String.format(HIVE_ARRAY_TYPE_NAME_FORMAT, toHiveType(arrayTypeInfo.getComponentInfo()));
	}

	/**
	 * Convert Hive data type to a Flink data type.
	 * TODO: the following Hive types are not supported in Flink yet, including CHAR, VARCHAR, DECIMAL, MAP, STRUCT
	 *      [FLINK-12386] Support complete mapping between Flink and Hive data types
	 *
	 * @param hiveType a Hive data type
	 * @return the corresponding Flink data type
	 */
	public static TypeInformation toFlinkType(TypeInfo hiveType) {
		switch (hiveType.getCategory()) {
			case PRIMITIVE:
				return toFlinkPrimitiveType((PrimitiveTypeInfo) hiveType);
			case LIST:
				ListTypeInfo listTypeInfo = (ListTypeInfo) hiveType;
				return BasicArrayTypeInfo.getInfoFor(toFlinkType(listTypeInfo.getListElementTypeInfo()).getTypeClass());
			default:
				throw new UnsupportedOperationException(
					String.format("Flink doesn't support Hive data type %s yet.", hiveType));
		}
	}

	// TODO: the following Hive types are not supported in Flink yet, including CHAR, VARCHAR, DECIMAL, MAP, STRUCT
	//    [FLINK-12386] Support complete mapping between Flink and Hive data types
	private static TypeInformation toFlinkPrimitiveType(PrimitiveTypeInfo hiveType) {
		switch (hiveType.getPrimitiveCategory()) {
			// For CHAR(p) and VARCHAR(p) types, map them to String for now because Flink doesn't yet support them.
			case CHAR:
			case VARCHAR:
			case STRING:
				return BasicTypeInfo.STRING_TYPE_INFO;
			case BOOLEAN:
				return BasicTypeInfo.BOOLEAN_TYPE_INFO;
			case BYTE:
				return BasicTypeInfo.BYTE_TYPE_INFO;
			case SHORT:
				return BasicTypeInfo.SHORT_TYPE_INFO;
			case INT:
				return BasicTypeInfo.INT_TYPE_INFO;
			case LONG:
				return BasicTypeInfo.LONG_TYPE_INFO;
			case FLOAT:
				return BasicTypeInfo.FLOAT_TYPE_INFO;
			case DOUBLE:
				return BasicTypeInfo.DOUBLE_TYPE_INFO;
			case DATE:
				return BasicTypeInfo.DATE_TYPE_INFO;
			case TIMESTAMP:
				return SqlTimeTypeInfo.TIMESTAMP;
			case BINARY:
				return BasicArrayTypeInfo.BYTE_ARRAY_TYPE_INFO;
			default:
				throw new UnsupportedOperationException(
					String.format("Flink doesn't support Hive primitive type %s yet", hiveType));
		}
	}
}
