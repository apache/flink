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

package org.apache.flink.table.types;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryArray;
import org.apache.flink.table.dataformat.BinaryGeneric;
import org.apache.flink.table.dataformat.BinaryMap;
import org.apache.flink.table.dataformat.BinaryString;
import org.apache.flink.table.dataformat.Decimal;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LegacyTypeInformationType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TypeInformationAnyType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Map;

import static org.apache.flink.table.types.PlannerTypeUtils.isPrimitive;

/**
 * Get internal(sql engine execution data formats) and default external class for {@link LogicalType}.
 */
public class ClassLogicalTypeConverter {

	/**
	 * Get default external class for {@link LogicalType}.
	 * TODO change TimestampType default conversion class to {@link LocalDateTime} from {@link Timestamp}.
	 * TODO relace it with getting class from {@link TypeConversions#fromLogicalToDataType}.
	 */
	@Deprecated
	public static Class getDefaultExternalClassForType(LogicalType type) {
		switch (type.getTypeRoot()) {
			case BOOLEAN:
				return Boolean.class;
			case TINYINT:
				return Byte.class;
			case SMALLINT:
				return Short.class;
			case INTEGER:
				return Integer.class;
			case DATE:
				return Date.class;
			case TIME_WITHOUT_TIME_ZONE:
				return Time.class;
			case INTERVAL_YEAR_MONTH:
				return Integer.class;
			case BIGINT:
				return Long.class;
			case TIMESTAMP_WITHOUT_TIME_ZONE:
				return Timestamp.class;
			case INTERVAL_DAY_TIME:
				return Long.class;
			case FLOAT:
				return Float.class;
			case DOUBLE:
				return Double.class;
			case VARCHAR:
				return String.class;
			case DECIMAL:
				return BigDecimal.class;
			case ARRAY:
				if (type instanceof LegacyTypeInformationType) {
					return ((LegacyTypeInformationType) type).getTypeInformation().getTypeClass();
				}
				ArrayType arrayType = (ArrayType) type;
				LogicalType elementType = arrayType.getElementType();
				if (elementType.isNullable() || !isPrimitive(elementType)) {
					return Array.newInstance(getDefaultExternalClassForType(elementType), 0).getClass();
				} else {
					switch (arrayType.getElementType().getTypeRoot()) {
						case BOOLEAN:
							return boolean[].class;
						case TINYINT:
							return byte[].class;
						case SMALLINT:
							return short[].class;
						case INTEGER:
							return int[].class;
						case BIGINT:
							return long[].class;
						case FLOAT:
							return float[].class;
						case DOUBLE:
							return double[].class;
						default:
							throw new RuntimeException("Not support type: " + type);
					}
				}
			case MAP:
			case MULTISET:
				return Map.class;
			case ROW:
				return Row.class;
			case VARBINARY:
				return byte[].class;
			case ANY:
				TypeInformation typeInfo = type instanceof LegacyTypeInformationType ?
						((LegacyTypeInformationType) type).getTypeInformation() :
						((TypeInformationAnyType) type).getTypeInformation();
				return typeInfo.getTypeClass();
			default:
				throw new RuntimeException("Not support type: " + type);
		}
	}

	/**
	 * Get internal(sql engine execution data formats) class for {@link LogicalType}.
	 */
	public static Class getInternalClassForType(LogicalType type) {
		switch (type.getTypeRoot()) {
			case BOOLEAN:
				return Boolean.class;
			case TINYINT:
				return Byte.class;
			case SMALLINT:
				return Short.class;
			case INTEGER:
			case DATE:
			case TIME_WITHOUT_TIME_ZONE:
			case INTERVAL_YEAR_MONTH:
				return Integer.class;
			case BIGINT:
			case TIMESTAMP_WITHOUT_TIME_ZONE:
			case INTERVAL_DAY_TIME:
				return Long.class;
			case FLOAT:
				return Float.class;
			case DOUBLE:
				return Double.class;
			case VARCHAR:
				return BinaryString.class;
			case DECIMAL:
				return Decimal.class;
			case ARRAY:
				return BinaryArray.class;
			case MAP:
			case MULTISET:
				return BinaryMap.class;
			case ROW:
				return BaseRow.class;
			case VARBINARY:
				return byte[].class;
			case ANY:
				return BinaryGeneric.class;
			default:
				throw new RuntimeException("Not support type: " + type);
		}
	}
}
