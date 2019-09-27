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

package org.apache.flink.table.runtime.types;

import org.apache.flink.table.dataformat.BaseArray;
import org.apache.flink.table.dataformat.BaseMap;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryGeneric;
import org.apache.flink.table.dataformat.BinaryString;
import org.apache.flink.table.dataformat.Decimal;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.utils.TypeConversions;

/**
 * Get internal(sql engine execution data formats) and default external class for {@link LogicalType}.
 */
public class ClassLogicalTypeConverter {

	@Deprecated
	public static Class getDefaultExternalClassForType(LogicalType type) {
		return TypeConversions.fromLogicalToDataType(type).getConversionClass();
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
			case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
			case INTERVAL_DAY_TIME:
				return Long.class;
			case FLOAT:
				return Float.class;
			case DOUBLE:
				return Double.class;
			case CHAR:
			case VARCHAR:
				return BinaryString.class;
			case DECIMAL:
				return Decimal.class;
			case ARRAY:
				return BaseArray.class;
			case MAP:
			case MULTISET:
				return BaseMap.class;
			case ROW:
				return BaseRow.class;
			case BINARY:
			case VARBINARY:
				return byte[].class;
			case ANY:
				return BinaryGeneric.class;
			default:
				throw new RuntimeException("Not support type: " + type);
		}
	}
}
