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

package org.apache.flink.table.runtime.typeutils;

import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;

import static org.apache.flink.table.types.logical.LogicalTypeRoot.ARRAY;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.BIGINT;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.BOOLEAN;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.DECIMAL;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.INTEGER;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.MAP;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.MULTISET;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.RAW;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.ROW;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE;

/**
 * Utils for type.
 */
public class TypeCheckUtils {

	public static boolean isNumeric(LogicalType type) {
		return type.getTypeRoot().getFamilies().contains(LogicalTypeFamily.NUMERIC);
	}

	public static boolean isTemporal(LogicalType type) {
		return isTimePoint(type) || isTimeInterval(type);
	}

	public static boolean isTimePoint(LogicalType type) {
		return type.getTypeRoot().getFamilies().contains(LogicalTypeFamily.DATETIME);
	}

	public static boolean isRowTime(LogicalType type) {
		return type instanceof TimestampType && ((TimestampType) type).getKind() == TimestampKind.ROWTIME;
	}

	public static boolean isProcTime(LogicalType type) {
		return type instanceof TimestampType && ((TimestampType) type).getKind() == TimestampKind.PROCTIME;
	}

	public static boolean isTimeInterval(LogicalType type) {
		// ordered by type root definition
		switch (type.getTypeRoot()) {
			case INTERVAL_YEAR_MONTH:
			case INTERVAL_DAY_TIME:
				return true;
			default:
				return false;
		}
	}

	public static boolean isCharacterString(LogicalType type) {
		return type.getTypeRoot().getFamilies().contains(LogicalTypeFamily.CHARACTER_STRING);
	}

	public static boolean isBinaryString(LogicalType type) {
		return type.getTypeRoot().getFamilies().contains(LogicalTypeFamily.BINARY_STRING);
	}

	public static boolean isTimestamp(LogicalType type) {
		return type.getTypeRoot() == TIMESTAMP_WITHOUT_TIME_ZONE;
	}

	public static boolean isTimestampWithLocalZone(LogicalType type) {
		return type.getTypeRoot() == TIMESTAMP_WITH_LOCAL_TIME_ZONE;
	}

	public static boolean isBoolean(LogicalType type) {
		return type.getTypeRoot() == BOOLEAN;
	}

	public static boolean isDecimal(LogicalType type) {
		return type.getTypeRoot() == DECIMAL;
	}

	public static boolean isInteger(LogicalType type) {
		return type.getTypeRoot() == INTEGER;
	}

	public static boolean isLong(LogicalType type) {
		return type.getTypeRoot() == BIGINT;
	}

	public static boolean isArray(LogicalType type) {
		return type.getTypeRoot() == ARRAY;
	}

	public static boolean isMap(LogicalType type) {
		return type.getTypeRoot() == MAP;
	}

	public static boolean isMultiset(LogicalType type) {
		return type.getTypeRoot() == MULTISET;
	}

	public static boolean isRaw(LogicalType type) {
		return type.getTypeRoot() == RAW;
	}

	public static boolean isRow(LogicalType type) {
		return type.getTypeRoot() == ROW;
	}

	public static boolean isComparable(LogicalType type) {
		return !isRaw(type) && !isMap(type) && !isMultiset(type) && !isRow(type) && !isArray(type);
	}

	public static boolean isMutable(LogicalType type) {
		// ordered by type root definition
		switch (type.getTypeRoot()) {
			case CHAR:
			case VARCHAR: // the internal representation of String is StringData which is mutable
			case ARRAY:
			case MULTISET:
			case MAP:
			case ROW:
			case STRUCTURED_TYPE:
			case RAW:
				return true;
			case TIMESTAMP_WITH_TIME_ZONE:
				throw new UnsupportedOperationException("Unsupported type: " + type);
			case DISTINCT_TYPE:
				return isMutable(((DistinctType) type).getSourceType());
			default:
				return false;
		}
	}

	public static boolean isReference(LogicalType type) {
		// ordered by type root definition
		switch (type.getTypeRoot()) {
			case BOOLEAN:
			case TINYINT:
			case SMALLINT:
			case INTEGER:
			case BIGINT:
			case FLOAT:
			case DOUBLE:
			case DATE:
			case TIME_WITHOUT_TIME_ZONE:
			case TIMESTAMP_WITHOUT_TIME_ZONE:
			case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
			case INTERVAL_YEAR_MONTH:
			case INTERVAL_DAY_TIME:
				return false;
			case TIMESTAMP_WITH_TIME_ZONE:
				throw new UnsupportedOperationException("Unsupported type: " + type);
			case DISTINCT_TYPE:
				return isReference(((DistinctType) type).getSourceType());
			default:
				return true;
		}
	}
}
