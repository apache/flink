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

package org.apache.flink.table.types.logical.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.ZonedTimestampType;

/**
 * Utilities for handling {@link LogicalType}s.
 */
@Internal
public final class LogicalTypeUtils {

	private static final TimeAttributeRemover TIME_ATTRIBUTE_REMOVER = new TimeAttributeRemover();

	public static LogicalType removeTimeAttributes(LogicalType logicalType) {
		return logicalType.accept(TIME_ATTRIBUTE_REMOVER);
	}

	/**
	 * Returns the conversion class for the given {@link LogicalType} that is used by the
	 * table runtime.
	 *
	 * @see RowData
	 */
	public static Class<?> toInternalConversionClass(LogicalType type) {
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
			case INTERVAL_DAY_TIME:
				return Long.class;
			case TIMESTAMP_WITHOUT_TIME_ZONE:
			case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
				return TimestampData.class;
			case FLOAT:
				return Float.class;
			case DOUBLE:
				return Double.class;
			case CHAR:
			case VARCHAR:
				return StringData.class;
			case DECIMAL:
				return DecimalData.class;
			case ARRAY:
				return ArrayData.class;
			case MAP:
			case MULTISET:
				return MapData.class;
			case ROW:
				return RowData.class;
			case BINARY:
			case VARBINARY:
				return byte[].class;
			case RAW:
				return RawValueData.class;
			default:
				throw new UnsupportedOperationException("Unsupported type: " + type);
		}
	}

	// --------------------------------------------------------------------------------------------

	private static class TimeAttributeRemover extends LogicalTypeDuplicator {

		@Override
		public LogicalType visit(TimestampType timestampType) {
			return new TimestampType(
				timestampType.isNullable(),
				timestampType.getPrecision());
		}

		@Override
		public LogicalType visit(ZonedTimestampType zonedTimestampType) {
			return new ZonedTimestampType(
				zonedTimestampType.isNullable(),
				zonedTimestampType.getPrecision());
		}

		@Override
		public LogicalType visit(LocalZonedTimestampType localZonedTimestampType) {
			return new LocalZonedTimestampType(
				localZonedTimestampType.isNullable(),
				localZonedTimestampType.getPrecision());
		}
	}

	private LogicalTypeUtils() {
		// no instantiation
	}
}
