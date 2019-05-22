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
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.ZonedTimestampType;

/**
 * Utilities for checking {@link LogicalType}.
 */
@Internal
public final class LogicalTypeChecks {

	private static final TimeAttributeChecker TIME_ATTRIBUTE_CHECKER = new TimeAttributeChecker();

	public static boolean hasRoot(LogicalType logicalType, LogicalTypeRoot typeRoot) {
		return logicalType.getTypeRoot() == typeRoot;
	}

	public static boolean hasFamily(LogicalType logicalType, LogicalTypeFamily family) {
		return logicalType.getTypeRoot().getFamilies().contains(family);
	}

	public static boolean isTimeAttribute(LogicalType logicalType) {
		return logicalType.accept(TIME_ATTRIBUTE_CHECKER) != TimestampKind.REGULAR;
	}

	public static boolean isRowtimeAttribute(LogicalType logicalType) {
		return logicalType.accept(TIME_ATTRIBUTE_CHECKER) == TimestampKind.ROWTIME;
	}

	public static boolean isProctimeAttribute(LogicalType logicalType) {
		return logicalType.accept(TIME_ATTRIBUTE_CHECKER) == TimestampKind.PROCTIME;
	}

	private LogicalTypeChecks() {
		// no instantiation
	}

	// --------------------------------------------------------------------------------------------

	private static class TimeAttributeChecker extends LogicalTypeDefaultVisitor<TimestampKind> {

		@Override
		public TimestampKind visit(TimestampType timestampType) {
			return timestampType.getKind();
		}

		@Override
		public TimestampKind visit(ZonedTimestampType zonedTimestampType) {
			return zonedTimestampType.getKind();
		}

		@Override
		public TimestampKind visit(LocalZonedTimestampType localZonedTimestampType) {
			return localZonedTimestampType.getKind();
		}

		@Override
		protected TimestampKind defaultMethod(LogicalType logicalType) {
			// we don't verify that type is actually a timestamp
			return TimestampKind.REGULAR;
		}
	}
}
