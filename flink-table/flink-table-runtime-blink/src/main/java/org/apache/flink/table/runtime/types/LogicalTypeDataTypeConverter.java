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

import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LegacyTypeInformationType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.utils.TypeConversions;

/**
 * Converter between {@link DataType} and {@link LogicalType}.
 *
 * <p>This class is for:
 * 1.Source, Sink.
 * 2.UDF, UDTF, UDAF.
 * 3.TableEnv.
 */
@Deprecated
public class LogicalTypeDataTypeConverter {

	@Deprecated
	public static DataType fromLogicalTypeToDataType(LogicalType logicalType) {
		return TypeConversions.fromLogicalToDataType(logicalType);
	}

	/**
	 * It convert {@link LegacyTypeInformationType} to planner types.
	 */
	@Deprecated
	public static LogicalType fromDataTypeToLogicalType(DataType dataType) {
		return PlannerTypeUtils.removeLegacyTypes(dataType.getLogicalType());
	}
}
