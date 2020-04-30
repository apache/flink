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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.table.runtime.typeutils.BigDecimalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;

import static org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.fromLogicalTypeToDataType;

/**
 * Converter between {@link TypeInformation} and {@link LogicalType}.
 *
 * <p>This class is for:
 * 1.Source, Sink.
 * 2.UDF, UDTF.
 * 3.Agg, AggFunctions, Expression, DataView.
 */
@Deprecated
public class TypeInfoLogicalTypeConverter {

	/**
	 * It will lose some information. (Like {@link PojoTypeInfo} will converted to {@link RowType})
	 * It and {@link TypeInfoLogicalTypeConverter#fromLogicalTypeToTypeInfo} not allows back-and-forth conversion.
	 */
	public static LogicalType fromTypeInfoToLogicalType(TypeInformation typeInfo) {
		DataType dataType = TypeConversions.fromLegacyInfoToDataType(typeInfo);
		return LogicalTypeDataTypeConverter.fromDataTypeToLogicalType(dataType);
	}

	/**
	 * Use {@link BigDecimalTypeInfo} to retain precision and scale of decimal.
	 */
	public static TypeInformation fromLogicalTypeToTypeInfo(LogicalType type) {
		DataType dataType = fromLogicalTypeToDataType(type)
				.nullable()
				.bridgedTo(ClassLogicalTypeConverter.getDefaultExternalClassForType(type));
		return TypeInfoDataTypeConverter.fromDataTypeToTypeInfo(dataType);
	}
}
