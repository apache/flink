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

package org.apache.flink.table.runtime.connector.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connector.source.DynamicTableSource.DataStructureConverter;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.util.DataFormatConverters.DataFormatConverter;
import org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeTransformation;
import org.apache.flink.table.types.utils.DataTypeUtils;

import static org.apache.flink.table.data.util.DataFormatConverters.getConverterForDataType;
import static org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext.InternalConversionClassTransformation.INTERNAL_CLASS_TRANSFORM;
import static org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter.fromDataTypeToTypeInfo;
import static org.apache.flink.table.types.logical.utils.LogicalTypeUtils.toInternalConversionClass;

/**
 * Implementation of {@link ScanTableSource.Context}. Currently we delegate
 * {@link #createTypeInformation} and {@link #createDataStructureConverter} to
 * {@link TypeInfoDataTypeConverter} and {@link DataFormatConverter}.
 *
 * <p>In the future, we can code generate the implementation of {@link #createDataStructureConverter}
 * for better performance.
 */
public class ScanRuntimeProviderContext implements ScanTableSource.Context {

	public static final ScanRuntimeProviderContext INSTANCE = new ScanRuntimeProviderContext();

	@Override
	public TypeInformation<?> createTypeInformation(DataType producedDataType) {
		DataType internalDataType = DataTypeUtils.transform(producedDataType, INTERNAL_CLASS_TRANSFORM);
		return fromDataTypeToTypeInfo(internalDataType);
	}

	@SuppressWarnings("unchecked")
	@Override
	public DataStructureConverter createDataStructureConverter(DataType producedDataType) {
		DataFormatConverter<Object, Object> converter = getConverterForDataType(producedDataType);
		return new DataFormatConverterWrapper(converter);
	}

	static final class InternalConversionClassTransformation implements TypeTransformation {
		static final TypeTransformation INTERNAL_CLASS_TRANSFORM = new  InternalConversionClassTransformation();
		@Override
		public DataType transform(DataType typeToTransform) {
			Class<?> internalClass = toInternalConversionClass(typeToTransform.getLogicalType());
			return typeToTransform.bridgedTo(internalClass);
		}
	}
}
