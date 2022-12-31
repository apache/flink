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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connector.source.DynamicTableSource.DataStructureConverter;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.types.utils.DataTypeUtils.validateInputDataType;

/** Implementation of {@link ScanTableSource.Context}. */
@Internal
public final class ScanRuntimeProviderContext implements ScanTableSource.ScanContext {

    public static final ScanRuntimeProviderContext INSTANCE = new ScanRuntimeProviderContext();

    private final ScanTableSource.ScanPurpose scanPurpose;
    private final Map<String, Object> contextParameters;

    public ScanRuntimeProviderContext() {
        this(ScanTableSource.ScanPurpose.SELECT);
    }

    public ScanRuntimeProviderContext(ScanTableSource.ScanPurpose scanPurpose) {
        this(scanPurpose, new HashMap<>());
    }

    public ScanRuntimeProviderContext(
            ScanTableSource.ScanPurpose scanPurpose, Map<String, Object> contextParameters) {
        this.scanPurpose = scanPurpose;
        this.contextParameters = contextParameters;
    }

    @Override
    public TypeInformation<?> createTypeInformation(DataType producedDataType) {
        validateInputDataType(producedDataType);
        return InternalTypeInfo.of(producedDataType.getLogicalType());
    }

    @Override
    public TypeInformation<?> createTypeInformation(LogicalType producedLogicalType) {
        return InternalTypeInfo.of(producedLogicalType);
    }

    @Override
    public DataStructureConverter createDataStructureConverter(DataType producedDataType) {
        validateInputDataType(producedDataType);
        return new DataStructureConverterWrapper(
                DataStructureConverters.getConverter(producedDataType));
    }

    @Override
    public ScanTableSource.ScanPurpose getScanPurpose() {
        return scanPurpose;
    }

    @Override
    public void addContextParameter(String key, Object value) {
        this.contextParameters.put(key, value);
    }
}
