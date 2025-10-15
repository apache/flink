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
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.VectorSearchTableSource;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import static org.apache.flink.table.types.utils.DataTypeUtils.validateInputDataType;

/** Implementation of {@link VectorSearchTableSource.VectorSearchContext }. */
public class VectorSearchRuntimeProviderContext
        implements VectorSearchTableSource.VectorSearchContext {

    private final int[][] searchColumns;
    private final ReadableConfig runtimeConfig;

    public VectorSearchRuntimeProviderContext(int[][] searchColumns, ReadableConfig runtimeConfig) {
        this.searchColumns = searchColumns;
        this.runtimeConfig = runtimeConfig;
    }

    @Override
    public int[][] getSearchColumns() {
        return searchColumns;
    }

    @Override
    public ReadableConfig runtimeConfig() {
        return runtimeConfig;
    }

    @Override
    public <T> TypeInformation<T> createTypeInformation(DataType producedDataType) {
        validateInputDataType(producedDataType);
        return InternalTypeInfo.of(producedDataType.getLogicalType());
    }

    @Override
    public <T> TypeInformation<T> createTypeInformation(LogicalType producedLogicalType) {
        return InternalTypeInfo.of(producedLogicalType);
    }

    @Override
    public DynamicTableSource.DataStructureConverter createDataStructureConverter(
            DataType producedDataType) {
        validateInputDataType(producedDataType);
        return new DataStructureConverterWrapper(
                DataStructureConverters.getConverter(producedDataType));
    }
}
