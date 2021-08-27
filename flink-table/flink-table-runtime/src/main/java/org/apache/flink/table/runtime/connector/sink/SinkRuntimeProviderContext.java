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

package org.apache.flink.table.runtime.connector.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;

import static org.apache.flink.table.types.utils.DataTypeUtils.validateOutputDataType;

/** Implementation of {@link DynamicTableSink.Context}. */
@Internal
public final class SinkRuntimeProviderContext implements DynamicTableSink.Context {

    private final boolean isBounded;

    public SinkRuntimeProviderContext(boolean isBounded) {
        this.isBounded = isBounded;
    }

    @Override
    public boolean isBounded() {
        return isBounded;
    }

    @Override
    public TypeInformation<?> createTypeInformation(DataType consumedDataType) {
        validateOutputDataType(consumedDataType);
        return InternalTypeInfo.of(consumedDataType.getLogicalType());
    }

    @Override
    public DynamicTableSink.DataStructureConverter createDataStructureConverter(
            DataType consumedDataType) {
        validateOutputDataType(consumedDataType);
        return new DataStructureConverterWrapper(
                DataStructureConverters.getConverter(consumedDataType));
    }
}
