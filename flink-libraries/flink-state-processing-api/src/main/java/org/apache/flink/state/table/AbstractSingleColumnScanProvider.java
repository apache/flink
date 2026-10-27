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

package org.apache.flink.state.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.state.api.OperatorIdentifier;
import org.apache.flink.state.api.filter.SavepointKeyFilter;
import org.apache.flink.state.api.schema.StateSchemaInfo;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.function.Supplier;

/**
 * Base for scan providers whose mapping describes exactly one flattened LIST/MAP state via a single
 * fixed descriptor (see {@link SingleColumnStateMapping}): {@link
 * FlattenedSavepointDataStreamScanProvider} and {@link
 * WindowFlattenedSavepointDataStreamScanProvider}.
 */
@Internal
abstract class AbstractSingleColumnScanProvider<M extends SingleColumnStateMapping>
        extends AbstractSavepointDataStreamScanProvider<M> {

    protected AbstractSingleColumnScanProvider(
            @Nullable final String stateBackendType,
            final String statePath,
            final OperatorIdentifier operatorIdentifier,
            final Supplier<M> mappingSupplier,
            final RowType rowType,
            @Nullable final SavepointKeyFilter keyFilter) {
        super(stateBackendType, statePath, operatorIdentifier, mappingSupplier, rowType, keyFilter);
    }

    @Override
    @SuppressWarnings("rawtypes")
    protected final void prepareStateDescriptors(M mapping) {
        Map<String, StateSchemaInfo> fallbackSchemas =
                loadFallbackSchemas(
                        isSerializerMissing(
                                mapping.getStateType(),
                                mapping.getMapKeyTypeSerializer(),
                                mapping.getValueTypeSerializer()));

        StateDescriptor<?, ?> descriptor =
                buildStateDescriptor(
                        mapping.getStateName(),
                        mapping.getStateType(),
                        StateDescriptor.Type.UNKNOWN,
                        mapping.getMapKeyTypeSerializer(),
                        mapping.getValueTypeSerializer(),
                        fallbackSchemas);
        mapping.setStateDescriptor(descriptor);
    }
}
