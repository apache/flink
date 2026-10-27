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

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Base for scan providers whose mapping registers an arbitrary number of value columns, each as its
 * own state (see {@link MultiColumnStateMapping}): {@link SavepointDataStreamScanProvider} and
 * {@link WindowSavepointDataStreamScanProvider}.
 */
@Internal
abstract class AbstractMultiColumnScanProvider<M extends MultiColumnStateMapping>
        extends AbstractSavepointDataStreamScanProvider<M> {

    protected AbstractMultiColumnScanProvider(
            @Nullable final String stateBackendType,
            final String statePath,
            final OperatorIdentifier operatorIdentifier,
            final Supplier<M> mappingSupplier,
            final RowType rowType,
            @Nullable final SavepointKeyFilter keyFilter) {
        super(stateBackendType, statePath, operatorIdentifier, mappingSupplier, rowType, keyFilter);
    }

    /**
     * Builds descriptors for ALL original value columns so that key(-and-namespace) enumeration
     * works even when a projection removes all value columns from the output (e.g. {@code SELECT k
     * FROM t WHERE k = 5}).
     */
    @Override
    protected final void prepareStateDescriptors(M mapping) {
        List<StateValueColumnConfiguration> columns = mapping.getAllValueColumns();
        Map<String, StateSchemaInfo> fallbackSchemas =
                loadFallbackSchemas(
                        columns.stream()
                                .anyMatch(
                                        c ->
                                                isSerializerMissing(
                                                        c.getStateType(),
                                                        c.getMapKeyTypeSerializer(),
                                                        c.getValueTypeSerializer())));

        for (StateValueColumnConfiguration columnConfig : columns) {
            StateDescriptor<?, ?> descriptor =
                    buildStateDescriptor(
                            columnConfig.getStateName(),
                            columnConfig.getStateType(),
                            columnConfig.getActualStateKind(),
                            columnConfig.getMapKeyTypeSerializer(),
                            columnConfig.getValueTypeSerializer(),
                            fallbackSchemas);
            columnConfig.setStateDescriptor(descriptor);
        }
    }
}
