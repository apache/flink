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

import org.apache.flink.state.api.OperatorIdentifier;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.util.function.Supplier;

/**
 * Dynamic source for the general keyed/namespaced state tables, i.e. every table kind whose mapping
 * registers an arbitrary number of value columns (see {@link MultiColumnStateMapping}): the plain
 * keyed-state table ({@link StateTableMapping}) and the namespaced (e.g. window-scoped) keyed-state
 * table ({@link WindowStateTableMapping}).
 */
public class SavepointDynamicTableSource<M extends MultiColumnStateMapping>
        extends AbstractSavepointDynamicTableSource<M> implements SupportsProjectionPushDown {

    public SavepointDynamicTableSource(
            @Nullable final String stateBackendType,
            final String statePath,
            final OperatorIdentifier operatorIdentifier,
            final int keyColumnIndex,
            final Supplier<M> mappingSupplier,
            final RowType rowType,
            final String summaryString,
            final ScanProviderFactory<M> scanProviderFactory) {
        super(
                stateBackendType,
                statePath,
                operatorIdentifier,
                keyColumnIndex,
                mappingSupplier,
                rowType,
                summaryString,
                scanProviderFactory);
    }

    @Override
    protected AbstractSavepointDynamicTableSource<M> newInstance() {
        return new SavepointDynamicTableSource<>(
                stateBackendType,
                statePath,
                operatorIdentifier,
                keyColumnIndex,
                mappingSupplier,
                rowType,
                summaryString,
                scanProviderFactory);
    }

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void applyProjection(int[][] projectedFields, DataType producedDataType) {
        this.rowType = (RowType) producedDataType.getLogicalType();
        this.keyColumnIndex =
                TableMappingSupport.remapColumnIndex(projectedFields, this.keyColumnIndex);
        // Compose the projection lazily so the mapping is still resolved at scan time.
        final Supplier<M> prev = this.mappingSupplier;
        this.mappingSupplier = () -> (M) prev.get().project(projectedFields);
    }
}
