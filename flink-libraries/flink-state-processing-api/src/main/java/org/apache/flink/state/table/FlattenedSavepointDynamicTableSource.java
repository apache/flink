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
import org.apache.flink.state.api.filter.SavepointKeyFilter;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.util.function.Supplier;

/**
 * Dynamic source for a table exposing a single flattened LIST/MAP state, i.e. every table kind
 * whose mapping describes exactly one such state via a single fixed descriptor (see {@link
 * SingleColumnStateMapping}): the plain keyed variant ({@link FlattenedStateTableMapping}, 3-column
 * schema) and the namespaced (e.g. window-scoped) variant ({@link
 * WindowFlattenedStateTableMapping}, 4-column schema).
 *
 * <p>Unlike {@link SavepointDynamicTableSource}, projection push-down is not supported: the schema
 * is always exactly {@code (state_key[, state_window], index/map_key, value)}. Filter push-down on
 * {@code state_key} is supported (via {@link SavepointKeyFilter}), pruning key groups/keys even
 * though {@code state_key} is only part of the composite primary key.
 */
public class FlattenedSavepointDynamicTableSource<M extends SavepointStateMapping>
        extends AbstractSavepointDynamicTableSource<M> {

    public FlattenedSavepointDynamicTableSource(
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
        return new FlattenedSavepointDynamicTableSource<>(
                stateBackendType,
                statePath,
                operatorIdentifier,
                keyColumnIndex,
                mappingSupplier,
                rowType,
                summaryString,
                scanProviderFactory);
    }
}
