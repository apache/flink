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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.state.api.OperatorIdentifier;
import org.apache.flink.state.api.SavepointReader;
import org.apache.flink.state.api.filter.SavepointKeyFilter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Savepoint data stream scan provider for the general namespaced (e.g. window-scoped) keyed state
 * table, emitting one row per {@code (key, namespace)} pair (see {@link WindowKeyedStateReader}).
 *
 * <p>Unlike {@link SavepointDataStreamScanProvider}, this reads state registered under an arbitrary
 * namespace rather than {@code VoidNamespace}, so it drives {@link
 * SavepointReader#readWindowKeyedState}.
 */
@SuppressWarnings("rawtypes")
public class WindowSavepointDataStreamScanProvider
        extends AbstractMultiColumnScanProvider<WindowStateTableMapping> {

    public WindowSavepointDataStreamScanProvider(
            @Nullable final String stateBackendType,
            final String statePath,
            final OperatorIdentifier operatorIdentifier,
            final Supplier<WindowStateTableMapping> mappingSupplier,
            RowType rowType,
            @Nullable SavepointKeyFilter keyFilter) {
        super(stateBackendType, statePath, operatorIdentifier, mappingSupplier, rowType, keyFilter);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected DataStream<RowData> readState(
            SavepointReader savepointReader, WindowStateTableMapping mapping) throws Exception {
        List<String> stateNames =
                mapping.getAllValueColumns().stream()
                        .map(StateValueColumnConfiguration::getStateName)
                        .collect(Collectors.toList());

        return savepointReader.readWindowKeyedState(
                operatorIdentifier,
                new WindowKeyedStateReader(rowType, mapping),
                (TypeInformation) mapping.getKeyTypeInfo(),
                (TypeSerializer) mapping.getNamespaceSerializer(),
                stateNames,
                InternalTypeInfo.of(rowType),
                keyFilter);
    }
}
