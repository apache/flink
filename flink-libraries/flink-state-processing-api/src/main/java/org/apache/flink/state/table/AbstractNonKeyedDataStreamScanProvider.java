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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.state.api.OperatorIdentifier;
import org.apache.flink.state.api.SavepointReader;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;

import javax.annotation.Nullable;

import java.util.function.Supplier;

/**
 * Shared scan-time logic for {@link OperatorStateDataStreamScanProvider} and {@link
 * BroadcastStateDataStreamScanProvider}: opens the {@link SavepointReader} against the configured
 * state backend and resolves the lazy mapping. Subclasses supply the actual {@link SavepointReader}
 * call and its row-mapping logic via {@link #readState}.
 *
 * <p>Unlike keyed state (see {@link AbstractSavepointDataStreamScanProvider}), operator {@code
 * ListState}/{@code UnionState}/{@code BroadcastState} have no key column.
 */
@Internal
abstract class AbstractNonKeyedDataStreamScanProvider<M> implements DataStreamScanProvider {

    @Nullable protected final String stateBackendType;
    protected final String statePath;
    protected final OperatorIdentifier operatorIdentifier;
    private final Supplier<M> mappingSupplier;
    protected final RowType rowType;

    protected AbstractNonKeyedDataStreamScanProvider(
            @Nullable final String stateBackendType,
            final String statePath,
            final OperatorIdentifier operatorIdentifier,
            final Supplier<M> mappingSupplier,
            final RowType rowType) {
        this.stateBackendType = stateBackendType;
        this.statePath = statePath;
        this.operatorIdentifier = operatorIdentifier;
        this.mappingSupplier = mappingSupplier;
        this.rowType = rowType;
    }

    @Override
    public boolean isBounded() {
        return true;
    }

    @Override
    public DataStream<RowData> produceDataStream(
            ProviderContext providerContext, StreamExecutionEnvironment execEnv) {
        try {
            SavepointReader savepointReader =
                    AbstractSavepointDataStreamScanProvider.createSavepointReader(
                            stateBackendType, statePath, execEnv, getClass().getClassLoader());

            // Resolve the lazy mapping at scan time (class loading deferred from planning).
            return readState(savepointReader, mappingSupplier.get());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Drives the actual {@link SavepointReader} call for this scan and maps the result into {@link
     * #rowType}-shaped rows.
     */
    protected abstract DataStream<RowData> readState(SavepointReader savepointReader, M mapping)
            throws Exception;

    /**
     * The {@link TypeInformation} the {@link SavepointReader} needs for a raw state value: its SQL
     * logical type paired with the serializer the state was written with.
     */
    static TypeInformation<Object> externalTypeInfo(
            LogicalType logicalType, TypeSerializer<Object> serializer) {
        return ExternalTypeInfo.of(TypeConversions.fromLogicalToDataType(logicalType), serializer);
    }
}
