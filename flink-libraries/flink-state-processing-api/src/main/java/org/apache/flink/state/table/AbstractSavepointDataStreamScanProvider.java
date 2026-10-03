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
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.state.api.OperatorIdentifier;
import org.apache.flink.state.api.SavepointReader;
import org.apache.flink.state.api.filter.SavepointKeyFilter;
import org.apache.flink.state.api.functions.KeyedStateReaderFunction;
import org.apache.flink.state.api.schema.StateSchemaInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.StringUtils;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.function.Supplier;

/**
 * Shared scan-time logic for the keyed state scan providers: opening the {@link SavepointReader}
 * against the configured state backend, resolving the lazy mapping and building its state
 * descriptors. Subclasses supply the mapping-specific descriptor setup and {@link SavepointReader}
 * call.
 */
@Internal
@SuppressWarnings("rawtypes")
abstract class AbstractSavepointDataStreamScanProvider<M extends SavepointStateMapping>
        implements DataStreamScanProvider {

    @Nullable protected final String stateBackendType;
    protected final String statePath;
    protected final OperatorIdentifier operatorIdentifier;
    private final Supplier<M> mappingSupplier;
    protected final RowType rowType;
    @Nullable protected final SavepointKeyFilter keyFilter;

    protected AbstractSavepointDataStreamScanProvider(
            @Nullable final String stateBackendType,
            final String statePath,
            final OperatorIdentifier operatorIdentifier,
            final Supplier<M> mappingSupplier,
            final RowType rowType,
            @Nullable final SavepointKeyFilter keyFilter) {
        this.stateBackendType = stateBackendType;
        this.statePath = statePath;
        this.operatorIdentifier = operatorIdentifier;
        this.mappingSupplier = mappingSupplier;
        this.rowType = rowType;
        this.keyFilter = keyFilter;
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
                    createSavepointReader(
                            stateBackendType, statePath, execEnv, getClass().getClassLoader());

            // Resolve the lazy mapping at scan time (class loading deferred from planning).
            M mapping = mappingSupplier.get();
            prepareStateDescriptors(mapping);

            return readState(savepointReader, mapping);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Builds the {@link StateDescriptor}(s) needed for this scan and registers them onto {@code
     * mapping}, resolving fallback schemas from the savepoint header where no explicit serializer
     * is available.
     */
    protected abstract void prepareStateDescriptors(M mapping);

    /** Drives the actual {@link SavepointReader} call for this scan. */
    protected abstract DataStream<RowData> readState(SavepointReader savepointReader, M mapping)
            throws Exception;

    /**
     * Reads plain ({@code VoidNamespace}) keyed state. Namespaced (window) scan providers instead
     * drive {@link SavepointReader#readWindowKeyedState} from their own {@link #readState}.
     */
    @SuppressWarnings("unchecked")
    protected final DataStream<RowData> readVoidNamespaceKeyedState(
            SavepointReader savepointReader,
            M mapping,
            KeyedStateReaderFunction<Object, RowData> readerFunction)
            throws Exception {
        return savepointReader.readKeyedState(
                operatorIdentifier,
                readerFunction,
                (TypeInformation) mapping.getKeyTypeInfo(),
                InternalTypeInfo.of(rowType),
                keyFilter);
    }

    /**
     * Loads the configured (or overridden) {@link StateBackend} and opens a {@link SavepointReader}
     * against it. Also used by the non-keyed (operator state) scan providers, which cannot extend
     * this keyed-specific abstraction.
     */
    static SavepointReader createSavepointReader(
            @Nullable String stateBackendType,
            String statePath,
            StreamExecutionEnvironment execEnv,
            ClassLoader classLoader)
            throws Exception {
        Configuration configuration = Configuration.fromMap(execEnv.getConfiguration().toMap());
        if (!StringUtils.isNullOrWhitespaceOnly(stateBackendType)) {
            configuration.set(StateBackendOptions.STATE_BACKEND, stateBackendType);
        }
        StateBackend stateBackend =
                StateBackendLoader.loadStateBackendFromConfig(configuration, classLoader, null);
        return SavepointReader.read(execEnv, statePath, stateBackend);
    }

    /**
     * Reads the savepoint header and returns a map of state name → {@link StateSchemaInfo}, or an
     * empty map if no serializer is missing (avoiding the header read entirely).
     */
    protected final Map<String, StateSchemaInfo> loadFallbackSchemas(boolean anyMissingSerializer) {
        return SavepointFallbackSchemaLoader.loadFallbackSchemas(
                statePath, operatorIdentifier, anyMissingSerializer);
    }

    /**
     * Whether the serializer(s) needed to build a state descriptor could not be resolved from the
     * preloaded savepoint metadata, in which case they must be restored from the savepoint header.
     */
    static boolean isSerializerMissing(
            SavepointConnectorOptions.StateType stateType,
            @Nullable TypeSerializer mapKeyTypeSerializer,
            @Nullable TypeSerializer valueTypeSerializer) {
        return valueTypeSerializer == null
                || (stateType == SavepointConnectorOptions.StateType.MAP
                        && mapKeyTypeSerializer == null);
    }

    /**
     * Builds the {@link StateDescriptor} for a single VALUE/LIST/MAP state, restoring the original
     * serializer from {@code fallbackSchemas} for any serializer that could not be resolved from
     * the preloaded savepoint metadata.
     *
     * <p>For the coarse {@code VALUE} shape, {@code actualStateKind} distinguishes what the state
     * was actually registered as: a {@code .reduce()}/{@code .aggregate()} window function's
     * window-contents state is registered as {@code REDUCING}/{@code AGGREGATING} rather than plain
     * {@code VALUE}, and the state backend rejects descriptor lookups whose {@link
     * StateDescriptor.Type} doesn't exactly match. In those cases a matching {@link
     * ReducingStateDescriptor}/{@link AggregatingStateDescriptor} is built instead, using a
     * reduce/aggregate function that is never invoked since callers only ever read this state.
     */
    @SuppressWarnings("unchecked")
    protected static StateDescriptor<?, ?> buildStateDescriptor(
            String name,
            SavepointConnectorOptions.StateType stateType,
            StateDescriptor.Type actualStateKind,
            @Nullable TypeSerializer mapKeyTypeSerializer,
            @Nullable TypeSerializer valueTypeSerializer,
            Map<String, StateSchemaInfo> fallbackSchemas) {
        switch (stateType) {
            case VALUE:
                TypeSerializer valueSerializer =
                        resolveValueSerializer(name, valueTypeSerializer, fallbackSchemas);
                switch (actualStateKind) {
                    case REDUCING:
                        return new ReducingStateDescriptor<>(
                                name, new NoOpReduceFunction(), valueSerializer);
                    case AGGREGATING:
                        return new AggregatingStateDescriptor<>(
                                name, new IdentityAggregateFunction(), valueSerializer);
                    default:
                        return new ValueStateDescriptor<>(name, valueSerializer);
                }

            case LIST:
                return new ListStateDescriptor<>(
                        name, resolveValueSerializer(name, valueTypeSerializer, fallbackSchemas));

            case MAP:
                if (valueTypeSerializer != null && mapKeyTypeSerializer != null) {
                    return new MapStateDescriptor<>(
                            name, mapKeyTypeSerializer, valueTypeSerializer);
                }
                StateSchemaInfo schema =
                        SavepointFallbackSchemaLoader.getSchema(name, fallbackSchemas);
                return new MapStateDescriptor<>(
                        name,
                        SavepointFallbackSchemaLoader.buildFallbackSerializer(
                                schema.mapKeySnapshot),
                        SavepointFallbackSchemaLoader.buildFallbackSerializer(
                                schema.valueSnapshot));

            default:
                throw new UnsupportedOperationException("Unsupported state type: " + stateType);
        }
    }

    private static TypeSerializer resolveValueSerializer(
            String name,
            @Nullable TypeSerializer valueTypeSerializer,
            Map<String, StateSchemaInfo> fallbackSchemas) {
        if (valueTypeSerializer != null) {
            return valueTypeSerializer;
        }
        return SavepointFallbackSchemaLoader.buildFallbackSerializer(
                SavepointFallbackSchemaLoader.getSchema(name, fallbackSchemas).valueSnapshot);
    }

    /**
     * Never-invoked {@link ReduceFunction} used to build a {@link ReducingStateDescriptor} for
     * read-only access: callers only call {@code ReducingState.get()}, which never merges.
     */
    private static final class NoOpReduceFunction implements ReduceFunction<Object> {
        @Override
        public Object reduce(Object value1, Object value2) {
            throw new UnsupportedOperationException(
                    "This reduce function only supports read-only state access and should never be invoked.");
        }
    }

    /**
     * {@link AggregateFunction} used to build an {@link AggregatingStateDescriptor} for read-only
     * access, with the accumulator type used as both {@code ACC} and {@code OUT} so that {@code
     * AggregatingState.get()} returns the raw accumulator (matching the SQL column, which reflects
     * the accumulator's serializer).
     */
    private static final class IdentityAggregateFunction
            implements AggregateFunction<Object, Object, Object> {
        @Override
        public Object createAccumulator() {
            throw new UnsupportedOperationException(
                    "This aggregate function only supports read-only state access and should never be invoked.");
        }

        @Override
        public Object add(Object value, Object accumulator) {
            throw new UnsupportedOperationException(
                    "This aggregate function only supports read-only state access and should never be invoked.");
        }

        @Override
        public Object getResult(Object accumulator) {
            return accumulator;
        }

        @Override
        public Object merge(Object a, Object b) {
            throw new UnsupportedOperationException(
                    "This aggregate function only supports read-only state access and should never be invoked.");
        }
    }
}
