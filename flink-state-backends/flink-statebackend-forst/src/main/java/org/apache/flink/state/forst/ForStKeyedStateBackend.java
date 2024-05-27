/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.forst;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.v2.State;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.asyncprocessing.StateExecutor;
import org.apache.flink.runtime.asyncprocessing.StateRequestHandler;
import org.apache.flink.runtime.state.AsyncKeyedStateBackend;
import org.apache.flink.runtime.state.SerializedCompositeKeyBuilder;
import org.apache.flink.runtime.state.v2.StateDescriptor;
import org.apache.flink.runtime.state.v2.ValueStateDescriptor;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A KeyedStateBackend that stores its state in {@code ForSt}. This state backend can store very
 * large state that exceeds memory even disk to remote storage.
 */
public class ForStKeyedStateBackend<K> implements AsyncKeyedStateBackend {

    private static final Logger LOG = LoggerFactory.getLogger(ForStKeyedStateBackend.class);

    /** The key serializer. */
    protected final TypeSerializer<K> keySerializer;

    /** Supplier to create SerializedCompositeKeyBuilder. */
    private final Supplier<SerializedCompositeKeyBuilder<K>> serializedKeyBuilder;

    /** Supplier to create DataOutputSerializer to serialize value. */
    private final Supplier<DataOutputSerializer> valueSerializerView;

    /** Supplier to create DataInputDeserializer to deserialize value. */
    private final Supplier<DataInputDeserializer> valueDeserializerView;

    /** The container of ForSt options. */
    private final ForStResourceContainer optionsContainer;

    /** Factory function to create column family options from state name. */
    private final Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory;

    /**
     * We are not using the default column family for Flink state ops, but we still need to remember
     * this handle so that we can close it properly when the backend is closed. Note that the one
     * returned by {@link RocksDB#open(String)} is different from that by {@link
     * RocksDB#getDefaultColumnFamily()}, probably it's a bug of RocksDB java API.
     */
    private final ColumnFamilyHandle defaultColumnFamily;

    /** The native metrics monitor. */
    private final ForStNativeMetricMonitor nativeMetricMonitor;

    /**
     * Our ForSt database. The different k/v states that we have don't each have their own ForSt
     * instance. They all write to this instance but to their own column family.
     */
    protected final RocksDB db;

    /** Handler to handle state request. */
    private StateRequestHandler stateRequestHandler;

    /** Lock guarding the {@code managedStateExecutors} and {@code disposed}. */
    private final Object lock = new Object();

    /** The StateExecutors which are managed by this ForStKeyedStateBackend. */
    @GuardedBy("lock")
    private final Set<StateExecutor> managedStateExecutors;

    /** The flag indicating whether ForStKeyedStateBackend is closed. */
    @GuardedBy("lock")
    private boolean closed = false;

    // mark whether this backend is already disposed and prevent duplicate disposing
    private boolean disposed = false;

    public ForStKeyedStateBackend(
            ForStResourceContainer optionsContainer,
            TypeSerializer<K> keySerializer,
            Supplier<SerializedCompositeKeyBuilder<K>> serializedKeyBuilder,
            Supplier<DataOutputSerializer> valueSerializerView,
            Supplier<DataInputDeserializer> valueDeserializerView,
            RocksDB db,
            Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory,
            ColumnFamilyHandle defaultColumnFamilyHandle,
            ForStNativeMetricMonitor nativeMetricMonitor) {
        this.optionsContainer = Preconditions.checkNotNull(optionsContainer);
        this.keySerializer = keySerializer;
        this.serializedKeyBuilder = serializedKeyBuilder;
        this.valueSerializerView = valueSerializerView;
        this.valueDeserializerView = valueDeserializerView;
        this.db = db;
        this.columnFamilyOptionsFactory = columnFamilyOptionsFactory;
        this.defaultColumnFamily = defaultColumnFamilyHandle;
        this.nativeMetricMonitor = nativeMetricMonitor;
        this.managedStateExecutors = new HashSet<>(1);
    }

    @Override
    public void setup(@Nonnull StateRequestHandler stateRequestHandler) {
        this.stateRequestHandler = stateRequestHandler;
    }

    @Nonnull
    @Override
    @SuppressWarnings("unchecked")
    public <SV, S extends State> S createState(@Nonnull StateDescriptor<SV> stateDesc) {
        Preconditions.checkNotNull(
                stateRequestHandler,
                "A non-null stateRequestHandler must be setup before createState");
        ColumnFamilyHandle columnFamilyHandle =
                ForStOperationUtils.createColumnFamilyHandle(
                        stateDesc.getStateId(), db, columnFamilyOptionsFactory);
        if (stateDesc.getType() == StateDescriptor.Type.VALUE) {
            return (S)
                    new ForStValueState<>(
                            stateRequestHandler,
                            columnFamilyHandle,
                            (ValueStateDescriptor<SV>) stateDesc,
                            serializedKeyBuilder,
                            valueSerializerView,
                            valueDeserializerView);
        }
        throw new UnsupportedOperationException(
                String.format("Unsupported state type: %s", stateDesc.getType()));
    }

    @Override
    @Nonnull
    public StateExecutor createStateExecutor() {
        synchronized (lock) {
            if (closed) {
                throw new FlinkRuntimeException(
                        "Attempt to create StateExecutor after ForStKeyedStateBackend is disposed.");
            }
            // TODO: Make io parallelism configurable
            StateExecutor stateExecutor =
                    new ForStStateExecutor(4, db, optionsContainer.getWriteOptions());
            managedStateExecutors.add(stateExecutor);
            return stateExecutor;
        }
    }

    /** Should only be called by one thread, and only after all accesses to the DB happened. */
    @Override
    public void dispose() {
        if (this.disposed) {
            return;
        }
        synchronized (lock) {
            if (!closed) {
                IOUtils.closeQuietly(this);
            }
        }

        // IMPORTANT: null reference to signal potential async checkpoint workers that the db was
        // disposed, as
        // working on the disposed object results in SEGFAULTS.
        if (db != null) {

            // Metric collection occurs on a background thread. When this method returns
            // it is guaranteed that thr ForSt reference has been invalidated
            // and no more metric collection will be attempted against the database.
            if (nativeMetricMonitor != null) {
                nativeMetricMonitor.close();
            }

            IOUtils.closeQuietly(defaultColumnFamily);

            // ... and finally close the DB instance ...
            IOUtils.closeQuietly(db);

            LOG.info(
                    "Closed ForSt State Backend. Cleaning up ForSt local working directory {}, remote working directory {}.",
                    optionsContainer.getLocalBasePath(),
                    optionsContainer.getRemoteBasePath());

            try {
                optionsContainer.clearDirectories();
            } catch (Exception ex) {
                LOG.warn(
                        "Could not delete ForSt local working directory {}, remote working directory {}.",
                        optionsContainer.getLocalBasePath(),
                        optionsContainer.getRemoteBasePath(),
                        ex);
            }

            IOUtils.closeQuietly(optionsContainer);
        }
        this.disposed = true;
    }

    @VisibleForTesting
    File getLocalBasePath() {
        return optionsContainer.getLocalBasePath();
    }

    @VisibleForTesting
    Path getRemoteBasePath() {
        return optionsContainer.getRemoteBasePath();
    }

    @Override
    public void close() throws IOException {
        synchronized (lock) {
            if (closed) {
                return;
            }
            closed = true;
            for (StateExecutor executor : managedStateExecutors) {
                executor.shutdown();
            }
        }
    }
}
