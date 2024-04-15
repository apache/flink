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

package org.apache.flink.runtime.state;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.execution.RestoreMode;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;

import javax.annotation.Nonnull;

import java.util.Collection;

/**
 * A <b>State Backend</b> defines how the state of a streaming application is stored locally within
 * the cluster. Different State Backends store their state in different fashions, and use different
 * data structures to hold the state of a running application.
 *
 * <p>For example, the {@link org.apache.flink.runtime.state.hashmap.HashMapStateBackend hashmap
 * state backend} keeps working state in the memory of the TaskManager. The backend is lightweight
 * and without additional dependencies.
 *
 * <p>The {@code EmbeddedRocksDBStateBackend} stores working state in an embedded <a
 * href="http://rocksdb.org/">RocksDB</a> and is able to scale working state to many terabytes in
 * size, only limited by available disk space across all task managers.
 *
 * <h2>Raw Bytes Storage and Backends</h2>
 *
 * <p>The {@code StateBackend} creates services for <i>keyed state</i> and <i>operator state</i>.
 *
 * <p>The {@link CheckpointableKeyedStateBackend} and {@link OperatorStateBackend} created by this
 * state backend define how to hold the working state for keys and operators. They also define how
 * to checkpoint that state, frequently using the raw bytes storage (via the {@code
 * CheckpointStreamFactory}). However, it is also possible that for example a keyed state backend
 * simply implements the bridge to a key/value store, and that it does not need to store anything in
 * the raw byte storage upon a checkpoint.
 *
 * <h2>Serializability</h2>
 *
 * <p>State Backends need to be {@link java.io.Serializable serializable}, because they distributed
 * across parallel processes (for distributed execution) together with the streaming application
 * code.
 *
 * <p>Because of that, {@code StateBackend} implementations (typically subclasses of {@link
 * AbstractStateBackend}) are meant to be like <i>factories</i> that create the proper states stores
 * that provide access to the persistent storage and hold the keyed- and operator state data
 * structures. That way, the State Backend can be very lightweight (contain only configurations)
 * which makes it easier to be serializable.
 *
 * <h2>Thread Safety</h2>
 *
 * <p>State backend implementations have to be thread-safe. Multiple threads may be creating
 * keyed-/operator state backends concurrently.
 */
@PublicEvolving
public interface StateBackend extends java.io.Serializable {

    /**
     * Return the name of this backend, default is simple class name. {@link
     * org.apache.flink.runtime.state.delegate.DelegatingStateBackend} may return the simple class
     * name of the delegated backend.
     */
    default String getName() {
        return this.getClass().getSimpleName();
    }

    /**
     * Creates a new {@link CheckpointableKeyedStateBackend} that is responsible for holding
     * <b>keyed state</b> and checkpointing it.
     *
     * <p><i>Keyed State</i> is state where each value is bound to a key.
     *
     * @param parameters The arguments bundle for creating {@link CheckpointableKeyedStateBackend}.
     * @param <K> The type of the keys by which the state is organized.
     * @return The Keyed State Backend for the given job, operator, and key group range.
     * @throws Exception This method may forward all exceptions that occur while instantiating the
     *     backend.
     */
    <K> CheckpointableKeyedStateBackend<K> createKeyedStateBackend(
            KeyedStateBackendParameters<K> parameters) throws Exception;

    /**
     * Creates a new {@link AsyncKeyedStateBackend} which supports to access <b>keyed state</b>
     * asynchronously.
     *
     * <p><i>Keyed State</i> is state where each value is bound to a key.
     *
     * @param parameters The arguments bundle for creating {@link AsyncKeyedStateBackend}.
     * @param <K> The type of the keys by which the state is organized.
     * @return The Async Keyed State Backend for the given job, operator.
     * @throws Exception This method may forward all exceptions that occur while instantiating the
     *     backend.
     */
    @Experimental
    default <K> AsyncKeyedStateBackend createAsyncKeyedStateBackend(
            KeyedStateBackendParameters<K> parameters) throws Exception {
        throw new UnsupportedOperationException(
                "Don't support createAsyncKeyedStateBackend by default");
    }

    /**
     * Tells if a state backend supports the {@link AsyncKeyedStateBackend}.
     *
     * <p>If a state backend supports {@code AsyncKeyedStateBackend}, it could use {@link
     * #createAsyncKeyedStateBackend(KeyedStateBackendParameters)} to create an async keyed state
     * backend to access <b>keyed state</b> asynchronously.
     *
     * @return If the state backend supports {@link AsyncKeyedStateBackend}.
     */
    @Experimental
    default boolean supportsAsyncKeyedStateBackend() {
        return false;
    }

    /**
     * Creates a new {@link OperatorStateBackend} that can be used for storing operator state.
     *
     * <p>Operator state is state that is associated with parallel operator (or function) instances,
     * rather than with keys.
     *
     * @param parameters The arguments bundle for creating {@link OperatorStateBackend}.
     * @return The OperatorStateBackend for operator identified by the job and operator identifier.
     * @throws Exception This method may forward all exceptions that occur while instantiating the
     *     backend.
     */
    OperatorStateBackend createOperatorStateBackend(OperatorStateBackendParameters parameters)
            throws Exception;

    /** Whether the state backend uses Flink's managed memory. */
    default boolean useManagedMemory() {
        return false;
    }

    /**
     * Tells if a state backend supports the {@link RestoreMode#NO_CLAIM} mode.
     *
     * <p>If a state backend supports {@code NO_CLAIM} mode, it should create an independent
     * snapshot when it receives {@link CheckpointType#FULL_CHECKPOINT} in {@link
     * Snapshotable#snapshot(long, long, CheckpointStreamFactory, CheckpointOptions)}.
     *
     * @return If the state backend supports {@link RestoreMode#NO_CLAIM} mode.
     */
    default boolean supportsNoClaimRestoreMode() {
        return false;
    }

    default boolean supportsSavepointFormat(SavepointFormatType formatType) {
        return formatType == SavepointFormatType.CANONICAL;
    }

    /**
     * Parameters passed to {@link
     * StateBackend#createKeyedStateBackend(KeyedStateBackendParameters)}.
     *
     * @param <K> The type of the keys by which the state is organized.
     */
    @PublicEvolving
    interface KeyedStateBackendParameters<K> {
        /** @return The runtime environment of the executing task. */
        Environment getEnv();

        JobID getJobID();

        String getOperatorIdentifier();

        TypeSerializer<K> getKeySerializer();

        int getNumberOfKeyGroups();

        /** @return Range of key-groups for which the to-be-created backend is responsible. */
        KeyGroupRange getKeyGroupRange();

        TaskKvStateRegistry getKvStateRegistry();

        /** @return Provider for TTL logic to judge about state expiration. */
        TtlTimeProvider getTtlTimeProvider();

        MetricGroup getMetricGroup();

        @Nonnull
        Collection<KeyedStateHandle> getStateHandles();

        /**
         * @return The registry to which created closeable objects will be * registered during
         *     restore.
         */
        CloseableRegistry getCancelStreamRegistry();

        double getManagedMemoryFraction();

        CustomInitializationMetrics getCustomInitializationMetrics();
    }

    /**
     * Parameters passed to {@link
     * StateBackend#createOperatorStateBackend(OperatorStateBackendParameters)}.
     */
    @PublicEvolving
    interface OperatorStateBackendParameters {
        /** @return The runtime environment of the executing task. */
        Environment getEnv();

        String getOperatorIdentifier();

        @Nonnull
        Collection<OperatorStateHandle> getStateHandles();

        /**
         * @return The registry to which created closeable objects will be * registered during
         *     restore.
         */
        CloseableRegistry getCancelStreamRegistry();

        CustomInitializationMetrics getCustomInitializationMetrics();
    }

    @Experimental
    interface CustomInitializationMetrics {
        void addMetric(String name, long value);
    }
}
