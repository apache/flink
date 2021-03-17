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

package org.apache.flink.runtime.state.hashmap;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.BackendBuildingException;
import org.apache.flink.runtime.state.ConfigurableStateBackend;
import org.apache.flink.runtime.state.DefaultOperatorStateBackendBuilder;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.TaskStateManager;
import org.apache.flink.runtime.state.heap.HeapKeyedStateBackendBuilder;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSetFactory;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.util.TernaryBoolean;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This state backend holds the working state in the memory (JVM heap) of the TaskManagers and
 * checkpoints based on the configured {@link org.apache.flink.runtime.state.CheckpointStorage}.
 *
 * <h1>State Size Considerations</h1>
 *
 * <p>Working state is kept on the TaskManager heap. If a TaskManager executes multiple tasks
 * concurrently (if the TaskManager has multiple slots, or if slot-sharing is used) then the
 * aggregate state of all tasks needs to fit into that TaskManager's memory.
 *
 * <h1>Configuration</h1>
 *
 * <p>As for all state backends, this backend can either be configured within the application (by
 * creating the backend with the respective constructor parameters and setting it on the execution
 * environment) or by specifying it in the Flink configuration.
 *
 * <p>If the state backend was specified in the application, it may pick up additional configuration
 * parameters from the Flink configuration. For example, if the backend if configured in the
 * application without a default savepoint directory, it will pick up a default savepoint directory
 * specified in the Flink configuration of the running job/cluster. That behavior is implemented via
 * the {@link #configure(ReadableConfig, ClassLoader)} method.
 */
@PublicEvolving
public class HashMapStateBackend extends AbstractStateBackend implements ConfigurableStateBackend {

    private static final long serialVersionUID = 1L;

    // ------------------------------------------------------------------------

    /**
     * Switch to chose between synchronous and asynchronous snapshots. A value of 'undefined' means
     * not yet configured, in which case the default will be used.
     */
    private final TernaryBoolean asynchronousSnapshots;

    // -----------------------------------------------------------------------

    /**
     * Creates a new state backend that stores its checkpoint data in the file system and location
     * defined by the given URI.
     *
     * <p>A file system for the file system scheme in the URI (e.g., 'file://', 'hdfs://', or
     * 'S3://') must be accessible via {@link FileSystem#get(URI)}.
     *
     * <p>For a state backend targeting HDFS, this means that the URI must either specify the
     * authority (host and port), or that the Hadoop configuration that describes that information
     * must be in the classpath.
     */
    public HashMapStateBackend() {
        this(TernaryBoolean.UNDEFINED);
    }

    /**
     * Creates a new state backend that stores its checkpoint data in the file system and location
     * defined by the given URI.
     *
     * <p>A file system for the file system scheme in the URI (e.g., 'file://', 'hdfs://', or
     * 'S3://') must be accessible via {@link FileSystem#get(URI)}.
     *
     * <p>For a state backend targeting HDFS, this means that the URI must either specify the
     * authority (host and port), or that the Hadoop configuration that describes that information
     * must be in the classpath.
     *
     * @param asynchronousSnapshots Flag to switch between synchronous and asynchronous snapshot
     *     mode.
     */
    public HashMapStateBackend(boolean asynchronousSnapshots) {
        this(TernaryBoolean.fromBoolean(asynchronousSnapshots));
    }

    /**
     * Creates a new state backend that stores its checkpoint data in the file system and location
     * defined by the given URI.
     *
     * <p>A file system for the file system scheme in the URI (e.g., 'file://', 'hdfs://', or
     * 'S3://') must be accessible via {@link FileSystem#get(URI)}.
     *
     * <p>For a state backend targeting HDFS, this means that the URI must either specify the
     * authority (host and port), or that the Hadoop configuration that describes that information
     * must be in the classpath.
     *
     * @param asynchronousSnapshots Flag to switch between synchronous and asynchronous snapshot
     *     mode. If UNDEFINED, the value configured in the runtime configuration will be used.
     */
    public HashMapStateBackend(TernaryBoolean asynchronousSnapshots) {
        checkNotNull(asynchronousSnapshots, "asynchronousSnapshots");

        this.asynchronousSnapshots = asynchronousSnapshots;
    }

    private HashMapStateBackend(HashMapStateBackend original, ReadableConfig config) {
        // if asynchronous snapshots were configured, use that setting,
        // else check the configuration
        this.asynchronousSnapshots =
                original.asynchronousSnapshots.resolveUndefined(
                        config.get(CheckpointingOptions.ASYNC_SNAPSHOTS));
    }

    @Override
    public HashMapStateBackend configure(ReadableConfig config, ClassLoader classLoader)
            throws IllegalConfigurationException {
        return new HashMapStateBackend(this, config);
    }

    @Override
    public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
            Environment env,
            JobID jobID,
            String operatorIdentifier,
            TypeSerializer<K> keySerializer,
            int numberOfKeyGroups,
            KeyGroupRange keyGroupRange,
            TaskKvStateRegistry kvStateRegistry,
            TtlTimeProvider ttlTimeProvider,
            MetricGroup metricGroup,
            @Nonnull Collection<KeyedStateHandle> stateHandles,
            CloseableRegistry cancelStreamRegistry)
            throws IOException {

        TaskStateManager taskStateManager = env.getTaskStateManager();
        LocalRecoveryConfig localRecoveryConfig = taskStateManager.createLocalRecoveryConfig();
        HeapPriorityQueueSetFactory priorityQueueSetFactory =
                new HeapPriorityQueueSetFactory(keyGroupRange, numberOfKeyGroups, 128);

        return new HeapKeyedStateBackendBuilder<>(
                        kvStateRegistry,
                        keySerializer,
                        env.getUserCodeClassLoader().asClassLoader(),
                        numberOfKeyGroups,
                        keyGroupRange,
                        env.getExecutionConfig(),
                        ttlTimeProvider,
                        stateHandles,
                        getCompressionDecorator(env.getExecutionConfig()),
                        localRecoveryConfig,
                        priorityQueueSetFactory,
                        isUsingAsynchronousSnapshots(),
                        cancelStreamRegistry)
                .build();
    }

    @Override
    public OperatorStateBackend createOperatorStateBackend(
            Environment env,
            String operatorIdentifier,
            @Nonnull Collection<OperatorStateHandle> stateHandles,
            CloseableRegistry cancelStreamRegistry)
            throws BackendBuildingException {

        return new DefaultOperatorStateBackendBuilder(
                        env.getUserCodeClassLoader().asClassLoader(),
                        env.getExecutionConfig(),
                        isUsingAsynchronousSnapshots(),
                        stateHandles,
                        cancelStreamRegistry)
                .build();
    }

    /**
     * Gets whether the key/value data structures are asynchronously snapshotted.
     *
     * <p>If not explicitly configured, this is the default value of {@link
     * CheckpointingOptions#ASYNC_SNAPSHOTS}.
     */
    public boolean isUsingAsynchronousSnapshots() {
        return asynchronousSnapshots.getOrDefault(
                CheckpointingOptions.ASYNC_SNAPSHOTS.defaultValue());
    }
}
