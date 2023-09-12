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

package org.apache.flink.state.benchmark;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.changelog.fs.FsStateChangelogOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateChangelogOptions;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackendBuilder;
import org.apache.flink.contrib.streaming.state.RocksDBPriorityQueueConfig;
import org.apache.flink.contrib.streaming.state.RocksDBResourceContainer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.Path;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.KeyedStateFunction;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.TestLocalRecoveryConfig;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.changelog.StateChangelogStorageLoader;
import org.apache.flink.runtime.state.filesystem.FsCheckpointStorageAccess;
import org.apache.flink.runtime.state.heap.HeapKeyedStateBackend;
import org.apache.flink.runtime.state.heap.HeapKeyedStateBackendBuilder;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSetFactory;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.state.changelog.ChangelogKeyedStateBackend;
import org.apache.flink.streaming.api.operators.sorted.state.BatchExecutionStateBackend;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;

import org.rocksdb.RocksDBException;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

import static java.util.Collections.emptyList;
import static org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups.createUnregisteredTaskManagerJobMetricGroup;

/** Utils to create keyed state backend for state micro benchmark. */
public class StateBackendBenchmarkUtils {
    private static final String rootDirName = "benchmark";
    private static final String recoveryDirName = "localRecovery";
    private static final String dbDirName = "dbPath";
    private static final String checkpointDirName = "checkpointPath";
    private static final String changelogDirName = "changelogPath";
    private static File rootDir;

    public static KeyedStateBackend<Long> createKeyedStateBackend(
            StateBackendType backendType, File baseDir) throws IOException {
        switch (backendType) {
            case HEAP:
                rootDir = prepareDirectory(rootDirName, baseDir);
                return createHeapKeyedStateBackend(rootDir);
            case ROCKSDB:
                rootDir = prepareDirectory(rootDirName, baseDir);
                return createRocksDBKeyedStateBackend(rootDir);
            case HEAP_CHANGELOG:
                rootDir = prepareDirectory(rootDirName, baseDir);
                return createChangelogKeyedStateBackend(createHeapKeyedStateBackend(rootDir));
            case ROCKSDB_CHANGELOG:
                rootDir = prepareDirectory(rootDirName, baseDir);
                return createChangelogKeyedStateBackend(createRocksDBKeyedStateBackend(rootDir));
            case BATCH_EXECUTION:
                return createBatchExecutionStateBackend();
            default:
                throw new IllegalArgumentException("Unknown backend type: " + backendType);
        }
    }

    public static KeyedStateBackend<Long> createKeyedStateBackend(StateBackendType backendType)
            throws IOException {
        return createKeyedStateBackend(backendType, null);
    }

    private static CheckpointableKeyedStateBackend<Long> createBatchExecutionStateBackend() {
        return new BatchExecutionStateBackend()
                .createKeyedStateBackend(
                        MockEnvironment.builder().build(),
                        new JobID(),
                        "Test",
                        new LongSerializer(),
                        2,
                        new KeyGroupRange(0, 1),
                        null,
                        TtlTimeProvider.DEFAULT,
                        new UnregisteredMetricsGroup(),
                        Collections.emptyList(),
                        null);
    }

    private static ChangelogKeyedStateBackend<Long> createChangelogKeyedStateBackend(
            AbstractKeyedStateBackend<Long> delegatedKeyedStateBackend) throws IOException {
        File cpPathFile = prepareDirectory(checkpointDirName, rootDir);
        File changelogPathFile = prepareDirectory(changelogDirName, rootDir);
        return new ChangelogKeyedStateBackend<>(
                delegatedKeyedStateBackend,
                "Test",
                new ExecutionConfig(),
                TtlTimeProvider.DEFAULT,
                UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup(),
                Preconditions.checkNotNull(
                                StateChangelogStorageLoader.load(
                                        JobID.generate(),
                                        new Configuration()
                                                .set(
                                                        StateChangelogOptions
                                                                .STATE_CHANGE_LOG_STORAGE,
                                                        "filesystem")
                                                .set(
                                                        FsStateChangelogOptions.BASE_PATH,
                                                        changelogPathFile.getPath()),
                                        createUnregisteredTaskManagerJobMetricGroup(),
                                        TestLocalRecoveryConfig.disabled()))
                        .createWriter("test", KeyGroupRange.EMPTY_KEY_GROUP_RANGE, null),
                emptyList(),
                new FsCheckpointStorageAccess(
                        new Path(cpPathFile.getPath()), null, new JobID(), 1024, 4096));
    }

    private static RocksDBKeyedStateBackend<Long> createRocksDBKeyedStateBackend(File rootDir)
            throws IOException {
        File recoveryBaseDir = prepareDirectory(recoveryDirName, rootDir);
        File dbPathFile = prepareDirectory(dbDirName, rootDir);
        ExecutionConfig executionConfig = new ExecutionConfig();
        RocksDBResourceContainer resourceContainer = new RocksDBResourceContainer();
        RocksDBKeyedStateBackendBuilder<Long> builder =
                new RocksDBKeyedStateBackendBuilder<>(
                        "Test",
                        Thread.currentThread().getContextClassLoader(),
                        dbPathFile,
                        resourceContainer,
                        stateName -> resourceContainer.getColumnOptions(),
                        null,
                        LongSerializer.INSTANCE,
                        2,
                        new KeyGroupRange(0, 1),
                        executionConfig,
                        new LocalRecoveryConfig(null),
                        RocksDBPriorityQueueConfig.buildWithPriorityQueueType(
                                EmbeddedRocksDBStateBackend.PriorityQueueStateType.ROCKSDB),
                        TtlTimeProvider.DEFAULT,
                        LatencyTrackingStateConfig.disabled(),
                        new UnregisteredMetricsGroup(),
                        Collections.emptyList(),
                        AbstractStateBackend.getCompressionDecorator(executionConfig),
                        new CloseableRegistry());
        try {
            return builder.build();
        } catch (Exception e) {
            IOUtils.closeQuietly(resourceContainer);
            throw e;
        }
    }

    private static HeapKeyedStateBackend<Long> createHeapKeyedStateBackend(File rootDir)
            throws IOException {
        File recoveryBaseDir = prepareDirectory(recoveryDirName, rootDir);
        KeyGroupRange keyGroupRange = new KeyGroupRange(0, 1);
        int numberOfKeyGroups = keyGroupRange.getNumberOfKeyGroups();
        ExecutionConfig executionConfig = new ExecutionConfig();
        HeapPriorityQueueSetFactory priorityQueueSetFactory =
                new HeapPriorityQueueSetFactory(keyGroupRange, numberOfKeyGroups, 128);
        HeapKeyedStateBackendBuilder<Long> backendBuilder =
                new HeapKeyedStateBackendBuilder<>(
                        null,
                        new LongSerializer(),
                        Thread.currentThread().getContextClassLoader(),
                        numberOfKeyGroups,
                        keyGroupRange,
                        executionConfig,
                        TtlTimeProvider.DEFAULT,
                        LatencyTrackingStateConfig.disabled(),
                        Collections.emptyList(),
                        AbstractStateBackend.getCompressionDecorator(executionConfig),
                        new LocalRecoveryConfig(null),
                        priorityQueueSetFactory,
                        false,
                        new CloseableRegistry());
        return backendBuilder.build();
    }

    private static File prepareDirectory(String prefix, File parentDir) throws IOException {
        File target = File.createTempFile(prefix, "", parentDir);
        if (target.exists() && !target.delete()) {
            throw new IOException(
                    "Target dir {"
                            + target.getAbsolutePath()
                            + "} exists but failed to clean it up");
        } else if (!target.mkdirs()) {
            throw new IOException("Failed to create target directory: " + target.getAbsolutePath());
        }
        return target;
    }

    public static <T> ValueState<T> getValueState(
            KeyedStateBackend<T> backend, ValueStateDescriptor<T> stateDescriptor)
            throws Exception {

        return backend.getPartitionedState(
                VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, stateDescriptor);
    }

    public static <T> ListState<T> getListState(
            KeyedStateBackend<T> backend, ListStateDescriptor<T> stateDescriptor) throws Exception {

        return backend.getPartitionedState(
                VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, stateDescriptor);
    }

    public static <K, V> MapState<K, V> getMapState(
            KeyedStateBackend<K> backend, MapStateDescriptor<K, V> stateDescriptor)
            throws Exception {

        return backend.getPartitionedState(
                VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, stateDescriptor);
    }

    public static <K, S extends State, T> void applyToAllKeys(
            KeyedStateBackend<K> backend,
            final StateDescriptor<S, T> stateDescriptor,
            final KeyedStateFunction<K, S> function)
            throws Exception {
        backend.applyToAllKeys(
                VoidNamespace.INSTANCE,
                VoidNamespaceSerializer.INSTANCE,
                stateDescriptor,
                function);
    }

    public static <K, S extends State, T> void compactState(
            RocksDBKeyedStateBackend<K> backend, StateDescriptor<S, T> stateDescriptor)
            throws RocksDBException {
        backend.compactState(stateDescriptor);
    }

    public static void cleanUp(KeyedStateBackend<?> backend) throws IOException {
        backend.dispose();
        if (rootDir != null) {
            Path path = Path.fromLocalFile(rootDir);
            path.getFileSystem().delete(path, true);
        }
    }

    /** Enum of backend type. */
    public enum StateBackendType {
        HEAP,
        ROCKSDB,
        HEAP_CHANGELOG,
        ROCKSDB_CHANGELOG,
        BATCH_EXECUTION
    }
}
