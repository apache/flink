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

package org.apache.flink.test.state;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend.PriorityQueueStateType;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackendBuilder;
import org.apache.flink.contrib.streaming.state.RocksDBResourceContainer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.TestLocalRecoveryConfig;
import org.apache.flink.runtime.state.UncompressedStreamCompressionDecorator;
import org.apache.flink.runtime.state.heap.HeapKeyedStateBackend;
import org.apache.flink.runtime.state.heap.HeapKeyedStateBackendBuilder;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSetFactory;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;

import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.util.Collection;

/**
 * Specifications for creating state backends to be used in {@link
 * SavepointStateBackendSwitchTestBase}.
 */
public final class BackendSwitchSpecs {

    /**
     * A factory interface for creating a state backend that should be able to restore its state
     * from the given state handles.
     */
    public interface BackendSwitchSpec extends AutoCloseable {

        CheckpointableKeyedStateBackend<String> createBackend(
                KeyGroupRange keyGroupRange,
                int numKeyGroups,
                Collection<KeyedStateHandle> stateHandles)
                throws Exception;
    }

    /** Specification for a {@link RocksDBKeyedStateBackend}. */
    static final BackendSwitchSpec ROCKS = new RocksSpec(PriorityQueueStateType.ROCKSDB);

    /** Specification for a {@link RocksDBKeyedStateBackend} which stores its timers on heap. */
    static final BackendSwitchSpec ROCKS_HEAP_TIMERS = new RocksSpec(PriorityQueueStateType.HEAP);

    /** Specification for a {@link HeapKeyedStateBackend}. */
    static final BackendSwitchSpec HEAP = new HeapSpec();

    private static final class RocksSpec implements BackendSwitchSpec {

        private final TemporaryFolder temporaryFolder = new TemporaryFolder();
        private final PriorityQueueStateType queueStateType;

        public RocksSpec(PriorityQueueStateType queueStateType) {
            this.queueStateType = queueStateType;
        }

        @Override
        public CheckpointableKeyedStateBackend<String> createBackend(
                KeyGroupRange keyGroupRange,
                int numKeyGroups,
                Collection<KeyedStateHandle> stateHandles)
                throws Exception {
            final RocksDBResourceContainer optionsContainer = new RocksDBResourceContainer();

            temporaryFolder.create();
            return new RocksDBKeyedStateBackendBuilder<>(
                            "no-op",
                            ClassLoader.getSystemClassLoader(),
                            temporaryFolder.newFolder(),
                            optionsContainer,
                            stateName -> optionsContainer.getColumnOptions(),
                            new KvStateRegistry()
                                    .createTaskRegistry(new JobID(), new JobVertexID()),
                            StringSerializer.INSTANCE,
                            numKeyGroups,
                            keyGroupRange,
                            new ExecutionConfig(),
                            TestLocalRecoveryConfig.disabled(),
                            queueStateType,
                            TtlTimeProvider.DEFAULT,
                            LatencyTrackingStateConfig.disabled(),
                            new UnregisteredMetricsGroup(),
                            stateHandles,
                            UncompressedStreamCompressionDecorator.INSTANCE,
                            new CloseableRegistry())
                    .build();
        }

        @Override
        public void close() throws Exception {
            temporaryFolder.delete();
        }

        @Override
        public String toString() {
            return "ROCKS(" + queueStateType + ")";
        }
    }

    private static final class HeapSpec implements BackendSwitchSpec {
        @Override
        public CheckpointableKeyedStateBackend<String> createBackend(
                KeyGroupRange keyGroupRange,
                int numKeyGroups,
                Collection<KeyedStateHandle> stateHandles)
                throws Exception {
            ExecutionConfig executionConfig = new ExecutionConfig();
            return new HeapKeyedStateBackendBuilder<>(
                            Mockito.mock(TaskKvStateRegistry.class),
                            StringSerializer.INSTANCE,
                            this.getClass().getClassLoader(),
                            numKeyGroups,
                            keyGroupRange,
                            executionConfig,
                            TtlTimeProvider.DEFAULT,
                            LatencyTrackingStateConfig.disabled(),
                            stateHandles,
                            AbstractStateBackend.getCompressionDecorator(executionConfig),
                            TestLocalRecoveryConfig.disabled(),
                            new HeapPriorityQueueSetFactory(keyGroupRange, numKeyGroups, 128),
                            true,
                            new CloseableRegistry())
                    .build();
        }

        @Override
        public void close() throws Exception {}

        @Override
        public String toString() {
            return "HEAP";
        }
    }
}
