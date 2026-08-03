/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.changelog;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.UncompressedStreamCompressionDecorator;
import org.apache.flink.runtime.state.changelog.inmemory.InMemoryStateChangelogStorage;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.metrics.SizeTrackingStateConfig;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.state.ttl.mock.MockKeyedStateBackend;
import org.apache.flink.runtime.state.ttl.mock.MockKeyedStateBackend.MockSnapshotSupplier;
import org.apache.flink.runtime.state.ttl.mock.MockKeyedStateBackendBuilder;
import org.apache.flink.state.changelog.ChangelogStateBackendTestUtils.DummyCheckpointingStorageAccess;

import org.junit.jupiter.api.Test;

import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;

class ChangelogKeyedStateBackendMaterializationTest {

    @Test
    void testInitMaterializationAfterAbortedNativeSavepoint() throws Exception {
        final ChangelogKeyedStateBackend<Integer> backend = createChangelogBackend();

        try {
            final long savepointId = 1L;
            backend.snapshot(
                    savepointId,
                    0L,
                    new MemCheckpointStreamFactory(1000),
                    new CheckpointOptions(
                            SavepointType.savepoint(SavepointFormatType.NATIVE),
                            CheckpointStorageLocationReference.getDefault()));
            backend.notifyCheckpointAborted(savepointId);

            backend.getChangelogWriter().append(0, new byte[] {'s'});

            assertThat(backend.initMaterialization()).isPresent();
        } finally {
            backend.close();
            backend.dispose();
        }
    }

    private static ChangelogKeyedStateBackend<Integer> createChangelogBackend() {
        final MockKeyedStateBackend<Integer> delegatedBackend =
                new MockKeyedStateBackendBuilder<>(
                                new KvStateRegistry()
                                        .createTaskRegistry(new JobID(), new JobVertexID()),
                                IntSerializer.INSTANCE,
                                ChangelogKeyedStateBackendMaterializationTest.class.getClassLoader(),
                                1,
                                KeyGroupRange.of(0, 0),
                                new ExecutionConfig(),
                                TtlTimeProvider.DEFAULT,
                                LatencyTrackingStateConfig.disabled(),
                                SizeTrackingStateConfig.disabled(),
                                emptyList(),
                                UncompressedStreamCompressionDecorator.INSTANCE,
                                new CloseableRegistry(),
                                MockSnapshotSupplier.EMPTY)
                        .build();

        return new ChangelogKeyedStateBackend<>(
                delegatedBackend,
                "test",
                new ExecutionConfig(),
                TtlTimeProvider.DEFAULT,
                UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup(),
                new InMemoryStateChangelogStorage()
                        .createWriter("test", KeyGroupRange.EMPTY_KEY_GROUP_RANGE, null),
                emptyList(),
                new DummyCheckpointingStorageAccess());
    }
}
