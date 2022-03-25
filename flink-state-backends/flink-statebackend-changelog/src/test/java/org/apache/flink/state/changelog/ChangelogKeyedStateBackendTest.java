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

package org.apache.flink.state.changelog;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.UncompressedStreamCompressionDecorator;
import org.apache.flink.runtime.state.changelog.SequenceNumber;
import org.apache.flink.runtime.state.changelog.inmemory.InMemoryStateChangelogStorage;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.state.ttl.mock.MockKeyedStateBackend;
import org.apache.flink.runtime.state.ttl.mock.MockKeyedStateBackend.MockSnapshotSupplier;
import org.apache.flink.runtime.state.ttl.mock.MockKeyedStateBackendBuilder;
import org.apache.flink.state.changelog.ChangelogStateBackendTestUtils.DummyCheckpointingStorageAccess;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;

import java.util.concurrent.RunnableFuture;

import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;

/** {@link ChangelogKeyedStateBackend} test. */
@RunWith(Parameterized.class)
public class ChangelogKeyedStateBackendTest {

    @Parameterized.Parameters(name = "checkpointID={0}, materializationId={1}")
    public static Object[][] parameters() {
        return new Object[][] {
            {0L, 200L},
            {200L, 0L},
        };
    }

    @Parameter(0)
    public long checkpointId;

    @Parameter(1)
    public long materializationId;

    @Test
    public void testCheckpointConfirmation() throws Exception {
        MockKeyedStateBackend<Integer> mock = createMock();
        ChangelogKeyedStateBackend<Integer> changelog = createChangelog(mock);
        try {
            changelog.handleMaterializationResult(
                    SnapshotResult.empty(), materializationId, SequenceNumber.of(Long.MAX_VALUE));
            checkpoint(changelog, checkpointId).get().discardState();

            changelog.notifyCheckpointComplete(checkpointId);
            assertEquals(materializationId, mock.getLastCompletedCheckpointID());

        } finally {
            changelog.close();
            changelog.dispose();
        }
    }

    private MockKeyedStateBackend<Integer> createMock() {
        return new MockKeyedStateBackendBuilder<>(
                        new KvStateRegistry().createTaskRegistry(new JobID(), new JobVertexID()),
                        IntSerializer.INSTANCE,
                        getClass().getClassLoader(),
                        1,
                        KeyGroupRange.of(0, 0),
                        new ExecutionConfig(),
                        TtlTimeProvider.DEFAULT,
                        LatencyTrackingStateConfig.disabled(),
                        emptyList(),
                        UncompressedStreamCompressionDecorator.INSTANCE,
                        new CloseableRegistry(),
                        MockSnapshotSupplier.EMPTY)
                .build();
    }

    private ChangelogKeyedStateBackend<Integer> createChangelog(
            MockKeyedStateBackend<Integer> mock) {
        return new ChangelogKeyedStateBackend<>(
                mock,
                "test",
                new ExecutionConfig(),
                TtlTimeProvider.DEFAULT,
                new ChangelogStateBackendMetricGroup(
                        UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup()),
                new InMemoryStateChangelogStorage()
                        .createWriter("test", KeyGroupRange.EMPTY_KEY_GROUP_RANGE, null),
                emptyList(),
                new DummyCheckpointingStorageAccess());
    }

    private RunnableFuture<SnapshotResult<KeyedStateHandle>> checkpoint(
            ChangelogKeyedStateBackend<Integer> backend, long checkpointId) throws Exception {
        return backend.snapshot(
                checkpointId,
                0L,
                new MemCheckpointStreamFactory(1000),
                CheckpointOptions.forCheckpointWithDefaultLocation());
    }
}
