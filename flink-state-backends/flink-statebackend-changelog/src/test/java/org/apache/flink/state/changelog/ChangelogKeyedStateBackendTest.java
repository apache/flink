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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.jobgraph.JobVertexID;
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
import org.apache.flink.runtime.state.ttl.mock.MockKeyedStateBackendBuilder;
import org.apache.flink.state.changelog.ChangelogStateBackendTestUtils.DummyCheckpointingStorageAccess;

import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadLocalRandom;

import static java.util.Collections.emptyList;
import static org.apache.flink.runtime.state.ttl.mock.MockKeyedStateBackend.UN_NOTIFIED_CHECKPOINT_ID;
import static org.junit.Assert.assertEquals;

/** {@link ChangelogKeyedStateBackend} test. */
public class ChangelogKeyedStateBackendTest {

    private final List<Long> checkpointIds =
            Arrays.asList(201L, 202L, 203L, 204L, 205L, 206L, 207L, 208L);
    private final List<Long> materializationIds =
            Arrays.asList(UN_NOTIFIED_CHECKPOINT_ID, 1L, 1L, 2L, 2L, 2L, 3L, 3L);

    @Test
    public void testCheckpointsAllCompleted() throws Exception {
        testCheckpointsWithMaterialization(new FakeRandom(true));
    }

    @Test
    public void testCheckpointsAllAborted() throws Exception {
        testCheckpointsWithMaterialization(new FakeRandom(false));
    }

    @Test
    public void testCheckpointsMixed() throws Exception {
        testCheckpointsWithMaterialization(ThreadLocalRandom.current());
    }

    private void testCheckpointsWithMaterialization(Random random) throws Exception {
        MockKeyedStateBackend<Integer> mock = createMock();
        ChangelogKeyedStateBackend<Integer> changelog = createChangelog(mock);
        int rounds = checkpointIds.size();
        try {
            long lastMaterializationId = UN_NOTIFIED_CHECKPOINT_ID;
            NavigableMap<Long, Tuple2<Boolean, Long>> materializationIdByCheckpointId =
                    new TreeMap<>();
            // since we would not notify aborted materialization id smaller than last confirmed
            // success materialization id, we need to record all success
            // materialization ids to identify this.
            Set<Long> successMaterializationIds = new HashSet<>();
            for (int i = 0; i < rounds; i++) {
                long checkpointId = checkpointIds.get(i);
                checkpoint(changelog, checkpointId).get().discardState();
                boolean success = random.nextBoolean();
                materializationIdByCheckpointId.put(
                        checkpointId, Tuple2.of(success, lastMaterializationId));
                if (success) {
                    successMaterializationIds.add(lastMaterializationId);
                    changelog.notifyCheckpointComplete(checkpointId);
                    assertEquals(
                            findLastConfirmedMaterializationId(materializationIdByCheckpointId),
                            mock.getLastCompletedCheckpointID());
                } else {
                    changelog.notifyCheckpointAborted(checkpointId);
                    if (!successMaterializationIds.contains(lastMaterializationId)) {
                        assertEquals(
                                findLastAbortedMaterializationId(materializationIdByCheckpointId),
                                mock.getLastAbortedCheckpointID());
                    }
                }

                long materializationId = materializationIds.get(i);
                if (materializationId != lastMaterializationId) {
                    changelog.updateChangelogSnapshotState(
                            SnapshotResult.empty(),
                            materializationId,
                            SequenceNumber.of(checkpointId));
                    lastMaterializationId = materializationId;
                }
            }
        } finally {
            changelog.close();
            changelog.dispose();
        }
    }

    private long findLastConfirmedMaterializationId(
            NavigableMap<Long, Tuple2<Boolean, Long>> materializationIdByCheckpointId) {
        for (Map.Entry<Long, Tuple2<Boolean, Long>> entry :
                materializationIdByCheckpointId.descendingMap().entrySet()) {
            if (entry.getValue().f0) {
                return entry.getValue().f1;
            }
        }
        return MockKeyedStateBackend.UN_NOTIFIED_CHECKPOINT_ID;
    }

    private long findLastAbortedMaterializationId(
            NavigableMap<Long, Tuple2<Boolean, Long>> materializationIdByCheckpointId) {
        NavigableSet<Long> failedMaterializationIds = new TreeSet<>();
        NavigableSet<Long> successMaterializationIds = new TreeSet<>();
        for (Map.Entry<Long, Tuple2<Boolean, Long>> entry :
                materializationIdByCheckpointId.descendingMap().entrySet()) {
            long materializationId = entry.getValue().f1;
            if (entry.getValue().f0) {
                successMaterializationIds.add(materializationId);
            } else {
                failedMaterializationIds.add(materializationId);
            }
        }
        long lastSuccessMaterializationId =
                successMaterializationIds.isEmpty()
                        ? MockKeyedStateBackend.UN_NOTIFIED_CHECKPOINT_ID
                        : successMaterializationIds.last();
        failedMaterializationIds.removeAll(successMaterializationIds);
        Iterator<Long> iterator = failedMaterializationIds.descendingIterator();
        boolean lastMaterializationAlwaysFailed = true;
        while (iterator.hasNext()) {
            long failedMaterializationId = iterator.next();
            if (lastMaterializationAlwaysFailed
                    && failedMaterializationId > lastSuccessMaterializationId) {
                lastMaterializationAlwaysFailed = false;
                continue;
            }
            return failedMaterializationId;
        }
        return MockKeyedStateBackend.UN_NOTIFIED_CHECKPOINT_ID;
    }

    private MockKeyedStateBackend<Integer> createMock() {
        return new MockKeyedStateBackendBuilder<>(
                        new KvStateRegistry().createTaskRegistry(new JobID(), new JobVertexID()),
                        IntSerializer.INSTANCE,
                        getClass().getClassLoader(),
                        1,
                        KeyGroupRange.EMPTY_KEY_GROUP_RANGE,
                        new ExecutionConfig(),
                        TtlTimeProvider.DEFAULT,
                        LatencyTrackingStateConfig.disabled(),
                        emptyList(),
                        UncompressedStreamCompressionDecorator.INSTANCE,
                        new CloseableRegistry(),
                        true)
                .build();
    }

    private ChangelogKeyedStateBackend<Integer> createChangelog(
            MockKeyedStateBackend<Integer> mock) {
        return new ChangelogKeyedStateBackend<>(
                mock,
                "test",
                new ExecutionConfig(),
                TtlTimeProvider.DEFAULT,
                new InMemoryStateChangelogStorage()
                        .createWriter("test", KeyGroupRange.EMPTY_KEY_GROUP_RANGE),
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

    private static class FakeRandom extends Random {
        private static final long serialVersionUID = 1L;
        private final boolean alwaysBoolean;

        FakeRandom(boolean alwaysBoolean) {
            this.alwaysBoolean = alwaysBoolean;
        }

        @Override
        public boolean nextBoolean() {
            return alwaysBoolean;
        }
    }
}
