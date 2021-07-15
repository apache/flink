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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.InputChannelStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.runtime.state.StateObjectID;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.changelog.ChangelogStateBackendHandle.ChangelogStateBackendHandleImpl;
import org.apache.flink.runtime.state.changelog.ChangelogStateHandle;
import org.apache.flink.runtime.state.changelog.ChangelogStateHandleStreamImpl;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;

/** {@link StateObjectIdCollectingVisitor} test. */
public class StateObjectIdCollectingVisitorTest {
    @Test
    public void testVisitSimpleTaskSnapshot() {
        String id = "abc";
        checkIdCollection(singletonList(id), taskStateSnapshot(id));
    }

    @Test
    public void testVisitChangelogStateBackendHandleImpl() {
        KeyGroupRange keyGroupRange = KeyGroupRange.of(0, 5);

        String materializedPrivateStateId = "private";
        String materializedSharedStateId = "shared";
        String materializedMetaStateId = "meta";
        KeyedStateHandle materialized =
                incrementalStateHandle(
                        materializedPrivateStateId,
                        materializedSharedStateId,
                        materializedMetaStateId);

        String nonMaterializedStateId = "nonMaterializedStateId";
        ChangelogStateHandle nonMaterialized =
                new ChangelogStateHandleStreamImpl(
                        singletonList(Tuple2.of(streamStateHandle(nonMaterializedStateId), 0L)),
                        keyGroupRange,
                        0L);

        checkIdCollection(
                asList(
                        nonMaterializedStateId,
                        materializedPrivateStateId,
                        materializedSharedStateId,
                        materializedMetaStateId),
                new ChangelogStateBackendHandleImpl(
                        singletonList(materialized),
                        singletonList(nonMaterialized),
                        keyGroupRange));
    }

    /**
     * Tests that IDs of all states are collected (private, shared, meta) from {@link
     * IncrementalRemoteKeyedStateHandle}.
     */
    @Test
    public void testVisitIncrementalStateHandle() {
        String privateStateId = "private";
        String sharedStateId = "shared";
        String metaStateId = "meta";
        KeyedStateHandle handle =
                incrementalStateHandle(privateStateId, sharedStateId, metaStateId);
        checkIdCollection(
                asList(privateStateId, sharedStateId, metaStateId),
                new TaskStateSnapshot(
                        singletonMap(
                                new OperatorID(),
                                OperatorSubtaskState.builder()
                                        .setManagedKeyedState(handle)
                                        .build())));
    }

    @Test
    public void testVisitingFinishedSnapshot() {
        checkIdCollection(emptyList(), TaskStateSnapshot.FINISHED_ON_RESTORE);
    }

    private void checkIdCollection(List<String> expected, StateObject stateObject) {
        StateObjectIdCollectingVisitor visitor = new StateObjectIdCollectingVisitor();
        stateObject.accept(visitor);
        assertEquals(
                expected.stream().map(StateObjectID::of).collect(Collectors.toSet()),
                visitor.getStateObjectIDs());
    }

    private IncrementalRemoteKeyedStateHandle incrementalStateHandle(
            String privateStateId, String sharedStateId, String metaStateId) {
        return new IncrementalRemoteKeyedStateHandle(
                UUID.randomUUID(),
                KeyGroupRange.EMPTY_KEY_GROUP_RANGE,
                0L,
                singletonMap(StateObjectID.of(privateStateId), streamStateHandle(privateStateId)),
                singletonMap(StateObjectID.of(sharedStateId), streamStateHandle(sharedStateId)),
                streamStateHandle(metaStateId));
    }

    private static ByteStreamStateHandle streamStateHandle(String id) {
        return new ByteStreamStateHandle(id, new byte[0]);
    }

    static TaskStateSnapshot taskStateSnapshot(String... ids) {
        List<InputChannelStateHandle> col =
                Arrays.stream(ids)
                        .map(
                                id -> {
                                    StreamStateHandle streamStateHandle = streamStateHandle(id);
                                    return new InputChannelStateHandle(
                                            0,
                                            new InputChannelInfo(0, 0),
                                            streamStateHandle,
                                            emptyList(),
                                            0L);
                                })
                        .collect(Collectors.toList());
        return new TaskStateSnapshot(
                singletonMap(
                        new OperatorID(),
                        OperatorSubtaskState.builder()
                                .setInputChannelState(new StateObjectCollection<>(col))
                                .build()));
    }
}
