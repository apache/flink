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

package org.apache.flink.state.hashmap;

import org.apache.flink.runtime.state.IncrementalKeyedStateHandle;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;
import java.util.function.Function;

import static java.util.Arrays.stream;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertEquals;

/** {@link StateHandleHelper} test. */
public class StateHandleHelperTest {
    private static final UUID BACKEND_ID = UUID.randomUUID();
    private static final KeyGroupRangeOffsets OFFSETS = new KeyGroupRangeOffsets(0, 1);
    private static final KeyGroupRange KEY_GROUP_RANGE = KeyGroupRange.EMPTY_KEY_GROUP_RANGE;
    private static int stateCounter = 1;
    private static final KeyGroupsStateHandle STATE_1 =
            new KeyGroupsStateHandle(OFFSETS, streamStateHandle());
    private static final KeyGroupsStateHandle STATE_2 =
            new KeyGroupsStateHandle(OFFSETS, streamStateHandle());
    private static final KeyGroupsStateHandle STATE_3 =
            new KeyGroupsStateHandle(OFFSETS, streamStateHandle());

    @Test
    public void testFlattenState() {
        assertThat(
                new StateHandleHelper(BACKEND_ID, KEY_GROUP_RANGE)
                        .flattenForRecovery(buildIncremental(1L, STATE_1, STATE_2, STATE_3)),
                containsInAnyOrder(STATE_1, STATE_2, STATE_3));
    }

    @Test
    public void testCombineContainsAllState() {
        final long cp1 = 1L, cp2 = 2L;
        SnapshotResult<KeyedStateHandle> combineResult =
                new StateHandleHelper(BACKEND_ID, KEY_GROUP_RANGE)
                        .combine(
                                SnapshotResult.of(buildIncremental(cp1, STATE_1, STATE_2)),
                                cp1,
                                SnapshotResult.of(STATE_3),
                                cp2);
        //noinspection ConstantConditions
        Collection<StreamStateHandle> stateHandles =
                ((IncrementalKeyedStateHandle) combineResult.getJobManagerOwnedSnapshot())
                        .getSharedStateHandles()
                        .values();
        assertThat(stateHandles, containsInAnyOrder(STATE_1, STATE_2, STATE_3));
    }

    @Test
    public void testHandleOrder() {
        Collection<KeyGroupsStateHandle> expected = new ArrayList<>();

        StateHandleHelper helper = new StateHandleHelper(BACKEND_ID, KEY_GROUP_RANGE);
        SnapshotResult<KeyedStateHandle> state =
                SnapshotResult.of(helper.init(singletonList(STATE_1)));
        expected.add(STATE_1);

        for (int cpId = 1; cpId <= 10; cpId++) {
            KeyGroupsStateHandle handle = new KeyGroupsStateHandle(OFFSETS, streamStateHandle());
            state = helper.combine(state, cpId - 1, SnapshotResult.of(handle), cpId);
            expected.add(handle);
        }

        assertEquals(
                expected,
                helper.flattenForRecovery(
                        (IncrementalKeyedStateHandle) state.getJobManagerOwnedSnapshot()));
    }

    private static ByteStreamStateHandle streamStateHandle() {
        return new ByteStreamStateHandle(Integer.toString(stateCounter++), new byte[0]);
    }

    private static IncrementalRemoteKeyedStateHandle buildIncremental(
            long checkpointID, KeyGroupsStateHandle... states) {
        return new IncrementalRemoteKeyedStateHandle(
                BACKEND_ID,
                StateHandleHelperTest.KEY_GROUP_RANGE,
                checkpointID,
                stream(states)
                        .collect(
                                toMap(KeyGroupsStateHandle::getStateHandleId, Function.identity())),
                emptyMap(),
                streamStateHandle());
    }
}
