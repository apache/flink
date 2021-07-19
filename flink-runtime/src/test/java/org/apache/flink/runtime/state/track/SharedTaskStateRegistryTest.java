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

package org.apache.flink.runtime.state.track;

import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.runtime.state.StateUtilTest.TestStateObject;
import org.apache.flink.runtime.state.track.SharedTaskStateRegistry.StateObjectIDExtractor;
import org.apache.flink.util.function.BiConsumerWithException;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;
import static org.apache.flink.runtime.state.track.SharedTaskStateRegistry.StateObjectIDExtractor.IDENTITY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** {@link SharedTaskStateRegistry} test. */
public class SharedTaskStateRegistryTest {
    private static final String BACKEND_ID_1 = "b1";
    private static final String BACKEND_ID_2 = "b2";
    private static final long CP_ID_1 = 1L;
    private static final long CP_ID_2 = 2L;
    private static final long CP_ID_3 = 3L;
    private static final StateObject STATE_OBJECT_1 = new TestStateObject("s1", 1);
    private static final StateObject STATE_OBJECT_2 = new TestStateObject("s2", 1);

    @Test
    public void testIgnoreUnknownObject() throws Exception {
        runWith(
                (registry, cleaner) -> {
                    register(STATE_OBJECT_1, registry, BACKEND_ID_1);
                    registry.stateNotUsed(BACKEND_ID_1, STATE_OBJECT_2);
                });
    }

    @Test
    public void testIgnoreUnknownCheckpointOnSubsumption() throws Exception {
        runWith(
                (registry, cleaner) -> {
                    registry.checkpointSubsumed(BACKEND_ID_1, CP_ID_1);
                    register(STATE_OBJECT_1, registry, BACKEND_ID_1);
                    registry.checkpointStarting(BACKEND_ID_1, CP_ID_2);
                    registry.stateSnapshotCreated(BACKEND_ID_1, STATE_OBJECT_1, CP_ID_2);
                    registry.checkpointSubsumed(BACKEND_ID_1, CP_ID_2);
                    registry.checkpointSubsumed(BACKEND_ID_1, CP_ID_2);
                });
    }

    @Test
    public void testIgnoreDistributedState() throws Exception {
        runWith(
                (registry, cleaner) -> {
                    registry.addDistributedState(
                            new HashSet<>(asList(STATE_OBJECT_1, STATE_OBJECT_2)));

                    register(STATE_OBJECT_1, registry, BACKEND_ID_1);
                    registry.stateNotUsed(BACKEND_ID_1, STATE_OBJECT_1);

                    checkpoint(BACKEND_ID_1, CP_ID_1, STATE_OBJECT_2, registry);
                    registry.checkpointAborted(BACKEND_ID_1, CP_ID_1);
                    registry.checkpointSubsumed(BACKEND_ID_1, CP_ID_1);
                    registry.stateNotUsed(BACKEND_ID_1, STATE_OBJECT_2);
                });
    }

    @Test
    public void testDiscardWhenUnregistered() throws Exception {
        runWith(
                (registry, cleaner) -> {
                    register(STATE_OBJECT_1, registry, BACKEND_ID_1);
                    registry.stateNotUsed(BACKEND_ID_1, STATE_OBJECT_1);
                },
                STATE_OBJECT_1);
    }

    @Test
    public void testDiscardWhenUnusedImplicitly() throws Exception {
        runWith(
                (registry, cleaner) -> {
                    // cp1 uses state1
                    // cp2 doesn't; but this is not known at the time when it starts
                    // cp3 starts after cp2 snapshot is reported
                    // so cp3 is guaranteed to not use state1
                    // therefore, state1 can discarded after subsuming cp2

                    registry.checkpointStarting(BACKEND_ID_1, CP_ID_1);
                    registry.checkpointStarting(BACKEND_ID_1, CP_ID_2);
                    register(STATE_OBJECT_1, registry, BACKEND_ID_1);
                    registry.stateSnapshotCreated(BACKEND_ID_1, STATE_OBJECT_1, CP_ID_1);
                    register(STATE_OBJECT_2, registry, BACKEND_ID_1);
                    registry.stateSnapshotCreated(BACKEND_ID_1, STATE_OBJECT_2, CP_ID_2);

                    registry.checkpointStarting(BACKEND_ID_1, CP_ID_3);
                    registry.stateSnapshotCreated(BACKEND_ID_1, STATE_OBJECT_2, CP_ID_3);
                    registry.checkpointSubsumed(BACKEND_ID_1, CP_ID_1);
                },
                STATE_OBJECT_1);
    }

    @Test
    public void testDiscardWhenSubsumed() throws Exception {
        runWith(
                (registry, cleaner) -> {
                    checkpoint(BACKEND_ID_1, CP_ID_1, STATE_OBJECT_1, registry);
                    registry.stateNotUsed(BACKEND_ID_1, STATE_OBJECT_1);
                    registry.checkpointSubsumed(BACKEND_ID_1, CP_ID_1);
                },
                STATE_OBJECT_1);
        runWith(
                (registry, cleaner) -> {
                    checkpoint(BACKEND_ID_1, CP_ID_1, STATE_OBJECT_1, registry);
                    registry.checkpointSubsumed(BACKEND_ID_1, CP_ID_1);
                    registry.stateNotUsed(BACKEND_ID_1, STATE_OBJECT_1);
                },
                STATE_OBJECT_1);
    }

    @Test
    public void testDiscardWhenLaterCheckpointSubsumed() throws Exception {
        int numCheckpoints = 10;
        StateObject[] checkpointStates = new StateObject[numCheckpoints];
        for (int i = 0; i < numCheckpoints; i++) {
            checkpointStates[i] = new TestStateObject(Integer.toString(i), 1);
        }
        runWith(
                (registry, cleaner) -> {
                    for (int i = 0; i < numCheckpoints; i++) {
                        checkpoint(BACKEND_ID_1, i, checkpointStates[i], registry);
                        registry.stateNotUsed(BACKEND_ID_1, checkpointStates[i]);
                    }
                    registry.checkpointSubsumed(BACKEND_ID_1, numCheckpoints - 1);
                },
                checkpointStates);
    }

    @Test
    public void testDiscardWithMultipleBackends() throws Exception {
        runWith(
                (registry, cleaner) -> {
                    register(STATE_OBJECT_1, registry, BACKEND_ID_1, BACKEND_ID_2);
                    registry.stateNotUsed(BACKEND_ID_1, STATE_OBJECT_1);
                    assertNotDiscarded(cleaner);
                    registry.stateNotUsed(BACKEND_ID_2, STATE_OBJECT_1);
                },
                STATE_OBJECT_1);
    }

    @Test
    public void testDiscardWhenCheckpointAborted() throws Exception {
        runWith(
                (registry, cleaner) -> {
                    register(STATE_OBJECT_1, registry, BACKEND_ID_1);
                    registry.checkpointStarting(BACKEND_ID_1, CP_ID_1);
                    registry.checkpointAborted(BACKEND_ID_1, CP_ID_1);
                    assertNotDiscarded(cleaner);
                    registry.stateNotUsed(BACKEND_ID_1, STATE_OBJECT_1);
                },
                STATE_OBJECT_1);
    }

    @Test
    public void testDiscardOnClose() throws Exception {
        runWith(
                (registry, cleaner) -> {
                    register(STATE_OBJECT_1, registry, BACKEND_ID_1);
                    // starting and not subsuming any checkpoint at any point prevents deletion as
                    // such a checkpoint should be treated as potentially completed
                },
                IDENTITY,
                AssertionPoint.AFTER_CLOSE,
                STATE_OBJECT_1);
    }

    @Test
    public void testDiscardOnCloseSubsumed() throws Exception {
        runWith(
                (registry, cleaner) -> {
                    registry.checkpointStarting(BACKEND_ID_1, CP_ID_1);
                    register(STATE_OBJECT_1, registry, BACKEND_ID_1);
                    registry.checkpointSubsumed(BACKEND_ID_1, CP_ID_1);
                },
                IDENTITY,
                AssertionPoint.AFTER_CLOSE,
                STATE_OBJECT_1);
    }

    @Test
    public void testNoDiscardIfStillUsed() throws Exception {
        runWith(
                (registry, cleaner) -> {
                    checkpoint(BACKEND_ID_1, CP_ID_1, STATE_OBJECT_1, registry);
                    registry.checkpointSubsumed(BACKEND_ID_1, CP_ID_1);
                });
    }

    @Test
    public void testNoDiscardIfWasCheckpointed() throws Exception {
        runWith(
                (registry, cleaner) -> {
                    checkpoint(BACKEND_ID_1, CP_ID_1, STATE_OBJECT_1, registry);
                    registry.stateNotUsed(BACKEND_ID_1, STATE_OBJECT_1);
                });
    }

    @Test
    public void testNoDiscardIfCheckpointNotSubsumed() throws Exception {
        runWith(
                (registry, cleaner) -> {
                    checkpoint(BACKEND_ID_1, CP_ID_1, STATE_OBJECT_1, registry);
                    registry.checkpointStarting(BACKEND_ID_1, CP_ID_2);
                    registry.stateSnapshotCreated(BACKEND_ID_1, STATE_OBJECT_1, CP_ID_2);
                    registry.checkpointSubsumed(BACKEND_ID_1, CP_ID_1);
                    registry.stateNotUsed(BACKEND_ID_1, STATE_OBJECT_1);
                });
    }

    @Test
    public void testNoDiscardOnClose() throws Exception {
        runWith(
                (registry, cleaner) -> {
                    registry.stateUsed(singleton(BACKEND_ID_1), singletonList(STATE_OBJECT_1));
                    registry.checkpointStarting(BACKEND_ID_1, CP_ID_1);
                },
                IDENTITY,
                AssertionPoint.AFTER_CLOSE);
    }

    @Test(expected = IllegalStateException.class)
    public void testRequiresStart() throws Exception {
        runWith(
                (registry, cleaner) ->
                        registry.stateSnapshotCreated(BACKEND_ID_1, STATE_OBJECT_1, CP_ID_1));
    }

    @Test(expected = IllegalStateException.class)
    public void testNoDoubleStart() throws Exception {
        runWith(
                (registry, cleaner) -> {
                    registry.checkpointStarting(BACKEND_ID_1, CP_ID_1);
                    registry.checkpointStarting(BACKEND_ID_1, CP_ID_1);
                });
    }

    @Test(expected = IllegalStateException.class)
    public void testNoDoubleSnapshot() throws Exception {
        runWith(
                (registry, cleaner) -> {
                    registry.checkpointStarting(BACKEND_ID_1, CP_ID_1);
                    checkpoint(BACKEND_ID_1, CP_ID_1, STATE_OBJECT_1, registry);
                    registry.stateSnapshotCreated(BACKEND_ID_1, STATE_OBJECT_1, CP_ID_1);
                });
    }

    @Test
    public void testPendingCheckpointPreventsDiscard() throws Exception {
        runWith(
                (registry, cleaner) -> {
                    register(STATE_OBJECT_1, registry, BACKEND_ID_1, BACKEND_ID_2);
                    registry.checkpointStarting(BACKEND_ID_2, CP_ID_1);
                    registry.stateNotUsed(BACKEND_ID_1, STATE_OBJECT_1);
                });
    }

    @Test
    public void testDiscardWhenPendingCheckpointSubsumed() throws Exception {
        runWith(
                (registry, cleaner) -> {
                    register(STATE_OBJECT_1, registry, BACKEND_ID_1);
                    registry.checkpointStarting(BACKEND_ID_2, CP_ID_1);
                    registry.stateNotUsed(BACKEND_ID_1, STATE_OBJECT_1);
                    registry.checkpointSubsumed(BACKEND_ID_2, CP_ID_1);
                },
                STATE_OBJECT_1);
    }

    @Test
    public void testOutOfOrder() throws Exception {
        runWith(
                (registry, cleaner) -> {
                    checkpoint(BACKEND_ID_1, CP_ID_1, STATE_OBJECT_1, registry);
                    registry.stateNotUsed(BACKEND_ID_1, STATE_OBJECT_1);
                    registry.checkpointStarting(BACKEND_ID_1, CP_ID_2);
                    registry.stateSnapshotCreated(BACKEND_ID_1, STATE_OBJECT_1, CP_ID_2);
                });
    }

    private void runWith(
            BiConsumerWithException<
                            SharedTaskStateRegistry<StateObject>, TestTaskStateCleaner, Exception>
                    test,
            StateObject... expectDiscarded)
            throws Exception {
        runWith(test, IDENTITY, AssertionPoint.BEFORE_CLOSE, expectDiscarded);
    }

    private <T> void runWith(
            BiConsumerWithException<SharedTaskStateRegistry<T>, TestTaskStateCleaner, Exception>
                    test,
            StateObjectIDExtractor<T> idExtractor,
            AssertionPoint assertionPoint,
            StateObject... expectDiscarded)
            throws Exception {
        HashSet<StateObject> actual = null;
        try (TestTaskStateCleaner cleaner = new TestTaskStateCleaner()) {
            try (SharedTaskStateRegistry<T> registry =
                    new SharedTaskStateRegistry<>(cleaner, idExtractor)) {
                test.accept(registry, cleaner);
                if (assertionPoint == AssertionPoint.BEFORE_CLOSE) {
                    actual = new HashSet<>(cleaner.getDiscarded());
                }
            }
            if (assertionPoint == AssertionPoint.AFTER_CLOSE) {
                actual = new HashSet<>(cleaner.getDiscarded());
            }
        }
        assertEquals("Discarded state differs", new HashSet<>(asList(expectDiscarded)), actual);
    }

    private static class TestTaskStateCleaner implements TaskStateCleaner {
        private final List<StateObject> discarded = new ArrayList<>();

        @Override
        public void discardAsync(StateObject state) {
            discarded.add(state);
        }

        public List<StateObject> getDiscarded() {
            return unmodifiableList(discarded);
        }

        @Override
        public void close() {}
    }

    private void checkpoint(
            String backendId,
            long checkpointId,
            StateObject state,
            SharedTaskStateRegistry<StateObject> registry) {
        register(state, registry, backendId);
        registry.checkpointStarting(backendId, checkpointId);
        registry.stateSnapshotCreated(backendId, state, checkpointId);
    }

    private void register(
            StateObject state,
            SharedTaskStateRegistry<StateObject> registry,
            String... backendIds) {
        registry.stateUsed(new HashSet<>(asList(backendIds)), singletonList(state));
    }

    private void assertNotDiscarded(TestTaskStateCleaner cleaner) {
        assertTrue(cleaner.getDiscarded().isEmpty());
    }

    private enum AssertionPoint {
        BEFORE_CLOSE,
        AFTER_CLOSE
    }
}
