/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.ArrayListSerializer;
import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.StateTransformationFunction;
import org.apache.flink.runtime.state.internal.InternalKvState.StateIncrementalVisitor;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.apache.flink.runtime.state.testutils.StateEntryMatcher.entry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.assertj.core.api.HamcrestCondition.matching;
import static org.hamcrest.Matchers.containsInAnyOrder;

/** Test for {@link CopyOnWriteStateMap}. */
class CopyOnWriteStateMapTest {

    /** Testing the basic map operations. */
    @Test
    void testPutGetRemoveContainsTransform() throws Exception {
        final CopyOnWriteStateMap<Integer, Integer, ArrayList<Integer>> stateMap =
                new CopyOnWriteStateMap<>(new ArrayListSerializer<>(IntSerializer.INSTANCE));

        ArrayList<Integer> state11 = new ArrayList<>();
        state11.add(41);
        ArrayList<Integer> state21 = new ArrayList<>();
        state21.add(42);
        ArrayList<Integer> state12 = new ArrayList<>();
        state12.add(43);

        assertThat(stateMap.putAndGetOld(1, 1, state11)).isNull();
        assertThat(stateMap.get(1, 1)).isEqualTo(state11);
        assertThat(stateMap).hasSize(1);

        assertThat(stateMap.putAndGetOld(2, 1, state21)).isNull();
        assertThat(stateMap.get(2, 1)).isEqualTo(state21);
        assertThat(stateMap).hasSize(2);

        assertThat(stateMap.putAndGetOld(1, 2, state12)).isNull();
        assertThat(stateMap.get(1, 2)).isEqualTo(state12);
        assertThat(stateMap).hasSize(3);

        assertThat(stateMap.containsKey(2, 1)).isTrue();
        assertThat(stateMap.containsKey(3, 1)).isFalse();
        assertThat(stateMap.containsKey(2, 3)).isFalse();
        stateMap.put(2, 1, null);
        assertThat(stateMap.containsKey(2, 1)).isTrue();
        assertThat(stateMap).hasSize(3);
        assertThat(stateMap.get(2, 1)).isNull();
        stateMap.put(2, 1, state21);
        assertThat(stateMap).hasSize(3);

        assertThat(stateMap.removeAndGetOld(2, 1)).isEqualTo(state21);
        assertThat(stateMap.containsKey(2, 1)).isFalse();
        assertThat(stateMap).hasSize(2);

        stateMap.remove(1, 2);
        assertThat(stateMap.containsKey(1, 2)).isFalse();
        assertThat(stateMap).hasSize(1);

        assertThat(stateMap.removeAndGetOld(4, 2)).isNull();
        assertThat(stateMap).hasSize(1);

        StateTransformationFunction<ArrayList<Integer>, Integer> function =
                (previousState, value) -> {
                    previousState.add(value);
                    return previousState;
                };

        final int value = 4711;
        stateMap.transform(1, 1, value, function);
        state11 = function.apply(state11, value);
        assertThat(stateMap.get(1, 1)).isEqualTo(state11);
    }

    /** This test triggers incremental rehash and tests for corruptions. */
    @Test
    void testIncrementalRehash() {
        final CopyOnWriteStateMap<Integer, Integer, ArrayList<Integer>> stateMap =
                new CopyOnWriteStateMap<>(new ArrayListSerializer<>(IntSerializer.INSTANCE));

        int insert = 0;
        int remove = 0;
        while (!stateMap.isRehashing()) {
            stateMap.put(insert++, 0, new ArrayList<>());
            if (insert % 8 == 0) {
                stateMap.remove(remove++, 0);
            }
        }
        assertThat(stateMap).hasSize(insert - remove);
        while (stateMap.isRehashing()) {
            stateMap.put(insert++, 0, new ArrayList<>());
            if (insert % 8 == 0) {
                stateMap.remove(remove++, 0);
            }
        }
        assertThat(stateMap).hasSize(insert - remove);

        for (int i = 0; i < insert; ++i) {
            if (i < remove) {
                assertThat(stateMap.containsKey(i, 0)).isFalse();
            } else {
                assertThat(stateMap.containsKey(i, 0)).isTrue();
            }
        }
    }

    /**
     * This test does some random modifications to a state map and a reference (hash map). Then
     * draws snapshots, performs more modifications and checks snapshot integrity.
     */
    @Test
    void testRandomModificationsAndCopyOnWriteIsolation() throws Exception {
        final CopyOnWriteStateMap<Integer, Integer, ArrayList<Integer>> stateMap =
                new CopyOnWriteStateMap<>(new ArrayListSerializer<>(IntSerializer.INSTANCE));

        final HashMap<Tuple2<Integer, Integer>, ArrayList<Integer>> referenceMap = new HashMap<>();

        final Random random = new Random(42);

        // holds snapshots from the map under test
        CopyOnWriteStateMap.StateMapEntry<Integer, Integer, ArrayList<Integer>>[] snapshot = null;
        int snapshotSize = 0;

        // holds a reference snapshot from our reference map that we compare against
        Tuple3<Integer, Integer, ArrayList<Integer>>[] reference = null;

        int val = 0;

        int snapshotCounter = 0;
        int referencedSnapshotId = 0;

        final StateTransformationFunction<ArrayList<Integer>, Integer> transformationFunction =
                (previousState, value) -> {
                    if (previousState == null) {
                        previousState = new ArrayList<>();
                    }
                    previousState.add(value);
                    // we give back the original, attempting to spot errors in to copy-on-write
                    return previousState;
                };

        StateIncrementalVisitor<Integer, Integer, ArrayList<Integer>> updatingIterator =
                stateMap.getStateIncrementalVisitor(5);

        // the main loop for modifications
        for (int i = 0; i < 10_000_000; ++i) {

            int key = random.nextInt(20);
            int namespace = random.nextInt(4);
            Tuple2<Integer, Integer> compositeKey = new Tuple2<>(key, namespace);

            int op = random.nextInt(10);

            ArrayList<Integer> state = null;
            ArrayList<Integer> referenceState = null;

            switch (op) {
                case 0:
                case 1:
                    {
                        state = stateMap.get(key, namespace);
                        referenceState = referenceMap.get(compositeKey);
                        if (null == state) {
                            state = new ArrayList<>();
                            stateMap.put(key, namespace, state);
                            referenceState = new ArrayList<>();
                            referenceMap.put(compositeKey, referenceState);
                        }
                        break;
                    }
                case 2:
                    {
                        stateMap.put(key, namespace, new ArrayList<>());
                        referenceMap.put(compositeKey, new ArrayList<>());
                        break;
                    }
                case 3:
                    {
                        state = stateMap.putAndGetOld(key, namespace, new ArrayList<>());
                        referenceState = referenceMap.put(compositeKey, new ArrayList<>());
                        break;
                    }
                case 4:
                    {
                        stateMap.remove(key, namespace);
                        referenceMap.remove(compositeKey);
                        break;
                    }
                case 5:
                    {
                        state = stateMap.removeAndGetOld(key, namespace);
                        referenceState = referenceMap.remove(compositeKey);
                        break;
                    }
                case 6:
                    {
                        final int updateValue = random.nextInt(1000);
                        stateMap.transform(key, namespace, updateValue, transformationFunction);
                        referenceMap.put(
                                compositeKey,
                                transformationFunction.apply(
                                        referenceMap.remove(compositeKey), updateValue));
                        break;
                    }
                case 7:
                case 8:
                case 9:
                    if (!updatingIterator.hasNext()) {
                        updatingIterator = stateMap.getStateIncrementalVisitor(5);
                        if (!updatingIterator.hasNext()) {
                            break;
                        }
                    }
                    testStateIteratorWithUpdate(
                            updatingIterator, stateMap, referenceMap, op == 8, op == 9);
                    break;
                default:
                    {
                        fail("Unknown op-code " + op);
                    }
            }

            assertThat(stateMap).hasSize(referenceMap.size());

            if (state != null) {
                assertThat(referenceState).isNotNull();
                // mutate the states a bit...
                if (random.nextBoolean() && !state.isEmpty()) {
                    state.remove(state.size() - 1);
                    referenceState.remove(referenceState.size() - 1);
                } else {
                    state.add(val);
                    referenceState.add(val);
                    ++val;
                }
            }

            assertThat(state).isEqualTo(referenceState);

            // snapshot triggering / comparison / release
            if (i > 0 && i % 500 == 0) {

                if (snapshot != null) {
                    // check our referenced snapshot
                    deepCheck(reference, convert(snapshot, snapshotSize));

                    if (i % 1_000 == 0) {
                        // draw and release some other snapshot while holding on the old snapshot
                        ++snapshotCounter;
                        stateMap.snapshotMapArrays();
                        stateMap.releaseSnapshot(snapshotCounter);
                    }

                    // release the snapshot after some time
                    if (i % 5_000 == 0) {
                        snapshot = null;
                        reference = null;
                        snapshotSize = 0;
                        stateMap.releaseSnapshot(referencedSnapshotId);
                    }

                } else {
                    // if there is no more referenced snapshot, we create one
                    ++snapshotCounter;
                    referencedSnapshotId = snapshotCounter;
                    snapshot = stateMap.snapshotMapArrays();
                    snapshotSize = stateMap.size();
                    reference = manualDeepDump(referenceMap);
                }
            }
        }
    }

    /**
     * Test operations specific for StateIncrementalVisitor in {@code
     * testRandomModificationsAndCopyOnWriteIsolation()}.
     *
     * <p>Check next, update and remove during global iteration of StateIncrementalVisitor.
     */
    private static void testStateIteratorWithUpdate(
            StateIncrementalVisitor<Integer, Integer, ArrayList<Integer>> updatingIterator,
            CopyOnWriteStateMap<Integer, Integer, ArrayList<Integer>> stateMap,
            HashMap<Tuple2<Integer, Integer>, ArrayList<Integer>> referenceMap,
            boolean update,
            boolean remove) {

        for (StateEntry<Integer, Integer, ArrayList<Integer>> stateEntry :
                updatingIterator.nextEntries()) {
            Integer key = stateEntry.getKey();
            Integer namespace = stateEntry.getNamespace();
            Tuple2<Integer, Integer> compositeKey = new Tuple2<>(key, namespace);
            assertThat(stateEntry.getState()).isEqualTo(referenceMap.get(compositeKey));

            if (update) {
                ArrayList<Integer> newState = new ArrayList<>(stateEntry.getState());
                if (!newState.isEmpty()) {
                    newState.remove(0);
                }
                updatingIterator.update(stateEntry, newState);
                referenceMap.put(compositeKey, new ArrayList<>(newState));
                assertThat(stateMap.get(key, namespace)).isEqualTo(newState);
            }

            if (remove) {
                updatingIterator.remove(stateEntry);
                referenceMap.remove(compositeKey);
            }
        }
    }

    /**
     * This tests for the copy-on-write contracts, e.g. ensures that no copy-on-write is active
     * after all snapshots are released.
     */
    @Test
    void testCopyOnWriteContracts() {
        final CopyOnWriteStateMap<Integer, Integer, ArrayList<Integer>> stateMap =
                new CopyOnWriteStateMap<>(new ArrayListSerializer<>(IntSerializer.INSTANCE));

        ArrayList<Integer> originalState1 = new ArrayList<>(1);
        ArrayList<Integer> originalState2 = new ArrayList<>(1);
        ArrayList<Integer> originalState3 = new ArrayList<>(1);
        ArrayList<Integer> originalState4 = new ArrayList<>(1);
        ArrayList<Integer> originalState5 = new ArrayList<>(1);

        originalState1.add(1);
        originalState2.add(2);
        originalState3.add(3);
        originalState4.add(4);
        originalState5.add(5);

        stateMap.put(1, 1, originalState1);
        stateMap.put(2, 1, originalState2);
        stateMap.put(4, 1, originalState4);
        stateMap.put(5, 1, originalState5);

        // no snapshot taken, we get the original back
        assertThat(stateMap.get(1, 1)).isSameAs(originalState1);
        CopyOnWriteStateMapSnapshot<Integer, Integer, ArrayList<Integer>> snapshot1 =
                stateMap.stateSnapshot();
        // after snapshot1 is taken, we get a copy...
        final ArrayList<Integer> copyState = stateMap.get(1, 1);
        assertThat(copyState).isNotSameAs(originalState1);
        // ...and the copy is equal
        assertThat(copyState).isEqualTo(originalState1);

        // we make an insert AFTER snapshot1
        stateMap.put(3, 1, originalState3);

        // on repeated lookups, we get the same copy because no further snapshot was taken
        assertThat(stateMap.get(1, 1)).isSameAs(copyState);

        // we take snapshot2
        CopyOnWriteStateMapSnapshot<Integer, Integer, ArrayList<Integer>> snapshot2 =
                stateMap.stateSnapshot();
        // after the second snapshot, copy-on-write is active again for old entries
        assertThat(stateMap.get(1, 1)).isNotSameAs(copyState);
        // and equality still holds
        assertThat(stateMap.get(1, 1)).isEqualTo(copyState);

        // after releasing snapshot2
        stateMap.releaseSnapshot(snapshot2);
        // we still get the original of the untouched late insert (after snapshot1)
        assertThat(stateMap.get(3, 1)).isSameAs(originalState3);
        // but copy-on-write is still active for older inserts (before snapshot1)
        assertThat(stateMap.get(4, 1)).isNotSameAs(originalState4);

        // after releasing snapshot1
        stateMap.releaseSnapshot(snapshot1);
        // no copy-on-write is active
        assertThat(stateMap.get(5, 1)).isSameAs(originalState5);
    }

    @Test
    void testIteratingOverSnapshot() {
        ListSerializer<Integer> stateSerializer = new ListSerializer<>(IntSerializer.INSTANCE);
        final CopyOnWriteStateMap<Integer, Integer, List<Integer>> stateMap =
                new CopyOnWriteStateMap<>(stateSerializer);

        List<Integer> originalState1 = new ArrayList<>(1);
        List<Integer> originalState2 = new ArrayList<>(1);
        List<Integer> originalState3 = new ArrayList<>(1);
        List<Integer> originalState4 = new ArrayList<>(1);
        List<Integer> originalState5 = new ArrayList<>(1);

        originalState1.add(1);
        originalState2.add(2);
        originalState3.add(3);
        originalState4.add(4);
        originalState5.add(5);

        stateMap.put(1, 1, originalState1);
        stateMap.put(2, 1, originalState2);
        stateMap.put(3, 1, originalState3);
        stateMap.put(4, 1, originalState4);
        stateMap.put(5, 1, originalState5);

        CopyOnWriteStateMapSnapshot<Integer, Integer, List<Integer>> snapshot =
                stateMap.stateSnapshot();

        Iterator<StateEntry<Integer, Integer, List<Integer>>> iterator =
                snapshot.getIterator(
                        IntSerializer.INSTANCE, IntSerializer.INSTANCE, stateSerializer, null);

        assertThat(iterator)
                .toIterable()
                .is(
                        matching(
                                containsInAnyOrder(
                                        entry(1, 1, originalState1),
                                        entry(2, 1, originalState2),
                                        entry(3, 1, originalState3),
                                        entry(4, 1, originalState4),
                                        entry(5, 1, originalState5))));
    }

    @Test
    void testIteratingOverSnapshotWithTransform() {
        final CopyOnWriteStateMap<Integer, Integer, Long> stateMap =
                new CopyOnWriteStateMap<>(LongSerializer.INSTANCE);

        stateMap.put(1, 1, 10L);
        stateMap.put(2, 1, 11L);
        stateMap.put(3, 1, 12L);
        stateMap.put(4, 1, 13L);
        stateMap.put(5, 1, 14L);

        StateMapSnapshot<Integer, Integer, Long, ? extends StateMap<Integer, Integer, Long>>
                snapshot = stateMap.stateSnapshot();

        Iterator<StateEntry<Integer, Integer, Long>> iterator =
                snapshot.getIterator(
                        IntSerializer.INSTANCE,
                        IntSerializer.INSTANCE,
                        LongSerializer.INSTANCE,
                        new StateSnapshotTransformer<Long>() {
                            @Nullable
                            @Override
                            public Long filterOrTransform(@Nullable Long value) {
                                if (value == 12L) {
                                    return null;
                                } else {
                                    return value + 2L;
                                }
                            }
                        });

        assertThat(iterator)
                .toIterable()
                .is(
                        matching(
                                containsInAnyOrder(
                                        entry(1, 1, 12L),
                                        entry(2, 1, 13L),
                                        entry(4, 1, 15L),
                                        entry(5, 1, 16L))));
    }

    /** This tests that snapshot can be released correctly. */
    @Test
    void testSnapshotRelease() {
        final CopyOnWriteStateMap<Integer, Integer, Integer> stateMap =
                new CopyOnWriteStateMap<>(IntSerializer.INSTANCE);

        for (int i = 0; i < 10; i++) {
            stateMap.put(i, i, i);
        }

        CopyOnWriteStateMapSnapshot<Integer, Integer, Integer> snapshot = stateMap.stateSnapshot();
        assertThat(snapshot.isReleased()).isFalse();
        assertThat(stateMap.getSnapshotVersions()).contains(snapshot.getSnapshotVersion());

        snapshot.release();
        assertThat(snapshot.isReleased()).isTrue();
        assertThat(stateMap.getSnapshotVersions()).isEmpty();

        // verify that snapshot will release itself only once
        snapshot.release();
        assertThat(stateMap.getSnapshotVersions()).isEmpty();
    }

    @SuppressWarnings("unchecked")
    private static <K, N, S> Tuple3<K, N, S>[] convert(
            CopyOnWriteStateMap.StateMapEntry<K, N, S>[] snapshot, int mapSize) {

        Tuple3<K, N, S>[] result = new Tuple3[mapSize];
        int pos = 0;
        for (CopyOnWriteStateMap.StateMapEntry<K, N, S> entry : snapshot) {
            while (null != entry) {
                result[pos++] =
                        new Tuple3<>(entry.getKey(), entry.getNamespace(), entry.getState());
                entry = entry.next;
            }
        }
        assertThat(pos).isEqualTo(mapSize);
        return result;
    }

    @SuppressWarnings("unchecked")
    private Tuple3<Integer, Integer, ArrayList<Integer>>[] manualDeepDump(
            HashMap<Tuple2<Integer, Integer>, ArrayList<Integer>> map) {

        Tuple3<Integer, Integer, ArrayList<Integer>>[] result = new Tuple3[map.size()];
        int pos = 0;
        for (Map.Entry<Tuple2<Integer, Integer>, ArrayList<Integer>> entry : map.entrySet()) {
            Integer key = entry.getKey().f0;
            Integer namespace = entry.getKey().f1;
            result[pos++] = new Tuple3<>(key, namespace, new ArrayList<>(entry.getValue()));
        }
        return result;
    }

    private void deepCheck(
            Tuple3<Integer, Integer, ArrayList<Integer>>[] a,
            Tuple3<Integer, Integer, ArrayList<Integer>>[] b) {

        if (a == b) {
            return;
        }

        assertThat(a).hasSameSizeAs(b);

        Comparator<Tuple3<Integer, Integer, ArrayList<Integer>>> comparator =
                (o1, o2) -> {
                    int namespaceDiff = o1.f1 - o2.f1;
                    return namespaceDiff != 0 ? namespaceDiff : o1.f0 - o2.f0;
                };

        Arrays.sort(a, comparator);
        Arrays.sort(b, comparator);

        for (int i = 0; i < a.length; ++i) {
            Tuple3<Integer, Integer, ArrayList<Integer>> av = a[i];
            Tuple3<Integer, Integer, ArrayList<Integer>> bv = b[i];

            assertThat(av.f0).isEqualTo(bv.f0);
            assertThat(av.f1).isEqualTo(bv.f1);
            assertThat(av.f2).isEqualTo(bv.f2);
        }
    }
}
