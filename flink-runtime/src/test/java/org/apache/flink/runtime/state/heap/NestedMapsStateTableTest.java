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

package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.StateSnapshotTransformer;

import org.junit.Test;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.apache.flink.runtime.state.testutils.StateEntryMatcher.entry;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

/** Tests for {@link NestedMapsStateTable}. */
public class NestedMapsStateTableTest {
    @Test
    public void testIteratingOverSnapshot() {
        ListSerializer<Integer> stateSerializer = new ListSerializer<>(IntSerializer.INSTANCE);
        final NestedStateMap<Integer, Integer, List<Integer>> stateMap = new NestedStateMap<>();

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

        StateMapSnapshot<
                        Integer,
                        Integer,
                        List<Integer>,
                        ? extends StateMap<Integer, Integer, List<Integer>>>
                snapshot = stateMap.stateSnapshot();

        Iterator<StateEntry<Integer, Integer, List<Integer>>> iterator =
                snapshot.getIterator(
                        IntSerializer.INSTANCE, IntSerializer.INSTANCE, stateSerializer, null);
        assertThat(
                () -> iterator,
                containsInAnyOrder(
                        entry(1, 1, originalState1),
                        entry(2, 1, originalState2),
                        entry(3, 1, originalState3),
                        entry(4, 1, originalState4),
                        entry(5, 1, originalState5)));
    }

    @Test
    public void testIteratingOverSnapshotWithTransform() {
        final NestedStateMap<Integer, Integer, Long> stateMap = new NestedStateMap<>();

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
        assertThat(
                () -> iterator,
                containsInAnyOrder(
                        entry(1, 1, 12L), entry(2, 1, 13L), entry(4, 1, 15L), entry(5, 1, 16L)));
    }
}
