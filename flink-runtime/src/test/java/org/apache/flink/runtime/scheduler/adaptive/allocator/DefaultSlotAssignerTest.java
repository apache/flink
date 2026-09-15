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

package org.apache.flink.runtime.scheduler.adaptive.allocator;

import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlot;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Collections.singletonMap;
import static org.apache.flink.runtime.scheduler.adaptive.allocator.DefaultSlotAssigner.APPLICATION_MODE_EXECUTION_TARGET;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link DefaultSlotAssigner}. */
@ExtendWith(ParameterizedTestExtension.class)
class DefaultSlotAssignerTest {

    private static final TaskManagerLocation tml1 = new LocalTaskManagerLocation();
    private static final PhysicalSlot slot1OfTml1 = new TestingSlot(tml1);
    private static final PhysicalSlot slot2OfTml1 = new TestingSlot(tml1);
    private static final PhysicalSlot slot3OfTml1 = new TestingSlot(tml1);
    private static final PhysicalSlot slot4OfTml1 = new TestingSlot(tml1);

    private static final TaskManagerLocation tml2 = new LocalTaskManagerLocation();
    private static final PhysicalSlot slot1OfTml2 = new TestingSlot(tml2);
    private static final PhysicalSlot slot2OfTml2 = new TestingSlot(tml2);
    private static final PhysicalSlot slot3OfTml2 = new TestingSlot(tml2);

    private static final TaskManagerLocation tml3 = new LocalTaskManagerLocation();
    private static final PhysicalSlot slot1OfTml3 = new TestingSlot(tml3);
    private static final PhysicalSlot slot2OfTml3 = new TestingSlot(tml3);

    @Parameter int parallelism;

    @Parameter(value = 1)
    Collection<PhysicalSlot> freeSlots;

    @Parameter(value = 2)
    Map<TaskManagerLocation, Integer> expectedSlotsPerTaskManager;

    @TestTemplate
    void testPickSlotsIfNeeded() {
        final DefaultSlotAssigner slotAssigner =
                new DefaultSlotAssigner(
                        APPLICATION_MODE_EXECUTION_TARGET,
                        true,
                        DefaultSlotSharingResolver.INSTANCE,
                        SimpleSlotMatchingResolver.INSTANCE);

        final Collection<PhysicalSlot> picked =
                slotAssigner.pickSlotsIfNeeded(parallelism, freeSlots);

        // The total number of picked slots must exactly match the requested parallelism. This
        // is the key guarantee against the previous implementation, which could over-pick when
        // a single TaskManager did not satisfy the full request on its own.
        assertThat(picked).hasSize(parallelism);

        // Each picked slot must come from the free-slot pool.
        assertThat(freeSlots).containsAll(picked);

        // The slot count contributed by each TaskManager must exactly match expectations.
        // This validates both: (a) which TaskManagers are involved (minimal set), and (b) how
        // many slots each one contributes (no fragmentation beyond what is unavoidable).
        final Map<TaskManagerLocation, Long> actualPerTaskManager =
                picked.stream()
                        .collect(
                                Collectors.groupingBy(
                                        SlotInfo::getTaskManagerLocation, Collectors.counting()));
        final Map<TaskManagerLocation, Long> expectedPerTaskManager =
                expectedSlotsPerTaskManager.entrySet().stream()
                        .collect(
                                Collectors.toMap(Map.Entry::getKey, e -> e.getValue().longValue()));
        assertThat(actualPerTaskManager).isEqualTo(expectedPerTaskManager);
    }

    @Test
    void testPickSlotsIfNeededReturnsFullFreeSlotsWhenMinimizationBypassed() {
        // When the free-slot pool is not larger than the requested groups, the if condition
        // (freeSlots.size() > requestedGroups) is false, so pickSlotsInMinimalTaskExecutors
        // is bypassed entirely and the full free-slot pool must be returned unchanged.
        final DefaultSlotAssigner slotAssigner =
                new DefaultSlotAssigner(
                        APPLICATION_MODE_EXECUTION_TARGET,
                        true,
                        DefaultSlotSharingResolver.INSTANCE,
                        SimpleSlotMatchingResolver.INSTANCE);

        final Collection<PhysicalSlot> allFreeSlots =
                Arrays.asList(slot1OfTml1, slot2OfTml1, slot1OfTml2);

        // freeSlots.size() == requestedGroups -> the minimization path is skipped.
        final Collection<PhysicalSlot> picked = slotAssigner.pickSlotsIfNeeded(3, allFreeSlots);

        assertThat(picked).isSameAs(allFreeSlots);
    }

    @Parameters(name = "parallelism={0}, freeSlots={1}, expectedSlotsPerTaskManager={2}")
    private static Collection<Object[]> getTestingParameters() {
        return Arrays.asList(
                // -------- Original test cases (kept, with stricter assertions) --------

                // All 4 free slots are needed: every TM contributes all of its free slots.
                new Object[] {
                    4,
                    Arrays.asList(slot1OfTml1, slot2OfTml1, slot1OfTml2, slot2OfTml3),
                    expected(tml1, 2, tml2, 1, tml3, 1)
                },
                // Single largest TM (tml1 with 2 slots) already satisfies the request.
                new Object[] {
                    2,
                    Arrays.asList(slot1OfTml1, slot2OfTml1, slot1OfTml2, slot2OfTml3),
                    singletonMap(tml1, 2)
                },
                // Single TM (tml2 with 3 slots) exactly matches the request; tml1 ignored.
                new Object[] {
                    3,
                    Arrays.asList(slot1OfTml1, slot1OfTml2, slot2OfTml2, slot3OfTml2),
                    singletonMap(tml2, 3)
                },

                // -------- Regression guard: boundary trim --------

                // The largest TM has more slots than requested - only the exact number must be
                // taken. This is the fragmentation guard for the single-TM case.
                new Object[] {
                    1,
                    Arrays.asList(slot1OfTml1, slot2OfTml1, slot3OfTml1, slot4OfTml1),
                    singletonMap(tml1, 1)
                },

                // -------- Regression guard: cumulative case --------

                // tml1(4) cannot satisfy parallelism=5 alone, so tml2 contributes the boundary
                // slot. The previous implementation added ALL of tml2's slots, returning 7 slots
                // (over-pick) and increasing fragmentation. The new implementation limits tml2 to
                // 1 slot.
                new Object[] {
                    5,
                    Arrays.asList(
                            slot1OfTml1,
                            slot2OfTml1,
                            slot3OfTml1,
                            slot4OfTml1,
                            slot1OfTml2,
                            slot2OfTml2,
                            slot3OfTml2),
                    expected(tml1, 4, tml2, 1)
                },

                // -------- Regression guard: three-TM case, smallest TM untouched --------

                // tml1(4) + tml2(3) exactly equals 7, so tml3 must not be touched at all.
                // Sort order is deterministic here because all three TMs have distinct sizes.
                new Object[] {
                    7,
                    Arrays.asList(
                            slot1OfTml1,
                            slot2OfTml1,
                            slot3OfTml1,
                            slot4OfTml1,
                            slot1OfTml2,
                            slot2OfTml2,
                            slot3OfTml2,
                            slot1OfTml3,
                            slot2OfTml3),
                    expected(tml1, 4, tml2, 3)
                },

                // tml1(4) needs 2 more from tml2, tml3 stays untouched. Validates boundary
                // trim happening in the middle of the sorted iteration, not just at the end.
                new Object[] {
                    6,
                    Arrays.asList(
                            slot1OfTml1,
                            slot2OfTml1,
                            slot3OfTml1,
                            slot4OfTml1,
                            slot1OfTml2,
                            slot2OfTml2,
                            slot3OfTml2,
                            slot1OfTml3,
                            slot2OfTml3),
                    expected(tml1, 4, tml2, 2)
                });
    }

    private static Map<TaskManagerLocation, Integer> expected(Object... pairs) {
        final Map<TaskManagerLocation, Integer> map = new HashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            map.put((TaskManagerLocation) pairs[i], (Integer) pairs[i + 1]);
        }
        return map;
    }
}
