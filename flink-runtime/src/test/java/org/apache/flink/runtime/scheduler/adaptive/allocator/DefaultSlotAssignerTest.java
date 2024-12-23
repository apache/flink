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

import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.flink.runtime.scheduler.adaptive.allocator.JobInformation.VertexInformation;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link DefaultSlotAssigner}. */
@ExtendWith(ParameterizedTestExtension.class)
class DefaultSlotAssignerTest {

    private static final TaskManagerLocation tml1 = new LocalTaskManagerLocation();
    private static final SlotInfo slot1OfTml1 = new TestingSlot(tml1);
    private static final SlotInfo slot2OfTml1 = new TestingSlot(tml1);
    private static final SlotInfo slot3OfTml1 = new TestingSlot(tml1);

    private static final TaskManagerLocation tml2 = new LocalTaskManagerLocation();
    private static final SlotInfo slot1OfTml2 = new TestingSlot(tml2);
    private static final SlotInfo slot2OfTml2 = new TestingSlot(tml2);
    private static final SlotInfo slot3OfTml2 = new TestingSlot(tml2);

    private static final TaskManagerLocation tml3 = new LocalTaskManagerLocation();
    private static final SlotInfo slot1OfTml3 = new TestingSlot(tml3);
    private static final SlotInfo slot2OfTml3 = new TestingSlot(tml3);

    private static final List<SlotInfo> allSlots =
            Arrays.asList(
                    slot1OfTml1,
                    slot2OfTml1,
                    slot3OfTml1,
                    slot1OfTml2,
                    slot2OfTml2,
                    slot3OfTml2,
                    slot1OfTml3,
                    slot2OfTml3);

    private static final JobVertex jobVertex = new JobVertex("testingJobVertex");
    private static final SlotSharingGroup slotSharingGroup = new SlotSharingGroup();

    @Parameter int parallelism;

    @Parameter(value = 1)
    Collection<? extends SlotInfo> freeSlots;

    @Parameter(value = 2)
    List<TaskManagerLocation> minimalTaskExecutors;

    @TestTemplate
    void testAssignSlots() {
        final SlotAssigner slotAssigner = new DefaultSlotAssigner();
        final VertexInformation vertexInfo =
                new TestVertexInformation(jobVertex.getID(), parallelism, slotSharingGroup);
        final VertexParallelism vertexParallel =
                new VertexParallelism(
                        singletonMap(vertexInfo.getJobVertexID(), vertexInfo.getParallelism()));
        final JobInformation jobInformation = new TestJobInformation(singletonList(vertexInfo));
        final Set<TaskManagerLocation> keptTaskExecutors =
                slotAssigner.assignSlots(jobInformation, freeSlots, vertexParallel, null).stream()
                        .map(assignment -> assignment.getSlotInfo().getTaskManagerLocation())
                        .collect(Collectors.toSet());
        assertThat(minimalTaskExecutors).containsExactlyInAnyOrderElementsOf(keptTaskExecutors);
    }

    @Parameters(name = "parallelism={0}, freeSlots={1}, minimalTaskExecutors={2}")
    private static Collection<Object[]> getTestingParameters() {
        return Arrays.asList(
                new Object[] {
                    4,
                    Arrays.asList(slot1OfTml1, slot2OfTml1, slot1OfTml2, slot2OfTml3),
                    Arrays.asList(tml1, tml2, tml3)
                },
                new Object[] {2, allSlots, singletonList(tml3)},
                new Object[] {
                    3,
                    Arrays.asList(slot1OfTml1, slot1OfTml2, slot2OfTml2, slot3OfTml2),
                    Arrays.asList(tml1, tml2)
                },
                new Object[] {7, allSlots, Arrays.asList(tml1, tml2, tml3)});
    }
}
