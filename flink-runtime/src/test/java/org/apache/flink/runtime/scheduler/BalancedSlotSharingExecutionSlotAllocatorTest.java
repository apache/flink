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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.util.LogLevelExtension;
import org.apache.flink.util.Preconditions;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createRandomExecutionVertexId;
import static org.apache.flink.runtime.scheduler.SlotSharingExecutionSlotAllocatorTest.AllocationContext;
import static org.assertj.core.api.Assertions.assertThat;

/** Test suite for {@link BalancedSlotSharingExecutionSlotAllocator}. */
@ExtendWith(LogLevelExtension.class)
class BalancedSlotSharingExecutionSlotAllocatorTest {

    private ExecutionVertexID[] createExecuitonVertexIDs(
            int number, List<ExecutionVertexID> collector) {
        Preconditions.checkState(number > 0);
        ExecutionVertexID[] executionVertexIDs = new ExecutionVertexID[number];
        Arrays.setAll(
                executionVertexIDs,
                new IntFunction<ExecutionVertexID>() {
                    @Override
                    public ExecutionVertexID apply(int value) {
                        ExecutionVertexID executionVertexId = createRandomExecutionVertexId();
                        collector.add(executionVertexId);
                        return executionVertexId;
                    }
                });
        return executionVertexIDs;
    }

    @Test
    void testAllocatePhysicalSlotForNewSharedSlot() {
        SlotSharingGroup ssg1 = new SlotSharingGroup();
        SlotSharingGroup ssg2 = new SlotSharingGroup();
        List<ExecutionVertexID> collector = new ArrayList<>();

        AllocationContext context =
                AllocationContext.newBuilder()
                        .withAllocatorSupplier(BalancedSlotSharingExecutionSlotAllocator::new)
                        .addGroupAndResource(ssg1, createExecuitonVertexIDs(2, collector))
                        .addGroupAndResource(ssg1, createExecuitonVertexIDs(2, collector))
                        .addGroupAndResource(ssg1, createExecuitonVertexIDs(3, collector))
                        .addGroupAndResource(ssg2, createExecuitonVertexIDs(2, collector))
                        .addGroupAndResource(ssg2, createExecuitonVertexIDs(2, collector))
                        .addGroupAndResource(ssg2, createExecuitonVertexIDs(3, collector))
                        .build();

        Map<ExecutionSlotSharingGroup, List<ExecutionVertexID>> sortedMap =
                context.getAllocator().groupByExecutionSlotSharingGroup(collector);

        assertThat(sortedMap.values().stream().map(List::size).collect(Collectors.toList()))
                .containsExactly(3, 2, 2, 3, 2, 2);
    }
}
