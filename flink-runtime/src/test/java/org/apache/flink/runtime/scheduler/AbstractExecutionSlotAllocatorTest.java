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

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.runtime.scheduler.ExecutionSlotAllocatorTestUtils.createSchedulingRequirements;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;

/** Tests for {@link AbstractExecutionSlotAllocator}. */
public class AbstractExecutionSlotAllocatorTest extends TestLogger {

    private AbstractExecutionSlotAllocator executionSlotAllocator;

    @Before
    public void setUp() throws Exception {
        executionSlotAllocator = new TestingExecutionSlotAllocator();
    }

    @Test
    public void testCancel() {
        final ExecutionVertexID executionVertexId = new ExecutionVertexID(new JobVertexID(), 0);

        final List<ExecutionVertexSchedulingRequirements> schedulingRequirements =
                createSchedulingRequirements(executionVertexId);
        final List<SlotExecutionVertexAssignment> assignments =
                executionSlotAllocator.allocateSlotsFor(schedulingRequirements);

        executionSlotAllocator.cancel(executionVertexId);

        assertThat(assignments.get(0).getLogicalSlotFuture().isCancelled(), is(true));
    }

    @Test(expected = IllegalStateException.class)
    public void testValidateSchedulingRequirements() {
        final ExecutionVertexID executionVertexId = new ExecutionVertexID(new JobVertexID(), 0);

        final List<ExecutionVertexSchedulingRequirements> schedulingRequirements =
                createSchedulingRequirements(executionVertexId);
        executionSlotAllocator.allocateSlotsFor(schedulingRequirements);

        executionSlotAllocator.validateSchedulingRequirements(schedulingRequirements);
    }

    @Test
    public void testCreateAndRegisterSlotExecutionVertexAssignment() {
        final ExecutionVertexID executionVertexId = new ExecutionVertexID(new JobVertexID(), 0);

        final List<ExecutionVertexSchedulingRequirements> schedulingRequirements =
                createSchedulingRequirements(executionVertexId);
        final List<SlotExecutionVertexAssignment> assignments =
                executionSlotAllocator.allocateSlotsFor(schedulingRequirements);

        assertThat(assignments, hasSize(1));

        final SlotExecutionVertexAssignment assignment = assignments.get(0);
        assertThat(assignment.getExecutionVertexId(), is(executionVertexId));
        assertThat(assignment.getLogicalSlotFuture().isDone(), is(false));
        assertThat(
                executionSlotAllocator.getPendingSlotAssignments().values(), contains(assignment));

        assignment.getLogicalSlotFuture().cancel(false);

        assertThat(executionSlotAllocator.getPendingSlotAssignments().keySet(), hasSize(0));
    }

    @Test
    public void testCompletedExecutionVertexAssignmentWillBeUnregistered() {
        final ExecutionVertexID executionVertexId = new ExecutionVertexID(new JobVertexID(), 0);

        final List<ExecutionVertexSchedulingRequirements> schedulingRequirements =
                createSchedulingRequirements(executionVertexId);
        final List<SlotExecutionVertexAssignment> assignments =
                executionSlotAllocator.allocateSlotsFor(schedulingRequirements);

        assignments.get(0).getLogicalSlotFuture().cancel(false);

        assertThat(executionSlotAllocator.getPendingSlotAssignments().keySet(), hasSize(0));
    }

    @Test
    public void testComputeAllPriorAllocationIds() {
        final List<AllocationID> expectAllocationIds =
                Arrays.asList(new AllocationID(), new AllocationID());
        final List<ExecutionVertexSchedulingRequirements> testSchedulingRequirements =
                Arrays.asList(
                        createSchedulingRequirement(0, expectAllocationIds.get(0)),
                        createSchedulingRequirement(1, expectAllocationIds.get(0)),
                        createSchedulingRequirement(2, expectAllocationIds.get(1)),
                        createSchedulingRequirement(3));

        final Set<AllocationID> allPriorAllocationIds =
                AbstractExecutionSlotAllocator.computeAllPriorAllocationIds(
                        testSchedulingRequirements);
        assertThat(allPriorAllocationIds, containsInAnyOrder(expectAllocationIds.toArray()));
    }

    private ExecutionVertexSchedulingRequirements createSchedulingRequirement(
            final int subtaskIndex) {
        return createSchedulingRequirement(subtaskIndex, null);
    }

    private ExecutionVertexSchedulingRequirements createSchedulingRequirement(
            final int subtaskIndex, @Nullable final AllocationID previousAllocationId) {
        return new ExecutionVertexSchedulingRequirements.Builder()
                .withExecutionVertexId(new ExecutionVertexID(new JobVertexID(), subtaskIndex))
                .withSlotSharingGroupId(new SlotSharingGroupId())
                .withPreviousAllocationId(previousAllocationId)
                .build();
    }

    private static class TestingExecutionSlotAllocator extends AbstractExecutionSlotAllocator {

        TestingExecutionSlotAllocator() {
            super(
                    new DefaultPreferredLocationsRetriever(
                            new TestingStateLocationRetriever(),
                            new TestingInputsLocationsRetriever.Builder().build()));
        }

        @Override
        public List<SlotExecutionVertexAssignment> allocateSlotsFor(
                final List<ExecutionVertexSchedulingRequirements>
                        executionVertexSchedulingRequirements) {

            final List<SlotExecutionVertexAssignment> slotExecutionVertexAssignments =
                    new ArrayList<>(executionVertexSchedulingRequirements.size());

            for (ExecutionVertexSchedulingRequirements schedulingRequirements :
                    executionVertexSchedulingRequirements) {
                slotExecutionVertexAssignments.add(
                        createAndRegisterSlotExecutionVertexAssignment(
                                schedulingRequirements.getExecutionVertexId(),
                                new CompletableFuture<>(),
                                throwable -> {}));
            }

            return slotExecutionVertexAssignments;
        }
    }
}
