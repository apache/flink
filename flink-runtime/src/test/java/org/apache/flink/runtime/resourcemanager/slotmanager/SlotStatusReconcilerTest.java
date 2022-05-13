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

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.util.TestLogger;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;

import javax.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.Queue;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.junit.Assert.assertThat;

/**
 * Tests for the {@link DefaultSlotTracker.SlotStatusStateReconciler}. Tests all state transitions
 * that could (or should not) occur due to a slot status update. This test only checks the target
 * state and job ID for state transitions, because the slot ID is not interesting and the slot state
 * is not *actually* being updated. We assume the reconciler locks in a set of transitions given a
 * source and target state, without worrying about the correctness of intermediate steps (because it
 * shouldn't; and it would be a bit annoying to setup).
 */
public class SlotStatusReconcilerTest extends TestLogger {

    private static final TaskExecutorConnection TASK_EXECUTOR_CONNECTION =
            new TaskExecutorConnection(
                    ResourceID.generate(),
                    new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway());

    @Test
    public void testSlotStatusReconciliationForFreeSlot() {
        JobID jobId1 = new JobID();
        StateTransitionTracker stateTransitionTracker = new StateTransitionTracker();

        DefaultSlotTracker.SlotStatusStateReconciler reconciler =
                createSlotStatusReconciler(stateTransitionTracker);

        DeclarativeTaskManagerSlot slot =
                new DeclarativeTaskManagerSlot(
                        new SlotID(ResourceID.generate(), 0),
                        ResourceProfile.ANY,
                        TASK_EXECUTOR_CONNECTION);

        // free -> free
        assertThat(reconciler.executeStateTransition(slot, null), is(false));
        assertThat(stateTransitionTracker.stateTransitions, empty());

        // free -> allocated
        assertThat(reconciler.executeStateTransition(slot, jobId1), is(true));
        assertThat(
                stateTransitionTracker.stateTransitions.remove(),
                is(transitionWithTargetStateForJob(SlotState.PENDING, jobId1)));
        assertThat(
                stateTransitionTracker.stateTransitions.remove(),
                is(transitionWithTargetStateForJob(SlotState.ALLOCATED, jobId1)));
    }

    @Test
    public void testSlotStatusReconciliationForPendingSlot() {
        JobID jobId1 = new JobID();
        StateTransitionTracker stateTransitionTracker = new StateTransitionTracker();

        DefaultSlotTracker.SlotStatusStateReconciler reconciler =
                createSlotStatusReconciler(stateTransitionTracker);

        DeclarativeTaskManagerSlot slot =
                new DeclarativeTaskManagerSlot(
                        new SlotID(ResourceID.generate(), 0),
                        ResourceProfile.ANY,
                        TASK_EXECUTOR_CONNECTION);
        slot.startAllocation(jobId1);

        // pending vs. free; should not trigger any transition because we are expecting a slot
        // allocation in the future
        assertThat(reconciler.executeStateTransition(slot, null), is(false));
        assertThat(stateTransitionTracker.stateTransitions, empty());

        // pending -> allocated
        assertThat(reconciler.executeStateTransition(slot, jobId1), is(true));
        assertThat(
                stateTransitionTracker.stateTransitions.remove(),
                is(transitionWithTargetStateForJob(SlotState.ALLOCATED, jobId1)));
    }

    @Test
    public void testSlotStatusReconciliationForPendingSlotWithDifferentJobID() {
        JobID jobId1 = new JobID();
        JobID jobId2 = new JobID();
        StateTransitionTracker stateTransitionTracker = new StateTransitionTracker();

        DefaultSlotTracker.SlotStatusStateReconciler reconciler =
                createSlotStatusReconciler(stateTransitionTracker);

        DeclarativeTaskManagerSlot slot =
                new DeclarativeTaskManagerSlot(
                        new SlotID(ResourceID.generate(), 0),
                        ResourceProfile.ANY,
                        TASK_EXECUTOR_CONNECTION);
        slot.startAllocation(jobId1);

        // pending(job1) -> free -> pending(job2) -> allocated(job2)
        assertThat(reconciler.executeStateTransition(slot, jobId2), is(true));
        assertThat(
                stateTransitionTracker.stateTransitions.remove(),
                is(transitionWithTargetStateForJob(SlotState.FREE, jobId1)));
        assertThat(
                stateTransitionTracker.stateTransitions.remove(),
                is(transitionWithTargetStateForJob(SlotState.PENDING, jobId2)));
        assertThat(
                stateTransitionTracker.stateTransitions.remove(),
                is(transitionWithTargetStateForJob(SlotState.ALLOCATED, jobId2)));
    }

    @Test
    public void testSlotStatusReconciliationForAllocatedSlot() {
        JobID jobId1 = new JobID();
        StateTransitionTracker stateTransitionTracker = new StateTransitionTracker();

        DefaultSlotTracker.SlotStatusStateReconciler reconciler =
                createSlotStatusReconciler(stateTransitionTracker);

        DeclarativeTaskManagerSlot slot =
                new DeclarativeTaskManagerSlot(
                        new SlotID(ResourceID.generate(), 0),
                        ResourceProfile.ANY,
                        TASK_EXECUTOR_CONNECTION);
        slot.startAllocation(jobId1);
        slot.completeAllocation();

        // allocated -> allocated
        assertThat(reconciler.executeStateTransition(slot, jobId1), is(false));
        assertThat(stateTransitionTracker.stateTransitions, empty());

        // allocated -> free
        assertThat(reconciler.executeStateTransition(slot, null), is(true));
        assertThat(
                stateTransitionTracker.stateTransitions.remove(),
                is(transitionWithTargetStateForJob(SlotState.FREE, jobId1)));
    }

    @Test
    public void testSlotStatusReconciliationForAllocatedSlotWithDifferentJobID() {
        JobID jobId1 = new JobID();
        JobID jobId2 = new JobID();
        StateTransitionTracker stateTransitionTracker = new StateTransitionTracker();

        DefaultSlotTracker.SlotStatusStateReconciler reconciler =
                createSlotStatusReconciler(stateTransitionTracker);

        DeclarativeTaskManagerSlot slot =
                new DeclarativeTaskManagerSlot(
                        new SlotID(ResourceID.generate(), 0),
                        ResourceProfile.ANY,
                        TASK_EXECUTOR_CONNECTION);
        slot.startAllocation(jobId1);
        slot.completeAllocation();

        // allocated(job1) -> free -> pending(job2) -> allocated(job2)
        assertThat(reconciler.executeStateTransition(slot, jobId2), is(true));
        assertThat(
                stateTransitionTracker.stateTransitions.remove(),
                is(transitionWithTargetStateForJob(SlotState.FREE, jobId1)));
        assertThat(
                stateTransitionTracker.stateTransitions.remove(),
                is(transitionWithTargetStateForJob(SlotState.PENDING, jobId2)));
        assertThat(
                stateTransitionTracker.stateTransitions.remove(),
                is(transitionWithTargetStateForJob(SlotState.ALLOCATED, jobId2)));
    }

    private static class StateTransitionTracker {
        Queue<SlotStateTransition> stateTransitions = new ArrayDeque<>();

        void notifyFree(DeclarativeTaskManagerSlot slot) {
            stateTransitions.add(new SlotStateTransition(SlotState.FREE, slot.getJobId()));
        }

        void notifyPending(JobID jobId) {
            stateTransitions.add(new SlotStateTransition(SlotState.PENDING, jobId));
        }

        void notifyAllocated(JobID jobId) {
            stateTransitions.add(new SlotStateTransition(SlotState.ALLOCATED, jobId));
        }
    }

    private static DefaultSlotTracker.SlotStatusStateReconciler createSlotStatusReconciler(
            StateTransitionTracker stateTransitionTracker) {
        return new DefaultSlotTracker.SlotStatusStateReconciler(
                stateTransitionTracker::notifyFree,
                (jobId, jobId2) -> stateTransitionTracker.notifyPending(jobId2),
                (jobId1, jobId12) -> stateTransitionTracker.notifyAllocated(jobId12));
    }

    private static class SlotStateTransition {

        private final SlotState newState;
        @Nullable private final JobID jobId;

        private SlotStateTransition(SlotState newState, @Nullable JobID jobId) {
            this.jobId = jobId;
            this.newState = newState;
        }

        @Override
        public String toString() {
            return "SlotStateTransition{" + ", newState=" + newState + ", jobId=" + jobId + '}';
        }
    }

    private static Matcher<SlotStateTransition> transitionWithTargetStateForJob(
            SlotState targetState, JobID jobId) {
        return new SlotStateTransitionMatcher(targetState, jobId);
    }

    private static class SlotStateTransitionMatcher extends TypeSafeMatcher<SlotStateTransition> {

        private final SlotState targetState;
        private final JobID jobId;

        private SlotStateTransitionMatcher(SlotState targetState, JobID jobId) {
            this.targetState = targetState;
            this.jobId = jobId;
        }

        @Override
        protected boolean matchesSafely(SlotStateTransition item) {
            return item.newState == targetState && jobId.equals(item.jobId);
        }

        @Override
        public void describeTo(Description description) {
            description
                    .appendText("a transition with targetState=")
                    .appendValue(targetState)
                    .appendText(" and jobId=")
                    .appendValue(jobId);
        }
    }
}
