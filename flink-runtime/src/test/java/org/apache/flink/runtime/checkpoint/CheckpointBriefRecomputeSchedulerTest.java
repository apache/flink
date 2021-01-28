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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class CheckpointBriefRecomputeSchedulerTest {

    private ManuallyTriggeredScheduledExecutor manuallyTriggeredScheduledExecutor;

    @Before
    public void setUp() throws Exception {
        manuallyTriggeredScheduledExecutor = new ManuallyTriggeredScheduledExecutor();
    }

    @Test
    public void testTaskReportFinishedBeforeAllReportsProcessed() {
        CollectComputeFunction function = new CollectComputeFunction();

        ExecutionAttemptID attemptId = new ExecutionAttemptID();

        CheckpointBriefRecomputeScheduler scheduler =
                new CheckpointBriefRecomputeScheduler(
                        0,
                        manuallyTriggeredScheduledExecutor,
                        System::currentTimeMillis,
                        () -> false,
                        function);

        CompletableFuture<Void> firstReportFuture = new CompletableFuture<>();
        scheduler.onTaskReport(attemptId, firstReportFuture);

        CompletableFuture<Void> secondReportFuture = new CompletableFuture<>();
        scheduler.onTaskReport(attemptId, secondReportFuture);

        scheduler.onTaskFinished(attemptId);

        firstReportFuture.complete(null);
        manuallyTriggeredScheduledExecutor.triggerAll();
        assertEquals(0, function.getTriggered().size());

        secondReportFuture.complete(null);
        manuallyTriggeredScheduledExecutor.triggerAll();
        assertEquals(1, function.getTriggered().size());
        assertThat(function.getTriggered().get(0), containsInAnyOrder(attemptId));
    }

    @Test
    public void testTaskReportFinishedAfterAllReportsProcessed() {
        CollectComputeFunction function = new CollectComputeFunction();

        ExecutionAttemptID attemptId = new ExecutionAttemptID();

        CheckpointBriefRecomputeScheduler scheduler =
                new CheckpointBriefRecomputeScheduler(
                        0,
                        manuallyTriggeredScheduledExecutor,
                        System::currentTimeMillis,
                        () -> false,
                        function);

        CompletableFuture<Void> firstReportFuture = new CompletableFuture<>();
        scheduler.onTaskReport(attemptId, firstReportFuture);

        CompletableFuture<Void> secondReportFuture = new CompletableFuture<>();
        scheduler.onTaskReport(attemptId, secondReportFuture);

        firstReportFuture.complete(null);
        manuallyTriggeredScheduledExecutor.triggerAll();
        assertEquals(0, function.getTriggered().size());

        secondReportFuture.complete(null);
        manuallyTriggeredScheduledExecutor.triggerAll();
        assertEquals(0, function.getTriggered().size());

        scheduler.onTaskFinished(attemptId);
        manuallyTriggeredScheduledExecutor.triggerAll();
        assertEquals(1, function.getTriggered().size());
        assertThat(function.getTriggered().get(0), containsInAnyOrder(attemptId));
    }

    @Test
    public void testTriggerMultipleRounds() {
        CollectComputeFunction function = new CollectComputeFunction();

        CheckpointBriefRecomputeScheduler scheduler =
                new CheckpointBriefRecomputeScheduler(
                        0,
                        manuallyTriggeredScheduledExecutor,
                        System::currentTimeMillis,
                        () -> false,
                        function);

        ExecutionAttemptID attemptID1 = new ExecutionAttemptID();
        scheduler.onTaskFinished(attemptID1);
        manuallyTriggeredScheduledExecutor.triggerAll();
        assertEquals(1, function.getTriggered().size());
        assertThat(function.getTriggered().get(0), containsInAnyOrder(attemptID1));

        function.reset();
        ExecutionAttemptID attemptID2 = new ExecutionAttemptID();
        scheduler.onTaskFinished(attemptID2);
        manuallyTriggeredScheduledExecutor.triggerAll();
        assertEquals(1, function.getTriggered().size());
        assertThat(function.getTriggered().get(0), containsInAnyOrder(attemptID2));
    }

    @Test
    public void testNotRecomputingDuringTriggerCheckpoints() {
        AtomicBoolean isTriggering = new AtomicBoolean(true);

        CollectComputeFunction function = new CollectComputeFunction();

        ExecutionAttemptID attemptId = new ExecutionAttemptID();

        CheckpointBriefRecomputeScheduler scheduler =
                new CheckpointBriefRecomputeScheduler(
                        0,
                        manuallyTriggeredScheduledExecutor,
                        System::currentTimeMillis,
                        isTriggering::get,
                        function);
        scheduler.onTaskFinished(attemptId);
        manuallyTriggeredScheduledExecutor.triggerAll();
        assertEquals(0, function.getTriggered().size());

        isTriggering.set(false);
        scheduler.onTriggerFinished();
        manuallyTriggeredScheduledExecutor.triggerAll();
        assertEquals(1, function.getTriggered().size());
        assertThat(function.getTriggered().get(0), containsInAnyOrder(attemptId));
    }

    @Test
    public void testDelayTriggeredIfNotReachedInterval() {
        AtomicLong now = new AtomicLong(System.currentTimeMillis());

        CollectComputeFunction function = new CollectComputeFunction();

        CheckpointBriefRecomputeScheduler scheduler =
                new CheckpointBriefRecomputeScheduler(
                        100, manuallyTriggeredScheduledExecutor, now::get, () -> false, function);

        // Trigger the first re-computing, which should be execute directly
        ExecutionAttemptID attemptID1 = new ExecutionAttemptID();
        scheduler.onTaskFinished(attemptID1);
        manuallyTriggeredScheduledExecutor.trigger();
        assertEquals(0, manuallyTriggeredScheduledExecutor.getScheduledTasks().size());
        assertEquals(1, function.getTriggered().size());
        assertThat(function.getTriggered().get(0), containsInAnyOrder(attemptID1));

        // Should delay the re-computing
        function.reset();
        now.set(now.get() + 5);
        ExecutionAttemptID attemptID2 = new ExecutionAttemptID();
        scheduler.onTaskFinished(attemptID2);
        manuallyTriggeredScheduledExecutor.trigger();
        assertEquals(1, manuallyTriggeredScheduledExecutor.getScheduledTasks().size());
        assertEquals(0, function.getTriggered().size());

        // Should not schedule another task
        ExecutionAttemptID attemptID3 = new ExecutionAttemptID();
        scheduler.onTaskFinished(attemptID3);
        manuallyTriggeredScheduledExecutor.trigger();
        assertEquals(1, manuallyTriggeredScheduledExecutor.getScheduledTasks().size());

        now.set(now.get() + 100);
        manuallyTriggeredScheduledExecutor.triggerScheduledTasks();
        assertEquals(1, function.getTriggered().size());
        assertThat(function.getTriggered().get(0), containsInAnyOrder(attemptID2, attemptID3));
    }

    private static class CollectComputeFunction implements Consumer<List<ExecutionAttemptID>> {
        private final List<List<ExecutionAttemptID>> triggered = new ArrayList<>();

        @Override
        public void accept(List<ExecutionAttemptID> executionAttemptIDS) {
            triggered.add(executionAttemptIDS);
        }

        public List<List<ExecutionAttemptID>> getTriggered() {
            return triggered;
        }

        public void reset() {
            triggered.clear();
        }
    }
}
