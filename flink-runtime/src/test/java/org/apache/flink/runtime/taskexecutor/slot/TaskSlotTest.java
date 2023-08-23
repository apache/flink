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

package org.apache.flink.runtime.taskexecutor.slot;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.util.concurrent.Executors;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createExecutionAttemptId;
import static org.assertj.core.api.Assertions.assertThat;

class TaskSlotTest {
    private static final JobID JOB_ID = new JobID();
    private static final AllocationID ALLOCATION_ID = new AllocationID();

    @Test
    void testTaskSlotClosedOnlyWhenAddedTasksTerminated() throws Exception {
        try (TaskSlot<TaskSlotPayload> taskSlot = createTaskSlot()) {
            taskSlot.markActive();
            TestingTaskSlotPayload task =
                    new TestingTaskSlotPayload(JOB_ID, createExecutionAttemptId(), ALLOCATION_ID);
            taskSlot.add(task);

            CompletableFuture<Void> closingFuture = taskSlot.closeAsync();
            task.waitForFailure();
            MemoryManager memoryManager = taskSlot.getMemoryManager();

            assertThat(closingFuture).isNotDone();
            assertThat(memoryManager.isShutdown()).isFalse();
            task.terminate();
            closingFuture.get();
            assertThat(memoryManager.isShutdown()).isTrue();
        }
    }

    private static <T extends TaskSlotPayload> TaskSlot<T> createTaskSlot() {
        return new TaskSlot<>(
                0,
                ResourceProfile.ZERO,
                MemoryManager.MIN_PAGE_SIZE,
                JOB_ID,
                ALLOCATION_ID,
                Executors.newDirectExecutorService());
    }
}
