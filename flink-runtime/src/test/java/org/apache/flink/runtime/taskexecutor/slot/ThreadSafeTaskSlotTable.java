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
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.rpc.MainThreadExecutable;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Iterator;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Testing implementation of {@link TaskSlotTable}. This class wraps a given {@link TaskSlotTable},
 * guarantees all the accesses are invoked on the given {@link MainThreadExecutable}.
 */
public class ThreadSafeTaskSlotTable<T extends TaskSlotPayload> implements TaskSlotTable<T> {

    private final TaskSlotTable<T> taskSlotTable;
    private final MainThreadExecutable mainThreadExecutable;

    public ThreadSafeTaskSlotTable(
            final TaskSlotTable<T> taskSlotTable, final MainThreadExecutable mainThreadExecutable) {
        this.taskSlotTable = Preconditions.checkNotNull(taskSlotTable);
        this.mainThreadExecutable = Preconditions.checkNotNull(mainThreadExecutable);
    }

    private void runAsync(Runnable runnable) {
        mainThreadExecutable.runAsync(runnable);
    }

    private <V> V callAsync(Callable<V> callable) {
        try {
            return mainThreadExecutable
                    .callAsync(
                            callable, Time.days(1) // practically infinite timeout
                            )
                    .get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void start(
            SlotActions initialSlotActions, ComponentMainThreadExecutor mainThreadExecutor) {
        runAsync(() -> taskSlotTable.start(initialSlotActions, mainThreadExecutor));
    }

    @Override
    public Set<AllocationID> getAllocationIdsPerJob(JobID jobId) {
        return callAsync(() -> taskSlotTable.getAllocationIdsPerJob(jobId));
    }

    @Override
    public Set<AllocationID> getActiveTaskSlotAllocationIds() {
        return callAsync(taskSlotTable::getActiveTaskSlotAllocationIds);
    }

    @Override
    public Set<AllocationID> getActiveTaskSlotAllocationIdsPerJob(JobID jobId) {
        return callAsync(() -> taskSlotTable.getActiveTaskSlotAllocationIdsPerJob(jobId));
    }

    @Override
    public SlotReport createSlotReport(ResourceID resourceId) {
        return callAsync(() -> taskSlotTable.createSlotReport(resourceId));
    }

    @Override
    public boolean allocateSlot(
            int index, JobID jobId, AllocationID allocationId, Time slotTimeout) {
        return callAsync(() -> taskSlotTable.allocateSlot(index, jobId, allocationId, slotTimeout));
    }

    @Override
    public boolean allocateSlot(
            int index,
            JobID jobId,
            AllocationID allocationId,
            ResourceProfile resourceProfile,
            Time slotTimeout) {
        return callAsync(
                () ->
                        taskSlotTable.allocateSlot(
                                index, jobId, allocationId, resourceProfile, slotTimeout));
    }

    @Override
    public boolean markSlotActive(AllocationID allocationId) throws SlotNotFoundException {
        return callAsync(() -> taskSlotTable.markSlotActive(allocationId));
    }

    @Override
    public boolean markSlotInactive(AllocationID allocationId, Time slotTimeout)
            throws SlotNotFoundException {
        return callAsync(() -> taskSlotTable.markSlotInactive(allocationId, slotTimeout));
    }

    @Override
    public int freeSlot(AllocationID allocationId) throws SlotNotFoundException {
        return callAsync(() -> taskSlotTable.freeSlot(allocationId));
    }

    @Override
    public int freeSlot(AllocationID allocationId, Throwable cause) throws SlotNotFoundException {
        return callAsync(() -> taskSlotTable.freeSlot(allocationId, cause));
    }

    @Override
    public boolean isValidTimeout(AllocationID allocationId, UUID ticket) {
        return callAsync(() -> taskSlotTable.isValidTimeout(allocationId, ticket));
    }

    @Override
    public boolean isAllocated(int index, JobID jobId, AllocationID allocationId) {
        return callAsync(() -> taskSlotTable.isAllocated(index, jobId, allocationId));
    }

    @Override
    public boolean tryMarkSlotActive(JobID jobId, AllocationID allocationId) {
        return callAsync(() -> taskSlotTable.tryMarkSlotActive(jobId, allocationId));
    }

    @Override
    public boolean isSlotFree(int index) {
        return callAsync(() -> taskSlotTable.isSlotFree(index));
    }

    @Override
    public boolean hasAllocatedSlots(JobID jobId) {
        return callAsync(() -> taskSlotTable.hasAllocatedSlots(jobId));
    }

    @Override
    public Iterator<TaskSlot<T>> getAllocatedSlots(JobID jobId) {
        return callAsync(() -> taskSlotTable.getAllocatedSlots(jobId));
    }

    @Nullable
    @Override
    public JobID getOwningJob(AllocationID allocationId) {
        return callAsync(() -> taskSlotTable.getOwningJob(allocationId));
    }

    @Override
    public boolean addTask(T task) throws SlotNotFoundException, SlotNotActiveException {
        return callAsync(() -> taskSlotTable.addTask(task));
    }

    @Override
    public T removeTask(ExecutionAttemptID executionAttemptID) {
        return callAsync(() -> taskSlotTable.removeTask(executionAttemptID));
    }

    @Override
    public T getTask(ExecutionAttemptID executionAttemptID) {
        return callAsync(() -> taskSlotTable.getTask(executionAttemptID));
    }

    @Override
    public Iterator<T> getTasks(JobID jobId) {
        return callAsync(() -> taskSlotTable.getTasks(jobId));
    }

    @Override
    public AllocationID getCurrentAllocation(int index) {
        return callAsync(() -> taskSlotTable.getCurrentAllocation(index));
    }

    @Override
    public MemoryManager getTaskMemoryManager(AllocationID allocationID)
            throws SlotNotFoundException {
        return callAsync(() -> taskSlotTable.getTaskMemoryManager(allocationID));
    }

    @Override
    public void notifyTimeout(AllocationID key, UUID ticket) {
        runAsync(() -> taskSlotTable.notifyTimeout(key, ticket));
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return callAsync(taskSlotTable::closeAsync);
    }

    @Override
    public void close() throws Exception {
        callAsync(
                () -> {
                    taskSlotTable.close();
                    return null;
                });
    }
}
