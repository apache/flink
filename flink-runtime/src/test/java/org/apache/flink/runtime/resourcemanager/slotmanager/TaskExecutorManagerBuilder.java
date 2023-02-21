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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.util.concurrent.Executors;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import java.time.Duration;
import java.util.concurrent.Executor;

/** Builder for {@link TaskExecutorManager}. */
public class TaskExecutorManagerBuilder {
    private WorkerResourceSpec defaultWorkerResourceSpec =
            new WorkerResourceSpec.Builder().setCpuCores(4).build();
    private int numSlotsPerWorker = 1;
    private int maxSlotNum = 1;
    private boolean waitResultConsumedBeforeRelease = true;
    private int redundantTaskManagerNum = 0;
    private Time taskManagerTimeout = Time.seconds(5);
    private Duration declareNeededResourceDelay = Duration.ofMillis(0);
    private final ScheduledExecutor scheduledExecutor;
    private Executor mainThreadExecutor = Executors.directExecutor();
    private ResourceAllocator newResourceAllocator = new TestingResourceAllocatorBuilder().build();

    public TaskExecutorManagerBuilder(ScheduledExecutor scheduledExecutor) {
        this.scheduledExecutor = scheduledExecutor;
    }

    public TaskExecutorManagerBuilder setDefaultWorkerResourceSpec(
            WorkerResourceSpec defaultWorkerResourceSpec) {
        this.defaultWorkerResourceSpec = defaultWorkerResourceSpec;
        return this;
    }

    public TaskExecutorManagerBuilder setNumSlotsPerWorker(int numSlotsPerWorker) {
        this.numSlotsPerWorker = numSlotsPerWorker;
        return this;
    }

    public TaskExecutorManagerBuilder setMaxNumSlots(int maxSlotNum) {
        this.maxSlotNum = maxSlotNum;
        return this;
    }

    public TaskExecutorManagerBuilder setWaitResultConsumedBeforeRelease(
            boolean waitResultConsumedBeforeRelease) {
        this.waitResultConsumedBeforeRelease = waitResultConsumedBeforeRelease;
        return this;
    }

    public TaskExecutorManagerBuilder setRedundantTaskManagerNum(int redundantTaskManagerNum) {
        this.redundantTaskManagerNum = redundantTaskManagerNum;
        return this;
    }

    public TaskExecutorManagerBuilder setTaskManagerTimeout(Time taskManagerTimeout) {
        this.taskManagerTimeout = taskManagerTimeout;
        return this;
    }

    public TaskExecutorManagerBuilder setMainThreadExecutor(Executor mainThreadExecutor) {
        this.mainThreadExecutor = mainThreadExecutor;
        return this;
    }

    public TaskExecutorManagerBuilder setResourceAllocator(ResourceAllocator newResourceAllocator) {
        this.newResourceAllocator = newResourceAllocator;
        return this;
    }

    public TaskExecutorManagerBuilder setDeclareNeededResourceDelay(
            Duration declareNeededResourceDelay) {
        this.declareNeededResourceDelay = declareNeededResourceDelay;
        return this;
    }

    public TaskExecutorManager createTaskExecutorManager() {
        return new TaskExecutorManager(
                defaultWorkerResourceSpec,
                numSlotsPerWorker,
                maxSlotNum,
                waitResultConsumedBeforeRelease,
                redundantTaskManagerNum,
                taskManagerTimeout,
                declareNeededResourceDelay,
                scheduledExecutor,
                mainThreadExecutor,
                newResourceAllocator);
    }
}
