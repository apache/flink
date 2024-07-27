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

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.slots.ResourceRequirement;

import java.time.Duration;
import java.util.Collection;
import java.util.function.Consumer;

import static org.apache.flink.configuration.JobManagerOptions.SLOT_REQUEST_MAX_INTERVAL;
import static org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter.forMainThread;

/** Builder for {@link DefaultDeclarativeSlotPool}. */
final class DefaultDeclarativeSlotPoolBuilder {

    private AllocatedSlotPool allocatedSlotPool = new DefaultAllocatedSlotPool();
    private Consumer<? super Collection<ResourceRequirement>> notifyNewResourceRequirements =
            ignored -> {};
    private Duration idleSlotTimeout = Duration.ofSeconds(20);
    private Duration rpcTimeout = Duration.ofSeconds(20);
    private Duration slotRequestMaxInterval = SLOT_REQUEST_MAX_INTERVAL.defaultValue();
    private ComponentMainThreadExecutor componentMainThreadExecutor = forMainThread();

    public DefaultDeclarativeSlotPoolBuilder setAllocatedSlotPool(
            AllocatedSlotPool allocatedSlotPool) {
        this.allocatedSlotPool = allocatedSlotPool;
        return this;
    }

    public DefaultDeclarativeSlotPoolBuilder setNotifyNewResourceRequirements(
            Consumer<? super Collection<ResourceRequirement>> notifyNewResourceRequirements) {
        this.notifyNewResourceRequirements = notifyNewResourceRequirements;
        return this;
    }

    public DefaultDeclarativeSlotPoolBuilder setIdleSlotTimeout(Duration idleSlotTimeout) {
        this.idleSlotTimeout = idleSlotTimeout;
        return this;
    }

    public DefaultDeclarativeSlotPoolBuilder setSlotRequestMaxInterval(
            Duration slotRequestMaxInterval) {
        this.slotRequestMaxInterval = slotRequestMaxInterval;
        return this;
    }

    public DefaultDeclarativeSlotPoolBuilder setComponentMainThreadExecutor(
            ComponentMainThreadExecutor componentMainThreadExecutor) {
        this.componentMainThreadExecutor = componentMainThreadExecutor;
        return this;
    }

    public DefaultDeclarativeSlotPool build() {
        return new DefaultDeclarativeSlotPool(
                new JobID(),
                allocatedSlotPool,
                notifyNewResourceRequirements,
                idleSlotTimeout,
                rpcTimeout,
                slotRequestMaxInterval,
                componentMainThreadExecutor);
    }

    public static DefaultDeclarativeSlotPoolBuilder builder() {
        return new DefaultDeclarativeSlotPoolBuilder();
    }
}
