/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.util.ResourceCounter;

import javax.annotation.Nonnull;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/** Requirement listener for testing. */
public final class RequirementListener {

    private ComponentMainThreadExecutor componentMainThreadExecutor;
    private Duration slotRequestMaxInterval;
    private ScheduledFuture<?> slotRequestMaxIntervalTimeoutFuture;

    public RequirementListener() {}

    public RequirementListener(
            ComponentMainThreadExecutor componentMainThreadExecutor,
            @Nonnull Duration slotRequestMaxInterval) {
        this.componentMainThreadExecutor = componentMainThreadExecutor;
        this.slotRequestMaxInterval = slotRequestMaxInterval;
    }

    private ResourceCounter requirements = ResourceCounter.empty();

    public void increaseRequirements(ResourceCounter requirements) {
        if (slotRequestMaxInterval.toMillis() <= 0L) {
            this.requirements = this.requirements.add(requirements);
            return;
        }

        if (!slotRequestMaxIntervalTimeoutFutureAssignable()) {
            slotRequestMaxIntervalTimeoutFuture.cancel(true);
        }
        slotRequestMaxIntervalTimeoutFuture =
                componentMainThreadExecutor.schedule(
                        () -> this.checkSlotRequestMaxIntervalTimeout(requirements),
                        slotRequestMaxInterval.toMillis(),
                        TimeUnit.MILLISECONDS);
    }

    public void decreaseRequirements(ResourceCounter requirements) {
        this.requirements = this.requirements.subtract(requirements);
    }

    public ResourceCounter getRequirements() {
        return requirements;
    }

    public void tryWaitSlotRequestIntervalTimeout() {
        if (Objects.nonNull(slotRequestMaxIntervalTimeoutFuture)) {
            try {
                slotRequestMaxIntervalTimeoutFuture.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public boolean slotRequestMaxIntervalTimeoutFutureAssignable() {
        return slotRequestMaxIntervalTimeoutFuture == null
                || slotRequestMaxIntervalTimeoutFuture.isDone()
                || slotRequestMaxIntervalTimeoutFuture.isCancelled();
    }

    public void checkSlotRequestMaxIntervalTimeout(ResourceCounter requirements) {
        if (slotRequestMaxInterval.toMillis() <= 0L) {
            return;
        }
        this.requirements = this.requirements.add(requirements);
    }
}
