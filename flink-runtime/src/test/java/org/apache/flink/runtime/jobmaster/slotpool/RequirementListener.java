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
final class RequirementListener {

    private final ComponentMainThreadExecutor componentMainThreadExecutor;
    private final Duration slotRequestMaxInterval;
    private ScheduledFuture<?> slotRequestFuture;
    private ResourceCounter requirements = ResourceCounter.empty();

    RequirementListener(
            ComponentMainThreadExecutor componentMainThreadExecutor,
            @Nonnull Duration slotRequestMaxInterval) {
        this.componentMainThreadExecutor = componentMainThreadExecutor;
        this.slotRequestMaxInterval = slotRequestMaxInterval;
    }

    void increaseRequirements(ResourceCounter requirements) {
        if (slotRequestMaxInterval.toMillis() <= 0L) {
            this.requirements = this.requirements.add(requirements);
            return;
        }

        if (!slotSlotRequestFutureAssignable()) {
            slotRequestFuture.cancel(true);
        }
        slotRequestFuture =
                componentMainThreadExecutor.schedule(
                        () -> this.checkSlotRequestMaxInterval(requirements),
                        slotRequestMaxInterval.toMillis(),
                        TimeUnit.MILLISECONDS);
    }

    void decreaseRequirements(ResourceCounter requirements) {
        this.requirements = this.requirements.subtract(requirements);
    }

    ResourceCounter getRequirements() {
        return requirements;
    }

    void tryWaitSlotRequestIsDone() {
        if (Objects.nonNull(slotRequestFuture)) {
            try {
                slotRequestFuture.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private boolean slotSlotRequestFutureAssignable() {
        return slotRequestFuture == null
                || slotRequestFuture.isDone()
                || slotRequestFuture.isCancelled();
    }

    private void checkSlotRequestMaxInterval(ResourceCounter requirements) {
        if (slotRequestMaxInterval.toMillis() <= 0L) {
            return;
        }
        this.requirements = this.requirements.add(requirements);
    }
}
