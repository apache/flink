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

package org.apache.flink.runtime.io.network.partition.consumer;

import java.util.concurrent.CompletableFuture;

/**
 * Abstracts the notification of the availability futures of {@link InputGate}s.
 *
 * <p>Should be created and closed outside of the lock.
 */
class GateNotificationHelper implements AutoCloseable {
    private final InputGate inputGate;
    private final Object availabilityMonitor;

    private CompletableFuture<?> toNotifyPriority;
    private CompletableFuture<?> toNotify;

    public GateNotificationHelper(InputGate inputGate, Object availabilityMonitor) {
        this.inputGate = inputGate;
        this.availabilityMonitor = availabilityMonitor;
    }

    @Override
    public void close() {
        if (toNotifyPriority != null) {
            toNotifyPriority.complete(null);
        }
        if (toNotify != null) {
            toNotify.complete(null);
        }
    }

    /** Must be called under lock to ensure integrity of priorityAvailabilityHelper. */
    public void notifyPriority() {
        toNotifyPriority = inputGate.priorityAvailabilityHelper.getUnavailableToResetAvailable();
    }

    /**
     * Must be called under lock to ensure integrity of availabilityHelper and allow notification.
     */
    public void notifyDataAvailable() {
        availabilityMonitor.notifyAll();
        toNotify = inputGate.availabilityHelper.getUnavailableToResetAvailable();
    }
}
