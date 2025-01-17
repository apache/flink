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

package org.apache.flink.datastream.impl.extension.eventtime.timer;

import org.apache.flink.datastream.api.extension.eventtime.timer.EventTimeManager;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.streaming.api.operators.InternalTimerService;

import javax.annotation.Nullable;

import java.util.function.Supplier;

/** The implementation of {@link EventTimeManager}. */
public class DefaultEventTimeManager implements EventTimeManager {

    /**
     * The timer service of operator, used in register event timer. Note that it cloud be null if
     * the operator is not a keyed operator.
     */
    @Nullable private final InternalTimerService<VoidNamespace> timerService;

    /** The supplier of the current event time. */
    private final Supplier<Long> eventTimeSupplier;

    public DefaultEventTimeManager(
            @Nullable InternalTimerService<VoidNamespace> timerService,
            Supplier<Long> eventTimeSupplier) {
        this.timerService = timerService;
        this.eventTimeSupplier = eventTimeSupplier;
    }

    @Override
    public void registerTimer(long timestamp) {
        if (timerService == null) {
            throw new UnsupportedOperationException(
                    "Registering event timer is not allowed in NonKeyed Stream.");
        }

        timerService.registerEventTimeTimer(VoidNamespace.INSTANCE, timestamp);
    }

    @Override
    public void deleteTimer(long timestamp) {
        if (timerService == null) {
            throw new UnsupportedOperationException(
                    "Deleting event timer is not allowed in NonKeyed Stream.");
        }

        timerService.deleteEventTimeTimer(VoidNamespace.INSTANCE, timestamp);
    }

    @Override
    public long currentTime() {
        return eventTimeSupplier.get();
    }
}
