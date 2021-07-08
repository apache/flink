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

package org.apache.flink.streaming.api;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.streaming.api.operators.InternalTimerService;

/** Implementation of {@link TimerService} that uses a {@link InternalTimerService}. */
@Internal
public class SimpleTimerService implements TimerService {

    private final InternalTimerService<VoidNamespace> internalTimerService;

    public SimpleTimerService(InternalTimerService<VoidNamespace> internalTimerService) {
        this.internalTimerService = internalTimerService;
    }

    @Override
    public long currentProcessingTime() {
        return internalTimerService.currentProcessingTime();
    }

    @Override
    public long currentWatermark() {
        return internalTimerService.currentWatermark();
    }

    @Override
    public void registerProcessingTimeTimer(long time) {
        internalTimerService.registerProcessingTimeTimer(VoidNamespace.INSTANCE, time);
    }

    @Override
    public void registerEventTimeTimer(long time) {
        internalTimerService.registerEventTimeTimer(VoidNamespace.INSTANCE, time);
    }

    @Override
    public void deleteProcessingTimeTimer(long time) {
        internalTimerService.deleteProcessingTimeTimer(VoidNamespace.INSTANCE, time);
    }

    @Override
    public void deleteEventTimeTimer(long time) {
        internalTimerService.deleteEventTimeTimer(VoidNamespace.INSTANCE, time);
    }
}
