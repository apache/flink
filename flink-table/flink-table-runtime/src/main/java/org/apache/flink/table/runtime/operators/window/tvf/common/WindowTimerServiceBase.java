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

package org.apache.flink.table.runtime.operators.window.tvf.common;

import org.apache.flink.streaming.api.operators.InternalTimerService;

import java.time.ZoneId;

/** A base implement of {@link WindowTimerService}. */
public abstract class WindowTimerServiceBase<W> implements WindowTimerService<W> {

    protected final InternalTimerService<W> internalTimerService;
    protected final ZoneId shiftTimeZone;

    public WindowTimerServiceBase(
            InternalTimerService<W> internalTimerService, ZoneId shiftTimeZone) {
        this.internalTimerService = internalTimerService;
        this.shiftTimeZone = shiftTimeZone;
    }

    @Override
    public ZoneId getShiftTimeZone() {
        return shiftTimeZone;
    }

    @Override
    public long currentProcessingTime() {
        return internalTimerService.currentProcessingTime();
    }

    @Override
    public long currentWatermark() {
        return internalTimerService.currentWatermark();
    }
}
