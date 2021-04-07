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

package org.apache.flink.table.runtime.operators.window.slicing;

import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.table.runtime.util.TimeWindowUtil;

import java.time.ZoneId;

/** Simple Implements of {@link WindowTimerService}. */
public class WindowTimerServiceImpl<W> implements WindowTimerService<W> {

    private final InternalTimerService<W> internalTimerService;
    private final ZoneId shiftTimeZone;

    public WindowTimerServiceImpl(
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

    @Override
    public InternalTimerService<W> getInternalTimerService() {
        return internalTimerService;
    }

    @Override
    public void registerProcessingTimeWindowTimer(W window, long windowEnd) {
        internalTimerService.registerProcessingTimeTimer(
                window, TimeWindowUtil.toEpochMillsForTimer(windowEnd, shiftTimeZone));
    }

    @Override
    public void deleteProcessingTimeWindowTimer(W window, long windowEnd) {
        internalTimerService.deleteProcessingTimeTimer(
                window, TimeWindowUtil.toEpochMillsForTimer(windowEnd, shiftTimeZone));
    }

    @Override
    public void registerEventTimeWindowTimer(W window, long windowEnd) {
        internalTimerService.registerEventTimeTimer(
                window, TimeWindowUtil.toEpochMillsForTimer(windowEnd, shiftTimeZone));
    }

    @Override
    public void deleteEventTimeWindowTimer(W window, long windowEnd) {
        internalTimerService.deleteEventTimeTimer(
                window, TimeWindowUtil.toEpochMillsForTimer(windowEnd, shiftTimeZone));
    }
}
