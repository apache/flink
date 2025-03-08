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

package org.apache.flink.table.runtime.operators.process;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.util.Preconditions;

/**
 * User-facing time context that converts to/from the desired time class.
 *
 * @param <TimeConversion> a conversion supported by {@link TimeConverter}
 */
@Internal
class ExternalTimeContext<TimeConversion>
        implements ProcessTableFunction.TimeContext<TimeConversion> {

    private final ReadableInternalTimeContext internalTimeContext;
    private final TimeConverter<TimeConversion> timeConverter;

    ExternalTimeContext(
            ReadableInternalTimeContext internalTimeContext,
            TimeConverter<TimeConversion> timeConverter) {
        this.internalTimeContext = internalTimeContext;
        this.timeConverter = timeConverter;
    }

    @Override
    public TimeConversion time() {
        final Long internal = internalTimeContext.time();
        if (internal == null) {
            return null;
        }
        return timeConverter.toExternal(internal);
    }

    @Override
    public TimeConversion currentWatermark() {
        final Long internal = internalTimeContext.currentWatermark();
        if (internal == null) {
            return null;
        }
        return timeConverter.toExternal(internal);
    }

    @Override
    public void registerOnTime(String name, TimeConversion time) {
        Preconditions.checkNotNull(name, "Name must not be null.");
        Preconditions.checkNotNull(time, "Time must not be null.");
        internalTimeContext.registerOnTime(name, timeConverter.toInternal(time));
    }

    @Override
    public void registerOnTime(TimeConversion time) {
        Preconditions.checkNotNull(time, "Time must not be null.");
        internalTimeContext.registerOnTime(timeConverter.toInternal(time));
    }

    @Override
    public void clearTimer(String name) {
        Preconditions.checkNotNull(name, "Name must not be null.");
        internalTimeContext.clearTimer(name);
    }

    @Override
    public void clearTimer(TimeConversion time) {
        Preconditions.checkNotNull(time, "Time must not be null.");
        internalTimeContext.clearTimer(timeConverter.toInternal(time));
    }

    @Override
    public void clearAllTimers() {
        internalTimeContext.clearAllTimers();
    }
}
