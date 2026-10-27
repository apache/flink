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
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.ProcessTableFunction.TimeContext;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Internal {@link TimeContext} for PTFs with set semantics. */
@Internal
class WritableInternalTimeContext extends ReadableInternalTimeContext {

    private final KeyedStateBackend<RowData> keyedStateBackend;
    private final MapState<StringData, Long> namedTimersMapState;
    private final InternalTimerService<StringData> namedTimerService;
    private final InternalTimerService<VoidNamespace> unnamedTimerService;

    WritableInternalTimeContext(
            KeyedStateBackend<RowData> keyedStateBackend,
            MapState<StringData, Long> namedTimersMapState,
            InternalTimerService<StringData> namedTimerService,
            InternalTimerService<VoidNamespace> unnamedTimerService) {
        this.keyedStateBackend = keyedStateBackend;
        this.namedTimersMapState = namedTimersMapState;
        this.namedTimerService = namedTimerService;
        this.unnamedTimerService = unnamedTimerService;
    }

    @Override
    public Long currentWatermark() {
        final long currentWatermark = unnamedTimerService.currentWatermark();
        if (currentWatermark == Long.MIN_VALUE) {
            return null;
        }
        return currentWatermark;
    }

    @Override
    public void registerOnTime(String name, Long time) {
        registerOnTimeInternal(name, time);
    }

    @Override
    public void registerOnTime(Long time) {
        registerOnTimeInternal(null, time);
    }

    @Override
    public void clearTimer(String name) {
        Preconditions.checkNotNull(name, "Timer name must not be null.");
        deleteTimerInternal(name);
    }

    @Override
    public void clearTimer(Long time) {
        deleteTimerInternal(time);
    }

    @Override
    public void clearAllTimers() {
        clearUnnamedTimers();
        clearNamedTimers();
    }

    private void registerOnTimeInternal(@Nullable String name, long newTime) {
        if (name != null) {
            replaceNamedTimer(StringData.fromString(name), newTime);
        } else {
            registerUnnamedTimer(newTime);
        }
    }

    // --------------------------------------------------------------------------------------------
    // Unnamed timers
    // --------------------------------------------------------------------------------------------

    protected void deleteTimerInternal(long time) {
        deleteUnnamedTimer(time);
    }

    private void registerUnnamedTimer(long time) {
        unnamedTimerService.registerEventTimeTimer(VoidNamespace.INSTANCE, time);
    }

    private void deleteUnnamedTimer(long time) {
        unnamedTimerService.deleteEventTimeTimer(VoidNamespace.INSTANCE, time);
    }

    private void clearUnnamedTimers() {
        final RowData currentKey = keyedStateBackend.getCurrentKey();
        final List<Long> unnamedTimers = new ArrayList<>();
        try {
            // The timer service iterates the timers of all keys and sets the key context to the
            // key of each visited timer. Only timers of the current key must be cleared.
            // The equals is safe: keys are BinaryRowData, which compares the serialized bytes and
            // not the object identity, so a key deserialized by the timer service still matches.
            unnamedTimerService.forEachEventTimeTimer(
                    (namespace, time) -> {
                        if (currentKey.equals(keyedStateBackend.getCurrentKey())) {
                            unnamedTimers.add(time);
                        }
                    });
        } catch (Exception e) {
            throw new FlinkRuntimeException(e);
        } finally {
            keyedStateBackend.setCurrentKey(currentKey);
        }
        unnamedTimers.forEach(
                time -> unnamedTimerService.deleteEventTimeTimer(VoidNamespace.INSTANCE, time));
    }

    // --------------------------------------------------------------------------------------------
    // Named timers
    // --------------------------------------------------------------------------------------------

    private void deleteTimerInternal(String name) {
        deleteNamedTimer(StringData.fromString(name));
    }

    private void replaceNamedTimer(StringData internalName, long newTime) {
        final Long existingTime;
        try {
            existingTime = namedTimersMapState.get(internalName);
        } catch (Exception e) {
            throw new FlinkRuntimeException(e);
        }
        if (existingTime != null) {
            namedTimerService.deleteEventTimeTimer(internalName, existingTime);
        }
        registerNamedTimer(internalName, newTime);
    }

    private void registerNamedTimer(StringData internalName, long newTime) {
        try {
            namedTimersMapState.put(internalName, newTime);
        } catch (Exception e) {
            throw new FlinkRuntimeException(e);
        }
        namedTimerService.registerEventTimeTimer(internalName, newTime);
    }

    private void deleteNamedTimer(StringData internalName) {
        final Long existingTime;
        try {
            existingTime = namedTimersMapState.get(internalName);
        } catch (Exception e) {
            throw new FlinkRuntimeException(e);
        }
        if (existingTime != null) {
            namedTimerService.deleteEventTimeTimer(internalName, existingTime);
            try {
                namedTimersMapState.remove(internalName);
            } catch (Exception e) {
                throw new FlinkRuntimeException(e);
            }
        }
    }

    private void clearNamedTimers() {
        final Iterable<Map.Entry<StringData, Long>> entries;
        try {
            entries = namedTimersMapState.entries();
        } catch (Exception e) {
            throw new FlinkRuntimeException(e);
        }
        entries.forEach(e -> namedTimerService.deleteEventTimeTimer(e.getKey(), e.getValue()));
        namedTimersMapState.clear();
    }
}
