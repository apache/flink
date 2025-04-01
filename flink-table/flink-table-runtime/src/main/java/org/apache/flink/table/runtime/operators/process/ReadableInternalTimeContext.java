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
import org.apache.flink.table.api.TableRuntimeException;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.table.types.inference.StaticArgumentTrait;

import javax.annotation.Nullable;

import static org.apache.flink.table.functions.ProcessTableFunction.TimeContext;

/**
 * Internal {@link TimeContext} for PTFs with row semantics, PTFs without table arguments, or traits
 * {@link StaticArgumentTrait#PASS_COLUMNS_THROUGH} and {@link StaticArgumentTrait#SUPPORT_UPDATES}.
 */
@Internal
class ReadableInternalTimeContext implements ProcessTableFunction.TimeContext<Long> {

    protected long currentWatermark;
    protected @Nullable Long time;

    void setTime(long currentWatermark, @Nullable Long time) {
        this.currentWatermark = currentWatermark;
        this.time = time;
    }

    @Override
    public Long time() {
        if (time == null) {
            return null;
        }
        return time;
    }

    @Override
    public Long currentWatermark() {
        if (currentWatermark == Long.MIN_VALUE) {
            return null;
        }
        return currentWatermark;
    }

    @Override
    public void registerOnTime(String name, Long time) {
        throw readOnlyException();
    }

    @Override
    public void registerOnTime(Long time) {
        throw readOnlyException();
    }

    @Override
    public void clearTimer(String name) {}

    @Override
    public void clearTimer(Long time) {}

    @Override
    public void clearAllTimers() {}

    private static TableRuntimeException readOnlyException() {
        return new TableRuntimeException(
                "Timers are not supported in the current PTF declaration. "
                        + "Note that only PTFs that take set semantic tables support timers. "
                        + "Also timers are not available for advanced traits such as supporting pass-through columns or updates.");
    }
}
