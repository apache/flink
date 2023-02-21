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

package org.apache.flink.table.runtime.operators.window.triggers;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.runtime.operators.window.Window;

import static org.apache.flink.table.runtime.util.TimeWindowUtil.toEpochMillsForTimer;

/** A {@code WindowTrigger} determines when a window should be evaluated to emit the results. */
@Internal
public abstract class WindowTrigger<W extends Window> extends Trigger<W> {

    /**
     * The {@link org.apache.flink.table.runtime.operators.window.triggers.Trigger.TriggerContext}
     * of the window trigger.
     */
    protected transient TriggerContext ctx;

    /**
     * Returns the trigger time of the window, this should be called after TriggerContext
     * initialized.
     */
    protected long triggerTime(W window) {
        return toEpochMillsForTimer(window.maxTimestamp(), ctx.getShiftTimeZone());
    }
}
