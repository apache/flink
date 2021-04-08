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

import org.apache.flink.annotation.Internal;

import java.time.ZoneId;

/**
 * Interface for working with window time and timers which considers timezone for window splitting.
 *
 * @param <W> Type of the window namespace to which timers are scoped.
 */
@Internal
public interface WindowTimerService<W> {

    /**
     * The shift timezone of the window, if the proctime or rowtime type is TIMESTAMP_LTZ, the shift
     * timezone is the timezone user configured in TableConfig, other cases the timezone is UTC
     * which means never shift when assigning windows.
     */
    ZoneId getShiftTimeZone();

    /** Returns the current processing time. */
    long currentProcessingTime();

    /** Returns the current event-time watermark. */
    long currentWatermark();

    /**
     * Registers a window timer to be fired when processing time passes the window. The window you
     * pass here will be provided when the timer fires.
     */
    void registerProcessingTimeWindowTimer(W window);

    /**
     * Registers a window timer to be fired when event time watermark passes the window. The window
     * you pass here will be provided when the timer fires.
     */
    void registerEventTimeWindowTimer(W window);
}
