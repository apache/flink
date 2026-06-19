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

package org.apache.flink.datastream.api.extension.eventtime.timer;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.datastream.api.stream.KeyedPartitionStream;

/**
 * This class is responsible for managing stuff related to event-time/timer. For example, register
 * and delete event timers, as well as retrieve event time. Note that methods for timer can only be
 * used in {@link KeyedPartitionStream}.
 */
@Experimental
public interface EventTimeManager {
    /**
     * Register an event timer for this process function. The {@code onEventTimer} method will be
     * invoked when the event time is reached.
     *
     * @param timestamp to trigger timer callback.
     */
    void registerTimer(long timestamp);

    /**
     * Deletes the event-time timer with the given trigger timestamp. This method has only an effect
     * if such a timer was previously registered and did not already expire.
     *
     * @param timestamp indicates the timestamp of the timer to delete.
     */
    void deleteTimer(long timestamp);

    /**
     * Get the current event time.
     *
     * @return current event time.
     */
    long currentTime();
}
