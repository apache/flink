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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.function.BiConsumerWithException;

/**
 * Interface for working with time and timers.
 *
 * <p>This is the internal version of {@link org.apache.flink.streaming.api.TimerService} that
 * allows to specify a key and a namespace to which timers should be scoped.
 *
 * @param <N> Type of the namespace to which timers are scoped.
 */
@Internal
public interface InternalTimerService<N> {

    /** Returns the current processing time. */
    long currentProcessingTime();

    /** Returns the current event-time watermark. */
    long currentWatermark();

    /**
     * Registers a timer to be fired when processing time passes the given time. The namespace you
     * pass here will be provided when the timer fires.
     */
    void registerProcessingTimeTimer(N namespace, long time);

    /** Deletes the timer for the given key and namespace. */
    void deleteProcessingTimeTimer(N namespace, long time);

    /**
     * Registers a timer to be fired when event time watermark passes the given time. The namespace
     * you pass here will be provided when the timer fires.
     */
    void registerEventTimeTimer(N namespace, long time);

    /** Deletes the timer for the given key and namespace. */
    void deleteEventTimeTimer(N namespace, long time);

    /**
     * Performs an action for each registered timer. The timer service will set the key context for
     * the timers key before invoking the action.
     */
    void forEachEventTimeTimer(BiConsumerWithException<N, Long, Exception> consumer)
            throws Exception;

    /**
     * Performs an action for each registered timer. The timer service will set the key context for
     * the timers key before invoking the action.
     */
    void forEachProcessingTimeTimer(BiConsumerWithException<N, Long, Exception> consumer)
            throws Exception;
}
