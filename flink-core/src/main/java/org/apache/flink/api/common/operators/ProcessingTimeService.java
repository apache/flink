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

package org.apache.flink.api.common.operators;

import org.apache.flink.annotation.PublicEvolving;

import java.io.IOException;
import java.util.concurrent.ScheduledFuture;

/**
 * A service that allows to get the current processing time and register timers that will execute
 * the given {@link ProcessingTimeCallback} when firing.
 */
@PublicEvolving
public interface ProcessingTimeService {
    /** Returns the current processing time. */
    long getCurrentProcessingTime();

    /**
     * Registers a task to be executed when (processing) time is {@code timestamp}.
     *
     * @param timestamp Time when the task is to be executed (in processing time)
     * @param target The task to be executed
     * @return The future that represents the scheduled task. This always returns some future, even
     *     if the timer was shut down
     */
    ScheduledFuture<?> registerTimer(long timestamp, ProcessingTimeCallback target);

    /**
     * A callback that can be registered via {@link #registerTimer(long, ProcessingTimeCallback)}.
     */
    @PublicEvolving
    interface ProcessingTimeCallback {
        /**
         * This method is invoked with the time which the callback register for.
         *
         * @param time The time this callback was registered for.
         */
        void onProcessingTime(long time) throws IOException, InterruptedException, Exception;
    }
}
