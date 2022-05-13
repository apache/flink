/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.changelog.fs;

import javax.annotation.concurrent.NotThreadSafe;

import static org.apache.flink.util.Preconditions.checkState;

/** Helper class to throttle upload requests when the in-flight data size limit is exceeded. */
@NotThreadSafe
class UploadThrottle {

    private final long maxBytesInFlight;

    private long inFlightBytesCounter = 0;

    UploadThrottle(long maxBytesInFlight) {
        this.maxBytesInFlight = maxBytesInFlight;
    }

    /**
     * Seize <b>bytes</b> capacity. It is the caller responsibility to ensure at least some capacity
     * {@link #hasCapacity() is available}. <strong>After</strong> this call, the caller is allowed
     * to actually use the seized capacity. When the capacity is not needed anymore, the caller is
     * required to {@link #releaseCapacity(long) release} it. Called by the Task thread.
     *
     * @throws IllegalStateException if capacity is unavailable.
     */
    public void seizeCapacity(long bytes) throws IllegalStateException {
        checkState(hasCapacity());
        inFlightBytesCounter += bytes;
    }

    /**
     * Release previously {@link #seizeCapacity(long) seized} capacity. Called by {@link
     * BatchingStateChangeUploadScheduler} (IO thread).
     */
    public void releaseCapacity(long bytes) {
        inFlightBytesCounter -= bytes;
    }

    /** Test whether some capacity is available. */
    public boolean hasCapacity() {
        return inFlightBytesCounter < maxBytesInFlight;
    }
}
