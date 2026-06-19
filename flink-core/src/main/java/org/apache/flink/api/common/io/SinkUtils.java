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

package org.apache.flink.api.common.io;

import org.apache.flink.annotation.Experimental;

import java.io.Serializable;
import java.time.Duration;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Utility class for sinks. */
@Experimental
public class SinkUtils implements Serializable {

    /**
     * Acquire permits on the given semaphore within a given allowed timeout and deal with errors.
     *
     * @param permits the mumber of permits to acquire.
     * @param maxConcurrentRequests the maximum number of permits the semaphore was initialized
     *     with.
     * @param maxConcurrentRequestsTimeout the timeout to acquire the permits.
     * @param semaphore the semaphore to acquire permits to.
     * @throws InterruptedException if the current thread was interrupted.
     * @throws TimeoutException if the waiting time elapsed before all permits were acquired.
     */
    public static void tryAcquire(
            int permits,
            int maxConcurrentRequests,
            Duration maxConcurrentRequestsTimeout,
            Semaphore semaphore)
            throws InterruptedException, TimeoutException {
        if (!semaphore.tryAcquire(
                permits, maxConcurrentRequestsTimeout.toMillis(), TimeUnit.MILLISECONDS)) {
            throw new TimeoutException(
                    String.format(
                            "Failed to acquire %d out of %d permits to send value in %s.",
                            permits, maxConcurrentRequests, maxConcurrentRequestsTimeout));
        }
    }
}
