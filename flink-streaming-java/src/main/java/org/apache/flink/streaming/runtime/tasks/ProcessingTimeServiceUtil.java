/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;

/** Utility for classes that implements the {@link ProcessingTimeService} interface. */
@Internal
public class ProcessingTimeServiceUtil {

    /**
     * Returns the remaining delay of the processing time specified by {@code processingTimestamp}.
     * This delay guarantees that the timer will be fired at least 1ms after the time it's
     * registered for.
     *
     * @param processingTimestamp the processing time in milliseconds
     * @param currentTimestamp the current processing timestamp; it usually uses {@link
     *     ProcessingTimeService#getCurrentProcessingTime()} to get
     * @return the remaining delay of the processing time
     */
    public static long getProcessingTimeDelay(long processingTimestamp, long currentTimestamp) {

        // Two cases of timers here:
        // (1) future/now timers(processingTimestamp >= currentTimestamp): delay the firing of the
        //   timer by 1 ms to align the semantics with watermark. A watermark T says we
        //   won't see elements in the future with a timestamp smaller or equal to T. Without this
        //   1ms delay, if we had fired the timer for T at the timestamp T, it would be possible
        //   that we would process another record for timestamp == T in the same millisecond, but
        //   after the timer for the timsetamp T has already been fired.
        // (2) past timers(processingTimestamp < currentTimestamp): do not need to delay the firing
        //   because currentTimestamp is larger than processingTimestamp pluses the 1ms offset.
        // TODO. The processing timers' performance can be further improved.
        //   see FLINK-23690 and https://github.com/apache/flink/pull/16744
        if (processingTimestamp >= currentTimestamp) {
            return processingTimestamp - currentTimestamp + 1;
        } else {
            return 0;
        }
    }
}
