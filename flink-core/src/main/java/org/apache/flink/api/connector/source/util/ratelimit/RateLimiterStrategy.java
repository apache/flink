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

package org.apache.flink.api.connector.source.util.ratelimit;

import org.apache.flink.annotation.Experimental;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A factory for {@link RateLimiter RateLimiters} which apply rate-limiting to a source sub-task.
 */
@Experimental
public interface RateLimiterStrategy extends Serializable {

    /**
     * Creates a {@link RateLimiter} that lets records through with rate proportional to the
     * parallelism. This method will be called once per source subtask. The cumulative rate over all
     * rate limiters for a source must not exceed the rate limit configured for the strategy.
     */
    RateLimiter createRateLimiter(int parallelism);

    /**
     * Creates a {@code RateLimiterStrategy} that is limiting the number of records per second.
     *
     * @param recordsPerSecond The number of records produced per second. The actual number of
     *     produced records is subject to rounding due to dividing the number of produced records
     *     among the parallel instances.
     */
    static RateLimiterStrategy perSecond(double recordsPerSecond) {
        return parallelism -> new GuavaRateLimiter(recordsPerSecond / parallelism);
    }

    /**
     * Creates a {@code RateLimiterStrategy} that is limiting the number of records per checkpoint.
     *
     * @param recordsPerCheckpoint The number of records produced per checkpoint. This value has to
     *     be greater or equal to parallelism. The actual number of produced records is subject to
     *     rounding due to dividing the number of produced records among the parallel instances.
     */
    static RateLimiterStrategy perCheckpoint(int recordsPerCheckpoint) {
        return parallelism -> {
            int recordsPerSubtask = recordsPerCheckpoint / parallelism;
            checkArgument(
                    recordsPerSubtask > 0,
                    "recordsPerCheckpoint has to be greater or equal to parallelism. "
                            + "Either decrease the parallelism or increase the number of "
                            + "recordsPerCheckpoint.");
            return new GatedRateLimiter(recordsPerSubtask);
        };
    }

    /** Creates a convenience {@code RateLimiterStrategy} that is not limiting the records rate. */
    static RateLimiterStrategy noOp() {
        return parallelism -> new NoOpRateLimiter();
    }
}
