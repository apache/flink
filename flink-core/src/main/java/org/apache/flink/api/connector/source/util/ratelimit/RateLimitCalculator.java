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

package org.apache.flink.api.connector.source.util.ratelimit;

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;
import java.util.List;

/**
 * Decides the new global target rate of a dynamically rate limited source.
 *
 * <p>The source coordinator calls {@link #computeNewRate} after every completed checkpoint. How
 * that global rate is then divided among the subtasks is decided by the {@link RateAllocations}.
 *
 * <p>Implementations must be {@link Serializable}, are invoked from the coordinator's single event
 * thread, and may keep state between calls. A custom implementation can be set through {@code
 * source.dynamic-rate-limit.calculator} (fully qualified class name with a public no-arg
 * constructor) or through {@link DynamicRateLimitParams}.
 */
@PublicEvolving
public interface RateLimitCalculator extends Serializable {

    /**
     * @param recentDurationsMillis rolling window of checkpoint durations, oldest first.
     * @param currentThroughput the current observed throughput (records/sec, summed across
     *     subtasks); callers fall back to the last target rate when no throughput has been observed
     *     yet.
     * @param checkpointIntervalMillis the checkpoint interval; non-positive or unbounded values
     *     mean there is no periodic checkpointing.
     * @param params the configured thresholds and factors.
     * @return the new global target rate in records per second.
     */
    double computeNewRate(
            List<Long> recentDurationsMillis,
            double currentThroughput,
            long checkpointIntervalMillis,
            DynamicRateLimitParams params);
}
