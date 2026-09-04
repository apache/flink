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

import org.apache.flink.annotation.Internal;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Divides the global dynamic rate limit among the parallel subtasks of a source, based on observed
 * throughput, to cope with partition skew and uneven split assignment.
 *
 * <p>This is a max-min fair allocation. A subtask whose observed throughput stays below {@value
 * #SATURATION_THRESHOLD} of its allocation is demand-limited: it cannot use its full share, so its
 * demand is taken as the observed throughput times {@value #HEADROOM} (so it can still grow).
 * Subtasks that used (nearly) all of their allocation, or that have not reported yet, are treated
 * as unbounded. Demand-limited subtasks are satisfied first and the remaining budget is split
 * evenly among the unbounded ones. If every subtask is demand-limited, the leftover is spread
 * evenly so that each can still ramp up.
 *
 * <p>A demand never drops below {@value #MIN_SHARE_FRACTION} of the even share, so an idle subtask
 * (e.g. one without splits yet) keeps enough rate to start up and be measured again, instead of
 * being starved at zero.
 *
 * <p>The result has one entry per subtask. Every rate is positive and finite, and the rates sum to
 * at most the total, provided the total is positive and finite.
 */
@Internal
public final class RateAllocations {

    static final double SATURATION_THRESHOLD = 0.9;
    static final double HEADROOM = 1.2;
    static final double MIN_SHARE_FRACTION = 0.1;

    private RateAllocations() {}

    /**
     * @param totalRecordsPerSecond the global rate budget across all subtasks.
     * @param subtasks one entry per parallel subtask.
     * @return the rate in records per second for each subtask id.
     */
    public static Map<Integer, Double> allocate(
            double totalRecordsPerSecond, List<SubtaskRateInfo> subtasks) {
        final Map<Integer, Double> result = new HashMap<>();
        if (subtasks.isEmpty()) {
            return result;
        }

        final double minDemand = MIN_SHARE_FRACTION * totalRecordsPerSecond / subtasks.size();
        final Map<Integer, Double> demands = new HashMap<>();
        final Set<Integer> pending = new LinkedHashSet<>();
        for (SubtaskRateInfo subtask : subtasks) {
            pending.add(subtask.getSubtaskId());
            if (isDemandLimited(subtask)) {
                demands.put(
                        subtask.getSubtaskId(),
                        Math.max(subtask.getObservedRecordsPerSecond() * HEADROOM, minDemand));
            }
        }

        double remaining = totalRecordsPerSecond;
        boolean changed = true;
        while (changed && !pending.isEmpty()) {
            changed = false;
            final double share = remaining / pending.size();
            for (final Iterator<Integer> it = pending.iterator(); it.hasNext(); ) {
                final Integer id = it.next();
                final Double demand = demands.get(id);
                if (demand != null && demand <= share) {
                    result.put(id, demand);
                    remaining -= demand;
                    it.remove();
                    changed = true;
                }
            }
        }

        if (!pending.isEmpty()) {
            final double share = remaining / pending.size();
            for (Integer id : pending) {
                result.put(id, share);
            }
        } else if (remaining > 0) {
            final double bonus = remaining / result.size();
            result.replaceAll((id, rate) -> rate + bonus);
        }
        return result;
    }

    private static boolean isDemandLimited(SubtaskRateInfo subtask) {
        return subtask.hasAllocation()
                && subtask.hasObservation()
                && subtask.getObservedRecordsPerSecond()
                        < subtask.getAllocatedRecordsPerSecond() * SATURATION_THRESHOLD;
    }
}
