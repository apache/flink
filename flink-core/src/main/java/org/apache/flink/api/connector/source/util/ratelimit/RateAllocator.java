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
import java.util.Map;

/**
 * Divides the global dynamic rate limit of a source among its parallel subtasks, e.g. to cope with
 * partition skew or uneven split assignment.
 *
 * <p>The default implementation is {@link DemandAwareRateAllocator}. A custom implementation is
 * selected through {@code source.dynamic-rate-limit.allocator} and must have a public no-arg
 * constructor. It is invoked by the source coordinator after each rate adjustment and must not
 * block.
 */
@PublicEvolving
public interface RateAllocator extends Serializable {

    /**
     * Splits the global rate budget across the subtasks.
     *
     * <p>Implementations should return one positive, finite rate per subtask id, summing to at most
     * {@code totalRecordsPerSecond}. The coordinator replaces a missing, non-positive or non-finite
     * rate with an even share and scales all rates down if they exceed the total, so a faulty
     * allocator degrades to an even split rather than breaking the limit.
     *
     * @param totalRecordsPerSecond the global rate budget across all subtasks.
     * @param subtasks one entry per parallel subtask.
     * @return the rate in records per second for each subtask id.
     */
    Map<Integer, Double> allocate(double totalRecordsPerSecond, List<SubtaskRateInfo> subtasks);
}
