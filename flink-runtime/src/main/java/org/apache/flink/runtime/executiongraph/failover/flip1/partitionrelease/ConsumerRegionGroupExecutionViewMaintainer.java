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
 * limitations under the License
 */

package org.apache.flink.runtime.executiongraph.failover.flip1.partitionrelease;

import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.runtime.scheduler.strategy.SchedulingPipelinedRegion;

import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Track the finished progress of {@link ConsumerRegionGroupExecutionView}s.
 *
 * <p>NOTE: It only contains {@link SchedulingPipelinedRegion}s that have {@link
 * ConsumedPartitionGroup}s.
 */
class ConsumerRegionGroupExecutionViewMaintainer {

    /**
     * The set of {@link ConsumerRegionGroupExecutionView}s that a SchedulingPipelinedRegion belongs
     * to.
     */
    private final Map<SchedulingPipelinedRegion, Set<ConsumerRegionGroupExecutionView>>
            executionViewByRegion = new HashMap<>();

    ConsumerRegionGroupExecutionViewMaintainer(
            Iterable<ConsumerRegionGroupExecutionView> executionViews) {

        for (ConsumerRegionGroupExecutionView executionView : executionViews) {
            for (SchedulingPipelinedRegion region : executionView) {
                executionViewByRegion
                        .computeIfAbsent(
                                region, r -> Collections.newSetFromMap(new IdentityHashMap<>()))
                        .add(executionView);
            }
        }
    }

    void regionFinished(SchedulingPipelinedRegion region) {
        for (ConsumerRegionGroupExecutionView executionView :
                executionViewByRegion.getOrDefault(region, Collections.emptySet())) {
            executionView.regionFinished(region);
        }
    }

    void regionUnfinished(SchedulingPipelinedRegion region) {
        for (ConsumerRegionGroupExecutionView executionView :
                executionViewByRegion.getOrDefault(region, Collections.emptySet())) {
            executionView.regionUnfinished(region);
        }
    }
}
