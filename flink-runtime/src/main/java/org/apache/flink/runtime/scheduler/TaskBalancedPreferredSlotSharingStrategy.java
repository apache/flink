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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;
import org.apache.flink.runtime.topology.Vertex;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

/**
 * This strategy tries to get a balanced tasks scheduling. Execution vertices, which are belong to
 * the same SlotSharingGroup, tend to be put evenly in each ExecutionSlotSharingGroup. Co-location
 * constraints will be respected.
 */
public class TaskBalancedPreferredSlotSharingStrategy extends AbstractSlotSharingStrategy {

    public static final Logger LOG =
            LoggerFactory.getLogger(TaskBalancedPreferredSlotSharingStrategy.class);

    TaskBalancedPreferredSlotSharingStrategy(
            final SchedulingTopology topology,
            final Set<SlotSharingGroup> slotSharingGroups,
            final Set<CoLocationGroup> coLocationGroups) {
        super(topology, slotSharingGroups, coLocationGroups);
    }

    @Override
    protected Map<ExecutionVertexID, ExecutionSlotSharingGroup> computeExecutionSlotSharingGroups(
            SchedulingTopology schedulingTopology) {
        return new TaskBalancedExecutionSlotSharingGroupBuilder(
                        getExecutionVertices(schedulingTopology, Vertex::getId),
                        this.logicalSlotSharingGroups,
                        this.coLocationGroups)
                .build();
    }

    public static class Factory implements SlotSharingStrategy.Factory {

        public TaskBalancedPreferredSlotSharingStrategy create(
                final SchedulingTopology topology,
                final Set<SlotSharingGroup> slotSharingGroups,
                final Set<CoLocationGroup> coLocationGroups) {

            return new TaskBalancedPreferredSlotSharingStrategy(
                    topology, slotSharingGroups, coLocationGroups);
        }
    }
}
