/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler.adaptive.allocator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.scheduler.TaskBalancedExecutionSlotSharingGroupBuilder;
import org.apache.flink.runtime.scheduler.adaptive.allocator.SlotSharingSlotAllocator.ExecutionSlotSharingGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** The implementation of the {@link SlotSharingResolver} based on tasks balanced resolver. */
@Internal
public enum TaskBalancedSlotSharingResolver implements SlotSharingResolver {
    INSTANCE;

    @Override
    public List<ExecutionSlotSharingGroup> getExecutionSlotSharingGroups(
            JobInformation jobInformation, VertexParallelism vertexParallelism) {
        return new TaskBalancedExecutionSlotSharingGroupBuilder(
                        getAllVertices(jobInformation, vertexParallelism),
                        jobInformation.getSlotSharingGroups(),
                        jobInformation.getCoLocationGroups())
                .build().values().stream()
                        .distinct()
                        .map(
                                fromGroup ->
                                        new ExecutionSlotSharingGroup(
                                                fromGroup.getSlotSharingGroup(),
                                                fromGroup.getExecutionVertexIds()))
                        .collect(Collectors.toList());
    }

    static Map<JobVertexID, List<ExecutionVertexID>> getAllVertices(
            JobInformation jobInformation, VertexParallelism vertexParallelism) {
        final Map<JobVertexID, List<ExecutionVertexID>> jobVertexToExecutionVertices =
                new HashMap<>();
        for (SlotSharingGroup slotSharingGroup : jobInformation.getSlotSharingGroups()) {
            slotSharingGroup
                    .getJobVertexIds()
                    .forEach(
                            jobVertexId -> {
                                int parallelism = vertexParallelism.getParallelism(jobVertexId);
                                for (int subtaskIdx = 0; subtaskIdx < parallelism; subtaskIdx++) {
                                    jobVertexToExecutionVertices
                                            .computeIfAbsent(
                                                    jobVertexId, ignored -> new ArrayList<>())
                                            .add(new ExecutionVertexID(jobVertexId, subtaskIdx));
                                }
                            });
        }
        return jobVertexToExecutionVertices;
    }
}
