/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler.adaptive.timeline;

import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.scheduler.VertexParallelismStore;
import org.apache.flink.runtime.scheduler.adaptive.JobGraphJobInformation;
import org.apache.flink.runtime.scheduler.adaptive.allocator.JobInformation;

import org.apache.flink.shaded.guava33.com.google.common.collect.Iterables;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Testing utils for {@link JobInformation}. */
public class TestingJobInformation implements JobInformation {

    private final Set<SlotSharingGroup> slotSharingGroups;
    private final Set<CoLocationGroup> coLocationGroups;
    private final Map<JobVertexID, JobVertex> jobVerticesById;
    private final VertexParallelismStore vertexParallelismStore;

    public TestingJobInformation(
            Set<SlotSharingGroup> slotSharingGroups,
            List<JobVertex> jobVertices,
            VertexParallelismStore vertexParallelismStore) {
        this.slotSharingGroups = new HashSet<>(slotSharingGroups);
        this.coLocationGroups = new HashSet<>();
        this.jobVerticesById =
                jobVertices.stream()
                        .collect(Collectors.toMap(JobVertex::getID, Function.identity()));
        this.vertexParallelismStore = vertexParallelismStore;
    }

    @Override
    public Collection<SlotSharingGroup> getSlotSharingGroups() {
        return slotSharingGroups;
    }

    @Override
    public Collection<CoLocationGroup> getCoLocationGroups() {
        return coLocationGroups;
    }

    @Override
    public JobInformation.VertexInformation getVertexInformation(JobVertexID jobVertexId) {
        return new JobGraphJobInformation.JobVertexInformation(
                jobVerticesById.get(jobVertexId),
                vertexParallelismStore.getParallelismInfo(jobVertexId));
    }

    @Override
    public Iterable<JobInformation.VertexInformation> getVertices() {
        return Iterables.transform(
                jobVerticesById.values(), (vertex) -> getVertexInformation(vertex.getID()));
    }

    @Override
    public VertexParallelismStore getVertexParallelismStore() {
        return vertexParallelismStore;
    }
}
