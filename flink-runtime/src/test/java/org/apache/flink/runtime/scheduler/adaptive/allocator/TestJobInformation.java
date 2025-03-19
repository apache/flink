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

package org.apache.flink.runtime.scheduler.adaptive.allocator;

import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class TestJobInformation implements JobInformation {

    private final Map<JobVertexID, VertexInformation> vertexIdToInformation;
    private final Set<SlotSharingGroup> slotSharingGroups;
    private final Set<CoLocationGroup> coLocationGroups;

    public TestJobInformation(Collection<? extends VertexInformation> vertexIdToInformation) {
        this.vertexIdToInformation =
                vertexIdToInformation.stream()
                        .collect(
                                Collectors.toMap(
                                        VertexInformation::getJobVertexID, Function.identity()));
        this.slotSharingGroups =
                vertexIdToInformation.stream()
                        .map(VertexInformation::getSlotSharingGroup)
                        .collect(Collectors.toSet());
        this.coLocationGroups =
                Collections.unmodifiableSet(
                        vertexIdToInformation.stream()
                                .map(VertexInformation::getCoLocationGroup)
                                .filter(Objects::nonNull)
                                .collect(Collectors.toSet()));
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
    public VertexInformation getVertexInformation(JobVertexID jobVertexId) {
        return vertexIdToInformation.get(jobVertexId);
    }

    @Override
    public Iterable<VertexInformation> getVertices() {
        return vertexIdToInformation.values();
    }
}
