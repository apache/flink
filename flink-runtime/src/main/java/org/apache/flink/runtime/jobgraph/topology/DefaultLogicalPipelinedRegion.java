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

package org.apache.flink.runtime.jobgraph.topology;

import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Set of {@link LogicalVertex} that are connected through pipelined {@link LogicalResult}. */
public class DefaultLogicalPipelinedRegion implements LogicalPipelinedRegion {

    private final Map<JobVertexID, LogicalVertex> vertexById;

    public DefaultLogicalPipelinedRegion(final Set<? extends LogicalVertex> logicalVertices) {
        checkNotNull(logicalVertices);

        this.vertexById =
                logicalVertices.stream()
                        .collect(Collectors.toMap(LogicalVertex::getId, Function.identity()));
    }

    @Override
    public Iterable<? extends LogicalVertex> getVertices() {
        return vertexById.values();
    }

    @Override
    public LogicalVertex getVertex(JobVertexID vertexId) {
        return vertexById.get(vertexId);
    }

    @Override
    public boolean contains(JobVertexID vertexId) {
        return vertexById.containsKey(vertexId);
    }

    @Override
    public String toString() {
        return "DefaultLogicalPipelinedRegion{"
                + "vertexIDs="
                + vertexById.values().stream()
                        .map(LogicalVertex::getId)
                        .collect(Collectors.toList())
                + '}';
    }
}
