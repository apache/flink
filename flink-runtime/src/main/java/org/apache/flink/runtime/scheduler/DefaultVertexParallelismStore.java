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

import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** Maintains the configured parallelisms for vertices, which should be defined by a scheduler. */
public class DefaultVertexParallelismStore implements MutableVertexParallelismStore {
    private final Map<JobVertexID, VertexParallelismInformation> vertexToParallelismInfo =
            new HashMap<>();

    @Override
    public void setParallelismInfo(JobVertexID vertexId, VertexParallelismInformation info) {
        vertexToParallelismInfo.put(vertexId, info);
    }

    @Override
    public VertexParallelismInformation getParallelismInfo(JobVertexID vertexId) {
        return Optional.ofNullable(vertexToParallelismInfo.get(vertexId))
                .orElseThrow(
                        () ->
                                new IllegalStateException(
                                        String.format(
                                                "No parallelism information set for vertex %s",
                                                vertexId)));
    }
}
