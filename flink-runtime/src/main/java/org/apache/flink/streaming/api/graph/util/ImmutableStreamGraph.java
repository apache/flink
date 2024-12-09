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

package org.apache.flink.streaming.api.graph.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.graph.StreamGraph;

import java.util.HashMap;
import java.util.Map;

/** Helper class that provides read-only StreamGraph. */
@Internal
public class ImmutableStreamGraph {
    private final StreamGraph streamGraph;

    private final Map<Integer, ImmutableStreamNode> immutableStreamNodes;

    public ImmutableStreamGraph(StreamGraph streamGraph) {
        this.streamGraph = streamGraph;
        this.immutableStreamNodes = new HashMap<>();
    }

    public ImmutableStreamNode getStreamNode(Integer vertexId) {
        if (streamGraph.getStreamNode(vertexId) == null) {
            return null;
        }
        return immutableStreamNodes.computeIfAbsent(
                vertexId, id -> new ImmutableStreamNode(streamGraph.getStreamNode(id)));
    }
}
