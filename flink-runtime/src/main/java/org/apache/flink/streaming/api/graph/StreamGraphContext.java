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

package org.apache.flink.streaming.api.graph;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.graph.util.ImmutableStreamGraph;
import org.apache.flink.streaming.api.graph.util.StreamEdgeUpdateRequestInfo;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;

import javax.annotation.Nullable;

import java.util.List;

/**
 * Defines a context for optimizing and working with a read-only view of a StreamGraph. It provides
 * methods to modify StreamEdges and StreamNodes within the StreamGraph.
 */
@Internal
public interface StreamGraphContext {

    /**
     * Returns a read-only view of the StreamGraph.
     *
     * @return a read-only view of the StreamGraph.
     */
    ImmutableStreamGraph getStreamGraph();

    /**
     * Retrieves the {@link StreamOperatorFactory} for the specified stream node id.
     *
     * @param streamNodeId the id of the stream node
     * @return the {@link StreamOperatorFactory} associated with the given {@code streamNodeId}, or
     *     {@code null} if no operator factory is available.
     */
    @Nullable
    StreamOperatorFactory<?> getOperatorFactory(Integer streamNodeId);

    /**
     * Atomically modifies stream edges within the StreamGraph.
     *
     * <p>This method ensures that all the requested modifications to stream edges are applied
     * atomically. This means that if any modification fails, none of the changes will be applied,
     * maintaining the consistency of the StreamGraph.
     *
     * @param requestInfos the stream edges to be modified.
     * @return true if all modifications were successful and applied atomically, false otherwise.
     */
    boolean modifyStreamEdge(List<StreamEdgeUpdateRequestInfo> requestInfos);
}
