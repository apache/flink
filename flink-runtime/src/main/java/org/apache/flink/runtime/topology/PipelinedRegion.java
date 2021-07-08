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

package org.apache.flink.runtime.topology;

/**
 * A pipelined region is a set of vertices connected via pipelined data exchanges.
 *
 * @param <VID> the type of the vertex ids
 * @param <RID> the type of the result ids
 * @param <V> the type of the vertices
 * @param <R> the type of the result
 */
public interface PipelinedRegion<
        VID extends VertexID,
        RID extends ResultID,
        V extends Vertex<VID, RID, V, R>,
        R extends Result<VID, RID, V, R>> {

    /**
     * Returns vertices that are in this pipelined region.
     *
     * @return Iterable over all vertices in this pipelined region
     */
    Iterable<? extends V> getVertices();

    /**
     * Returns the vertex with the specified vertex id.
     *
     * @param vertexId the vertex id used to look up the vertex
     * @return the vertex with the specified id
     * @throws IllegalArgumentException if there is no vertex in this pipelined region with the
     *     specified vertex id
     */
    V getVertex(VID vertexId);

    /**
     * Returns whether the vertex is in this pipelined region or not.
     *
     * @param vertexId the vertex id used to look up
     * @return the vertex is in this pipelined region or not
     */
    boolean contains(VID vertexId);
}
