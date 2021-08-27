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

package org.apache.flink.runtime.topology;

/** Extends the {@link BaseTopology} by pipelined regions. */
public interface Topology<
                VID extends VertexID,
                RID extends ResultID,
                V extends Vertex<VID, RID, V, R>,
                R extends Result<VID, RID, V, R>,
                PR extends PipelinedRegion<VID, RID, V, R>>
        extends BaseTopology<VID, RID, V, R> {

    /**
     * Returns all pipelined regions in this topology.
     *
     * @return Iterable over pipelined regions in this topology
     */
    Iterable<? extends PR> getAllPipelinedRegions();

    /**
     * The pipelined region for a specified vertex.
     *
     * @param vertexId the vertex id identifying the vertex for which the pipelined region should be
     *     returned
     * @return the pipelined region of the vertex
     * @throws IllegalArgumentException if there is no vertex in this topology with the specified
     *     vertex id
     */
    default PR getPipelinedRegionOfVertex(VID vertexId) {
        throw new UnsupportedOperationException();
    }
}
