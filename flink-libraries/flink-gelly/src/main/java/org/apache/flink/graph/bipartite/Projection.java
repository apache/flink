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

package org.apache.flink.graph.bipartite;

import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.graph.Vertex;

/**
 * The edge value of a full bipartite projection. This contains:
 *
 * <ul>
 *   <li>the ID and vertex value of the connecting vertex
 *   <li>the vertex value for the source and target vertex
 *   <li>both edge values from the bipartite edges
 * </ul>
 *
 * @param <KC> the key type of connecting vertices
 * @param <VVC> the vertex value type of connecting vertices
 * @param <VV> the vertex value type of top or bottom vertices
 * @param <EV> the edge value type from bipartite edges
 */
public class Projection<KC, VVC, VV, EV> extends Tuple6<KC, VVC, VV, VV, EV, EV> {

    public Projection() {}

    public Projection(
            Vertex<KC, VVC> connectingVertex,
            VV sourceVertexValue,
            VV targetVertexValue,
            EV sourceEdgeValue,
            EV targetEdgeValue) {
        this.f0 = connectingVertex.getId();
        this.f1 = connectingVertex.getValue();
        this.f2 = sourceVertexValue;
        this.f3 = targetVertexValue;
        this.f4 = sourceEdgeValue;
        this.f5 = targetEdgeValue;
    }

    public KC getIntermediateVertexId() {
        return this.f0;
    }

    public VVC getIntermediateVertexValue() {
        return this.f1;
    }

    public VV getsSourceVertexValue() {
        return this.f2;
    }

    public VV getTargetVertexValue() {
        return this.f3;
    }

    public EV getSourceEdgeValue() {
        return this.f4;
    }

    public EV getTargetEdgeValue() {
        return this.f5;
    }
}
