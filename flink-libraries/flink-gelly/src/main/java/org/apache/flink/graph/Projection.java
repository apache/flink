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

package org.apache.flink.graph;

import org.apache.flink.api.java.tuple.Tuple6;

/**
 * Result of projection of a connection between two vertices in a bipartite graph.
 * It contains:
 * <ul>
 *     <li>values of source and target vertices</li>
 *     <li>values of both intermediate edges connecting current vertex to another vertex</li>
 *     <li>key and value of an intermediate vertex from an opposite set</li>
 * </ul>
 *
 * @param <EV> the edge value type
 * @param <VK> the key type of vertices of an opposite set
 * @param <VV> the value type of vertices of an opposite set
 * @param <VVC> the vertex value type
 */
public class Projection<VK, VV, EV, VVC> extends Tuple6<VK, VV, EV, EV, VVC, VVC> {
	public Projection() {}

	public Projection(
			Vertex<VK, VV> intermediateVertex,
			EV sourceEdgeValue, EV targetEdgeValue,
			VVC sourceVertexValue, VVC targetVertexValue) {
		this.f0 = intermediateVertex.getId();
		this.f1 = intermediateVertex.getValue();
		this.f2 = sourceEdgeValue;
		this.f3 = targetEdgeValue;
		this.f4 = sourceVertexValue;
		this.f5 = targetVertexValue;
	}

	public VK getIntermediateVertexId() {
		return this.f0;
	}

	public VV getIntermediateVertexValue() {
		return this.f1;
	}

	public EV getSourceEdgeValue() {
		return this.f2;
	}

	public EV getTargetEdgeValue() {
		return this.f3;
	}

	public VVC getsSourceVertexValue() {
		return this.f4;
	}

	public VVC getTargetVertexValue() {
		return this.f5;
	}
}
