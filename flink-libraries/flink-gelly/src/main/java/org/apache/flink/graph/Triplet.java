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

import org.apache.flink.api.java.tuple.Tuple5;

/**
 * A Triplet stores and retrieves the edges along with their corresponding source and target vertices.
 * Triplets can be obtained from the input graph via the {@link org.apache.flink.graph.Graph#getTriplets()} method.
 *
 * @param <K> the vertex key type
 * @param <VV> the vertex value type
 * @param <EV> the edge value type
 */
public class Triplet <K, VV, EV> extends Tuple5<K, K, VV, VV, EV> {

	private static final long serialVersionUID = 1L;

	public Triplet() {}

	/**
	 * Constructs a Triplet from a given source vertex, target vertex, and edge.
	 *
	 * @param srcVertex
	 * @param trgVertex
	 * @param edge
	 */
	public Triplet(Vertex<K, VV> srcVertex, Vertex<K, VV> trgVertex, Edge<K, EV> edge) {
		this.f0 = srcVertex.f0;
		this.f2 = srcVertex.f1;
		this.f1 = trgVertex.f0;
		this.f3 = trgVertex.f1;
		this.f4 = edge.f2;
	}

	/**
	 * Constructs a Triplet from its src vertex id, src target id, src vertex value,
	 * src target value and edge value respectively.
	 *
	 * @param srcId
	 * @param trgId
	 * @param srcVal
	 * @param trgVal
	 * @param edgeVal
	 */
	public Triplet(K srcId, K trgId, VV srcVal, VV trgVal, EV edgeVal) {
		super(srcId, trgId, srcVal, trgVal, edgeVal);
	}

	public Vertex<K, VV> getSrcVertex() {
		return new Vertex<>(this.f0, this.f2);
	}

	public Vertex<K, VV> getTrgVertex() {
		return new Vertex<>(this.f1, this.f3);
	}

	public Edge<K, EV> getEdge() {
		return new Edge<>(this.f0, this.f1, this.f4);
	}
}
