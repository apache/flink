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

package flink.graphs;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.util.Collector;

public class Graph<K extends Comparable<K>, VV, EV> {

	private final DataSet<Vertex<K, VV>> vertices;
	
	private final DataSet<Edge<K, EV>> edges;
	
	/** a graph is directed by default */
	private boolean isUndirected = false; 
	
	
	public Graph(DataSet<Vertex<K, VV>> vertices, DataSet<Edge<K, EV>> edges) {
		this.vertices = vertices;
		this.edges = edges;
	}
	
	public Graph(DataSet<Vertex<K, VV>> vertices, DataSet<Edge<K, EV>> edges, 
			boolean undirected) {
		this.vertices = vertices;
		this.edges = edges;
		this.isUndirected = undirected;
	}


	public DataSet<Vertex<K, VV>> getVertices() {
		return vertices;
	}
	
	public DataSet<Edge<K, EV>> getEdges() {
		return edges;
	}
	
	/**
	 * Convert the directed graph into an undirected graph
	 * by adding all inverse-direction edges.
	 * 
	 */
	public Graph<K, VV, EV> getUndirected() throws UnsupportedOperationException {
		if (this.isUndirected) {
			throw new UnsupportedOperationException("");
		}
		else {
			DataSet<Edge<K, EV>> undirectedEdges = edges.flatMap(
					new FlatMapFunction<Edge<K,EV>, Edge<K,EV>>() {
						private static final long serialVersionUID = 1L;

						public void flatMap(Edge<K, EV> edge, Collector<Edge<K, EV>> out){
							out.collect(edge);
							out.collect(edge.reverse());
						}
			});
			return new Graph<K, VV, EV>(vertices, undirectedEdges);
		}
	}
	
	public static <K extends Comparable<K>, VV, EV> Graph<K, VV, EV> 
		create(DataSet<Vertex<K, VV>> vertices, DataSet<Edge<K, EV>> edges) {
		return new Graph<>(vertices, edges);
		
	}
}
