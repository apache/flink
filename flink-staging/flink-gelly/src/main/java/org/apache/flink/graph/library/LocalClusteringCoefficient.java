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

package org.apache.flink.graph.library;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.EdgesFunction;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.NeighborsFunctionWithVertexValue;
import org.apache.flink.graph.Vertex;

import java.io.Serializable;
import java.util.HashSet;

@SuppressWarnings("serial")
public class LocalClusteringCoefficient<K extends Comparable<K> & Serializable>
		implements GraphAlgorithm<K, Double, Double> {

	@Override
	public Graph<K, Double, Double> run(Graph<K, Double, Double> input) {

		// Get the neighbors of each vertex in a HashSet
		DataSet<Tuple2<K, HashSet<K>>> neighborhoods = input
				.reduceOnEdges(new NeighborhoodEdgesFunction<K>(), EdgeDirection.OUT);

		// Construct a new graph where the neighborhood is the vertex value
		Graph<K, HashSet<K>, Double> newGraph = input
				.mapVertices(new EmptyVertexMapFunction<K>())
				.joinWithVertices(neighborhoods, new NeighborhoodVertexMapFunction<K>());

		// Calculate clustering coefficient
		DataSet<Tuple2<K, Double>> clusteringCoefficients = newGraph
				.reduceOnNeighbors(new ClusteringCoefficientNeighborsFunction<K>(), EdgeDirection.OUT);

		// Construct a new graph where the clustering coefficient is the vertex value
		Graph<K, Double, Double> result = input
				.joinWithVertices(clusteringCoefficients, new ClusteringCoefficientVertexMapFunction<K>());

		return result;
	}

	private static final class NeighborhoodEdgesFunction<K extends Comparable<K> & Serializable>
			implements EdgesFunction<K, Double, Tuple2<K, HashSet<K>>> {

		@Override
		public Tuple2<K, HashSet<K>> iterateEdges(
				Iterable<Tuple2<K, Edge<K, Double>>> edges) throws Exception {

			K vertexId = null;
			HashSet<K> neighbors = new HashSet<K>();

			for (Tuple2<K, Edge<K, Double>> edge : edges) {
				vertexId = edge.f0;
				neighbors.add(edge.f1.f1);
			}

			return new Tuple2<K, HashSet<K>>(vertexId, neighbors);
		}
	}

	private static final class EmptyVertexMapFunction<K extends Comparable<K> & Serializable>
			implements MapFunction<Vertex<K, Double>, HashSet<K>> {

		@Override
		public HashSet<K> map(Vertex<K, Double> arg0) throws Exception {
			return new HashSet<K>();
		}
	}

	private static final class NeighborhoodVertexMapFunction<K extends Comparable<K> & Serializable>
			implements MapFunction<Tuple2<HashSet<K>, HashSet<K>>, HashSet<K>> {

		@Override
		public HashSet<K> map(Tuple2<HashSet<K>, HashSet<K>> arg) throws Exception {
			return arg.f1;
		}
	}

	private static final class ClusteringCoefficientNeighborsFunction<K extends Comparable<K> & Serializable>
			implements NeighborsFunctionWithVertexValue<K, HashSet<K>, Double, Tuple2<K, Double>> {

		@Override
		public Tuple2<K, Double> iterateNeighbors(Vertex<K, HashSet<K>> vertex,
				Iterable<Tuple2<Edge<K, Double>, Vertex<K, HashSet<K>>>> neighbors) throws Exception {

			int deg = vertex.getValue().size();
			int e = 0;

			// Calculate common neighbor count (e)
			for (Tuple2<Edge<K, Double>, Vertex<K, HashSet<K>>> neighbor : neighbors) {
				// Iterate neighbor's neighbors
				for (K nn : neighbor.f1.f1) {
					if (vertex.getValue().contains(nn)) {
						e++;
					}
				}
			}

			// Calculate clustering coefficient
			double cc;

			if (deg > 1) {
				cc = (double) e / (double) (deg * (deg - 1));
			} else {
				cc = 0.0;
			}

			return new Tuple2<K, Double>(vertex.getId(), cc);
		}
	}

	private static final class ClusteringCoefficientVertexMapFunction<K extends Comparable<K> & Serializable>
			implements MapFunction<Tuple2<Double, Double>, Double> {

		@Override
		public Double map(Tuple2<Double, Double> arg) throws Exception {
			return arg.f1;
		}


	}
}
