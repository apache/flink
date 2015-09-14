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

package org.apache.flink.graph.utils;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.Iterator;

/**
 * Class containing operations that are generally applicable to skewed graphs -
 * split/merge methods as well as methods for determining high - degree vertices.
 */
public class SplitVertex {

	/**
	 * Method that filters out the vertices with high - degree, given a data set of vertex IDs and their degrees.
	 *
	 * @param xMin - the threshold for differentiating a high - degree vertex from a low -degree vertex
	 * @param verticesWithDegrees - the data set of Tuple2 vertex ID - degree
	 * @param <K>
	 * @param <VV>
	 * @param <EV>
	 *
	 * @return a data set of skewed vertices
	 */
	public static <K extends Comparable<K> & Serializable, VV extends Serializable, EV extends Serializable>
		DataSet<Vertex<K, NullValue>> determineSkewedVertices(final double xMin,
														DataSet<Tuple2<K, Long>> verticesWithDegrees) throws Exception {

		return verticesWithDegrees
				.flatMap(new FlatMapFunction<Tuple2<K, Long>, Vertex<K, NullValue>>() {

					@Override
					public void flatMap(Tuple2<K, Long> vertexIdDegree,
										Collector<Vertex<K, NullValue>> collector) throws Exception {
						if(vertexIdDegree.f1 > xMin) {
							collector.collect(new Vertex<K, NullValue>(vertexIdDegree.f0, NullValue.getInstance()));
						}
					}
				});
	}

	/**
	 * Method that takes the skewed vertices and splits them into alpha subvertices recursively,
	 * forming a tree of `level` levels.
	 *
	 * The skewed vertex can be either the source or the destination of an edge. We hash its neighbor and add _ followed by
	 * a unique identifier to the vertex ID.
	 *
	 * @param skewedVertices
	 * @param graph - the initial graph
	 * @param alpha - the number of subvertices in which a node should be split
	 * @param level - the number of levels the tree will have
	 * @param xMin - the threshold for determining skewed vertices
	 *
	 * @return a data set of edges formed from the skewed vertices and their subnodes
	 */
	public static <VV extends Serializable, EV extends Serializable> Graph<String, Tuple2<String, VV>, EV> treeDeAggregate(
			DataSet<Vertex<String, NullValue>> skewedVertices,
			Graph<String, VV, EV> graph,
			final Integer alpha, Integer level,
			final double xMin) {

		DataSet<Edge<String, EV>> edgesWithSkewedVertices = graph.getEdges();
		DataSet<Vertex<String, VV>> vertices = graph.getVertices();

		for(int i=0; i<level; i++) {
			edgesWithSkewedVertices = edgesWithSkewedVertices.coGroup(skewedVertices).where(0).equalTo(0)
					.with(new SplitSourceCoGroup(i, alpha))
					.coGroup(skewedVertices).where(1).equalTo(0)
					.with(new SplitTargetCoGroup(i, alpha));

			// update the data set of vertices so that the skewed, split vertices will also contain the original
			// value of the vertex
			vertices = vertices.coGroup(skewedVertices).where(0).equalTo(0)
					.with(new SplitVerticesCoGroup<VV>(alpha));

			// update the skewed vertices to also contain their subvertices
			skewedVertices = skewedVertices.flatMap(new FlatMapFunction<Vertex<String, NullValue>, Vertex<String, NullValue>>() {
				@Override
				public void flatMap(Vertex<String, NullValue> vertex,
									Collector<Vertex<String, NullValue>> collector) throws Exception {

					for (int i = 0; i < alpha; i++) {
						collector.collect(new Vertex<String, NullValue>(vertex.getId() + "_" + i, NullValue.getInstance()));
					}
				}
			});

			// see whether the subnodes are still skewed and then continue splitting until max level is reached
			DataSet<Tuple2<String, Long>> skewedNodesWithDegrees = getDegreesSkewedNodes(edgesWithSkewedVertices,
					skewedVertices);
			skewedVertices = skewedNodesWithDegrees.flatMap(new FilterSkewedVertices(xMin));
		}

		DataSet<Vertex<String, Tuple2<String, VV>>> newVertices = vertices.map(new MapFunction<Vertex<String, VV>, Vertex<String, Tuple2<String, VV>>>() {

			@Override
			public Vertex<String, Tuple2<String, VV>> map(Vertex<String, VV> vertex) throws Exception {
				if (vertex.getId().indexOf("_") <= -1) {
					return new Vertex<String, Tuple2<String, VV>>(vertex.getId(), new Tuple2<String, VV>(vertex.getId(), vertex.getValue()));
				} else {
					int pos = vertex.getId().indexOf("_");

					return new Vertex<String, Tuple2<String, VV>>(vertex.getId(), new Tuple2<String, VV>(
							vertex.getId().substring(0, pos), vertex.getValue()));
				}
			}
		});

		return Graph.fromDataSet(newVertices, edgesWithSkewedVertices, graph.getContext());
	}

	private static <EV extends Serializable> DataSet<Tuple2<String, Long>> getDegreesSkewedNodes(
			DataSet<Edge<String, EV>> edgesWithSkewedVertices,
			DataSet<Vertex<String, NullValue>> skewedVertices) {

		DataSet<Edge<String, EV>> edgesWithSplitSource = skewedVertices
				.join(edgesWithSkewedVertices).where(0).equalTo(0)
				.with(new ProjectDuplicateEdge<EV>());

		DataSet<Edge<String, EV>> edgeWithSplitTarget = skewedVertices
				.join(edgesWithSkewedVertices).where(0).equalTo(1)
				.with(new ProjectDuplicateEdge<EV>());

		DataSet<Edge<String, EV>> splitEdges = edgesWithSplitSource.union(edgeWithSplitTarget).distinct();

		return splitEdges.groupBy(0).reduceGroup(new GroupReduceFunction<Edge<String, EV>, Tuple2<String, Long>>() {

			@Override
			public void reduce(Iterable<Edge<String, EV>> edges,
							Collector<Tuple2<String, Long>> collector) throws Exception {

				Iterator<Edge<String, EV>> edgeIterator = edges.iterator();
				Edge<String, EV> next = null;
				long count = 0;
				String id = null;

				while (edgeIterator.hasNext()) {
					next = edgeIterator.next();
					id = next.getSource();
					count++;
				}

				collector.collect(new Tuple2<String, Long>(id, count));
			}
		});
	}

	/**
	 * Method that identifies the split vertices and adds up the partial results.
	 *
	 * @param resultedVertices the vertices resulted from the computation of the algorithm
	 * @param level the number of levels in which a vertex is split
	 * @param groupReduce the function used to combine the values
	 *
	 * @return a data set of aggregated vertices containing the final result
	 */
	public static <VV extends Serializable> DataSet<Vertex<String, VV>> treeAggregate(DataSet<Vertex<String, VV>> resultedVertices,
																					Integer level,
																					GroupReduceFunction<Vertex<String, VV>, Vertex<String, VV>> groupReduce) {

		DataSet<Vertex<String, VV>> regularVertices = resultedVertices.filter(new FilterFunction<Vertex<String, VV>>() {

			@Override
			public boolean filter(Vertex<String, VV> vertex) throws Exception {

				return (vertex.getId().indexOf("_") <= -1);
			}
		});

		DataSet<Vertex<String, VV>> splitVertices = resultedVertices.filter(new FilterFunction<Vertex<String, VV>>() {

			@Override
			public boolean filter(Vertex<String, VV> vertex) throws Exception {

				return (vertex.getId().indexOf("_") > -1);
			}
		});

		// add up the partial values only for the split vertices
		for(int i = 0; i < level; i++) {
			splitVertices = splitVertices.flatMap(new FlatMapFunction<Vertex<String, VV>, Vertex<String, VV>>() {

				@Override
				public void flatMap(Vertex<String, VV> vertex, Collector<Vertex<String, VV>> collector) throws Exception {
					int pos = vertex.getId().lastIndexOf("_");
					if (pos >= 0 ) {
						collector.collect(new Vertex<String, VV>(vertex.getId().substring(0, pos), vertex.getValue()));
					} else {
						collector.collect(vertex);
					}
				}
			}).groupBy(0).reduceGroup(groupReduce);
		}

		return regularVertices.union(splitVertices);
	}

	/**
	 * Modify the edges to become associated with the initial vertices.
	 *
	 * @param splitEdges
	 * @param <EV>
	 * @return
	 */
	public static <EV extends Serializable> DataSet<Edge<String, EV>> cleanupEdges(DataSet<Edge<String, EV>> splitEdges) {

		return splitEdges.map(new MapFunction<Edge<String, EV>, Edge<String, EV>>() {

			@Override
			public Edge<String, EV> map(Edge<String, EV> edge) throws Exception {

				String srcId = edge.getSource();
				String trgId = edge.getTarget();

				int posUnderscoreSrc = srcId.indexOf("_");
				int posUnderscoreTrg = trgId.indexOf("_");

				if (posUnderscoreSrc > -1) {
					srcId = srcId.substring(0, posUnderscoreSrc);
				}

				if (posUnderscoreTrg > -1) {
					trgId = trgId.substring(0, posUnderscoreTrg);
				}

				return new Edge<String, EV>(srcId, trgId, edge.getValue());
			}
		});
	}

	/**
	 * Method that avoids performing a second treeDeAggregate when you just want to propagate the vertex
	 * values to the split vertices for algorithms that need more than one pass through the graph.
	 *
	 * Another treeDeAggregate would just add overhead since the vertices and edges remain the same
	 * and just the vertex values change.
	 *
	 * splitVertices will be joined with aggregatedVertices (via KeySelector) to return the id and the tag of
	 * the splitVertex along with the value of the aggregated vertex.
	 *
	 * @param splitVertices
	 * @param aggregatedVertices
	 * @param <VV>
	 *
	 * @return updated vertices
	 */
	public static <VV extends Serializable> DataSet<Vertex<String, Tuple2<String, VV>>> propagateValuesToSplitVertices(
			DataSet<Vertex<String, Tuple2<String, VV>>> splitVertices,
			DataSet<Vertex<String, VV>> aggregatedVertices) {

		return splitVertices.join(aggregatedVertices)
				.where(new KeySelector<Vertex<String, Tuple2<String, VV>>, String>() {
					@Override
					public String getKey(Vertex<String, Tuple2<String, VV>> vertex) throws Exception {
						int pos = vertex.getId().indexOf("_");

						if (pos > -1) {
							return vertex.getId().substring(0, pos);
						} else {
							return vertex.getId();
						}
					}
				}).equalTo(0).with(new JoinFunction<Vertex<String, Tuple2<String, VV>>,
						Vertex<String, VV>, Vertex<String, Tuple2<String, VV>>>() {

					@Override
					public Vertex<String, Tuple2<String, VV>> join(Vertex<String, Tuple2<String, VV>> splitVertex,
																Vertex<String, VV> aggregatedVertex) throws Exception {
						return new Vertex<String, Tuple2<String, VV>>(splitVertex.getId(),
								new Tuple2<String, VV>(splitVertex.getValue().f0, aggregatedVertex.getValue()));
					}
				});
	}

	// *************************************************************************
	// Node Splitting UDFs
	// *************************************************************************

	private static class FilterSkewedVertices implements FlatMapFunction<Tuple2<String, Long>, Vertex<String, NullValue>> {

		double xMin;

		public FilterSkewedVertices(double xMin) {
			this.xMin = xMin;
		}

		@Override
		public void flatMap(Tuple2<String, Long> verticesWithDegrees,
							Collector<Vertex<String, NullValue>> collector) throws Exception {
			if(verticesWithDegrees.f1 > xMin) {
				collector.collect(new Vertex<String, NullValue>(verticesWithDegrees.f0, NullValue.getInstance()));
			}
		}
	}

	private static final class  SplitSourceCoGroup<EV> implements
			CoGroupFunction<Edge<String, EV>, Vertex<String, NullValue>, Edge<String, EV>> {

		int currentLevel;
		int alpha;

		public SplitSourceCoGroup(int currentLevel, int alpha) {
			this.currentLevel = currentLevel;
			this.alpha = alpha;
		}

		@Override
		public void coGroup(Iterable<Edge<String, EV>> iterableEdges,
							Iterable<Vertex<String, NullValue>> iterableVertices,
							Collector<Edge<String, EV>> collector) throws Exception {
			Iterator<Vertex<String, NullValue>> vertexIterator = iterableVertices.iterator();
			Iterator<Edge<String, EV>> edgeIterator = iterableEdges.iterator();

			Vertex<String, NullValue> vertex = null;

			if(vertexIterator.hasNext()) {
				vertex = vertexIterator.next();
			}

			while(edgeIterator.hasNext()) {
				Edge<String, EV> edge = edgeIterator.next();

				int targetHashCode = edge.getTarget().hashCode();

				// avoid having the same hash code from one level to the other
				for(int i=0; i < currentLevel; i++) {
					targetHashCode = Sha.hash256(targetHashCode + "").hashCode();
				}

				if(targetHashCode < 0) {
					targetHashCode *= -1;
				}

				if(vertex != null) {
					collector.collect(new Edge<String, EV>(vertex.getId() + "_" + targetHashCode % alpha,
							edge.getTarget(), edge.getValue()));
				} else {
					collector.collect(edge);
				}

			}
		}
	}

	private static final class SplitTargetCoGroup<EV> implements
			CoGroupFunction<Edge<String, EV>, Vertex<String, NullValue>, Edge<String, EV>> {

		int currentLevel;
		int alpha;

		public SplitTargetCoGroup(int currentLevel, int alpha) {
			this.currentLevel = currentLevel;
			this.alpha = alpha;
		}

		@Override
		public void coGroup(Iterable<Edge<String, EV>> iterableEdges,
							Iterable<Vertex<String, NullValue>> iterableVertices,
							Collector<Edge<String, EV>> collector) throws Exception {

			Iterator<Vertex<String, NullValue>> vertexIterator = iterableVertices.iterator();
			Iterator<Edge<String, EV>> edgeIterator = iterableEdges.iterator();

			Vertex<String, NullValue> vertex = null;

			if(vertexIterator.hasNext()) {
				vertex = vertexIterator.next();
			}

			while(edgeIterator.hasNext()) {
				Edge<String, EV> edge = edgeIterator.next();

				int sourceHashCode = edge.getSource().hashCode();

				// avoid having the same hash code from one level to the other
				for(int i=0; i < currentLevel; i++) {
					sourceHashCode = Sha.hash256(sourceHashCode+"").hashCode();
				}

				if(sourceHashCode < 0) {
					sourceHashCode *= -1;
				}

				if (vertex != null) {
					collector.collect(new Edge<String, EV>(edge.getSource(),
							vertex.getId() + "_" + sourceHashCode % alpha, edge.getValue()));
				} else {
					collector.collect(edge);
				}
			}
		}
	}

	private static final class SplitVerticesCoGroup<VV> implements CoGroupFunction<Vertex<String, VV>, Vertex<String, NullValue>, Vertex<String, VV>> {

		int alpha;

		public SplitVerticesCoGroup(int alpha) {
			this.alpha = alpha;
		}


		@Override
		public void coGroup(Iterable<Vertex<String, VV>> vertex,
							Iterable<Vertex<String, NullValue>> skewedVertex,
							Collector<Vertex<String, VV>> collector) throws Exception {

			Iterator<Vertex<String, VV>> vertexIterator = vertex.iterator();
			Iterator<Vertex<String, NullValue>> skewedVertexIterator = skewedVertex.iterator();

			Vertex<String, VV> next = null;

			if (vertexIterator.hasNext()) {
				if (!skewedVertexIterator.hasNext()) {
					next = vertexIterator.next();
					collector.collect(next);
				} else {
					next = vertexIterator.next();
					for (int i = 0; i < alpha; i++) {
						collector.collect(new Vertex<String, VV>(next.getId() + "_" + i, next.getValue()));
					}
				}
			}
		}
	}

	private static class ProjectDuplicateEdge<EV> implements FlatJoinFunction<Vertex<String, NullValue>, Edge<String, EV>, Edge<String, EV>> {

		@Override
		public void join(Vertex<String, NullValue> vertex, Edge<String, EV> edge,
						Collector<Edge<String,EV>> collector) throws Exception {
			collector.collect(edge);
			collector.collect(new Edge<String, EV>(edge.getTarget(), edge.getSource(), edge.getValue()));
		}
	}
}
