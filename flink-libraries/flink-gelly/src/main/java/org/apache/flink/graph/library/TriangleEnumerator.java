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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * This library method enumerates unique triangles present in the input graph.
 * A triangle consists of three edges that connect three vertices with each other.
 * Edge directions are ignored here.
 * The method returns a DataSet of Tuple3, where the fields of each Tuple3 contain the Vertex IDs of a triangle.
 *
 * <p>The basic algorithm groups all edges that share a common vertex and builds triads,
 * i.e., triples of vertices that are connected by two edges. Then all triads are filtered
 * for which no third edge exists that closes the triangle.
 *
 * <p>For a group of <i>n</i> edges that share a common vertex, the number of built triads is quadratic <i>((n*(n-1))/2)</i>.
 * Therefore, an optimization of the algorithm is to group edges on the vertex with the smaller output degree to
 * reduce the number of triads.
 *
 * <p>This implementation extends the basic algorithm by computing output degrees of edge vertices and
 * grouping on edges on the vertex with the smaller degree.
 */
public class TriangleEnumerator<K extends Comparable<K>, VV, EV> implements
	GraphAlgorithm<K, VV, EV, DataSet<Tuple3<K, K, K>>> {

	@Override
	public DataSet<Tuple3<K, K, K>> run(Graph<K, VV, EV> input) throws Exception {

		DataSet<Edge<K, EV>> edges = input.getEdges();

		// annotate edges with degrees
		DataSet<EdgeWithDegrees<K>> edgesWithDegrees = edges.flatMap(new EdgeDuplicator<>())
				.groupBy(0).sortGroup(1, Order.ASCENDING).reduceGroup(new DegreeCounter<>())
				.groupBy(EdgeWithDegrees.V1, EdgeWithDegrees.V2).reduce(new DegreeJoiner<>());

		// project edges by degrees
		DataSet<Edge<K, NullValue>> edgesByDegree = edgesWithDegrees.map(new EdgeByDegreeProjector<>());
		// project edges by vertex id
		DataSet<Edge<K, NullValue>> edgesById = edgesByDegree.map(new EdgeByIdProjector<>());

		DataSet<Tuple3<K, K, K>> triangles = edgesByDegree
				// build triads
				.groupBy(EdgeWithDegrees.V1).sortGroup(EdgeWithDegrees.V2, Order.ASCENDING)
				.reduceGroup(new TriadBuilder<>())
				// filter triads
				.join(edgesById, JoinHint.REPARTITION_HASH_SECOND).where(Triad.V2, Triad.V3).equalTo(0, 1).with(new TriadFilter<>());

		return triangles;
	}

	/**
	 * Emits for an edge the original edge and its switched version.
	 */
	@SuppressWarnings("serial")
	private static final class EdgeDuplicator<K, EV> implements FlatMapFunction<Edge<K, EV>, Edge<K, EV>> {

		@Override
		public void flatMap(Edge<K, EV> edge, Collector<Edge<K, EV>> out) throws Exception {
			out.collect(edge);
			Edge<K, EV> reversed = edge.reverse();
			out.collect(reversed);
		}
	}

	/**
	 * Counts the number of edges that share a common vertex.
	 * Emits one edge for each input edge with a degree annotation for the shared vertex.
	 * For each emitted edge, the first vertex is the vertex with the smaller id.
	 */
	@SuppressWarnings("serial")
	private static final class DegreeCounter<K extends Comparable<K>, EV>
			implements GroupReduceFunction<Edge<K, EV>, EdgeWithDegrees<K>> {

		final ArrayList<K> otherVertices = new ArrayList<>();
		final EdgeWithDegrees<K> outputEdge = new EdgeWithDegrees<>();

		@Override
		public void reduce(Iterable<Edge<K, EV>> edgesIter, Collector<EdgeWithDegrees<K>> out) {

			Iterator<Edge<K, EV>> edges = edgesIter.iterator();
			otherVertices.clear();

			// get first edge
			Edge<K, EV> edge = edges.next();
			K groupVertex = edge.getSource();
			this.otherVertices.add(edge.getTarget());

			// get all other edges (assumes edges are sorted by second vertex)
			while (edges.hasNext()) {
				edge = edges.next();
				K otherVertex = edge.getTarget();
				// collect unique vertices
				if (!otherVertices.contains(otherVertex) && otherVertex != groupVertex) {
					this.otherVertices.add(otherVertex);
				}
			}
			int degree = this.otherVertices.size();

			// emit edges
			for (K otherVertex : this.otherVertices) {
				if (groupVertex.compareTo(otherVertex) < 0) {
					outputEdge.setFirstVertex(groupVertex);
					outputEdge.setFirstDegree(degree);
					outputEdge.setSecondVertex(otherVertex);
					outputEdge.setSecondDegree(0);
				} else {
					outputEdge.setFirstVertex(otherVertex);
					outputEdge.setFirstDegree(0);
					outputEdge.setSecondVertex(groupVertex);
					outputEdge.setSecondDegree(degree);
				}
				out.collect(outputEdge);
			}
		}
	}

	/**
	 * Builds an edge with degree annotation from two edges that have the same vertices and only one
	 * degree annotation.
	 */
	@SuppressWarnings("serial")
	@FunctionAnnotation.ForwardedFields("0;1")
	private static final class DegreeJoiner<K> implements ReduceFunction<EdgeWithDegrees<K>> {
		private final EdgeWithDegrees<K> outEdge = new EdgeWithDegrees<>();

		@Override
		public EdgeWithDegrees<K> reduce(EdgeWithDegrees<K> edge1, EdgeWithDegrees<K> edge2) throws Exception {

			// copy first edge
			outEdge.copyFrom(edge1);

			// set missing degree
			if (edge1.getFirstDegree() == 0 && edge1.getSecondDegree() != 0) {
				outEdge.setFirstDegree(edge2.getFirstDegree());
			} else if (edge1.getFirstDegree() != 0 && edge1.getSecondDegree() == 0) {
				outEdge.setSecondDegree(edge2.getSecondDegree());
			}

			return outEdge;
		}
	}

	/**
	 * Projects an edge (pair of vertices) such that the first vertex is the vertex with the smaller degree.
	 */
	@SuppressWarnings("serial")
	private static final class EdgeByDegreeProjector<K> implements MapFunction<EdgeWithDegrees<K>, Edge<K, NullValue>> {

		private Edge<K, NullValue> outEdge = new Edge<>();

		@Override
		public Edge<K, NullValue> map(EdgeWithDegrees<K> inEdge) throws Exception {

			// copy vertices to simple edge
			outEdge.setSource(inEdge.getFirstVertex());
			outEdge.setTarget(inEdge.getSecondVertex());
			outEdge.setValue(NullValue.getInstance());

			// flip vertices if first degree is larger than second degree.
			if (inEdge.getFirstDegree() > inEdge.getSecondDegree()) {
				outEdge = outEdge.reverse();
			}

			// return edge
			return outEdge;
		}
	}

	/**
	 * Projects an edge (pair of vertices) such that the id of the first is smaller than the id of the second.
	 */
	@SuppressWarnings("serial")
	private static final class EdgeByIdProjector<K extends Comparable<K>>
			implements MapFunction<Edge<K, NullValue>, Edge<K, NullValue>> {

		@Override
		public Edge<K, NullValue> map(Edge<K, NullValue> inEdge) throws Exception {

			// flip vertices if necessary
			if (inEdge.getSource().compareTo(inEdge.getTarget()) > 0) {
				inEdge = inEdge.reverse();
			}

			return inEdge;
		}
	}

	/**
	 * Builds triads (triples of vertices) from pairs of edges that share a vertex.
	 * The first vertex of a triad is the shared vertex, the second and third vertex are ordered by vertexId.
	 * Assumes that input edges share the first vertex and are in ascending order of the second vertex.
	 */
	@SuppressWarnings("serial")
	@FunctionAnnotation.ForwardedFields("0")
	private static final class TriadBuilder<K extends Comparable<K>>
			implements GroupReduceFunction<Edge<K, NullValue>, Triad<K>> {

		private final List<K> vertices = new ArrayList<>();
		private final Triad<K> outTriad = new Triad<>();

		@Override
		public void reduce(Iterable<Edge<K, NullValue>> edgesIter, Collector<Triad<K>> out) throws Exception {
			final Iterator<Edge<K, NullValue>> edges = edgesIter.iterator();

			// clear vertex list
			vertices.clear();

			// read first edge
			Edge<K, NullValue> firstEdge = edges.next();
			outTriad.setFirstVertex(firstEdge.getSource());
			vertices.add(firstEdge.getTarget());

			// build and emit triads
			while (edges.hasNext()) {
				K higherVertexId = edges.next().getTarget();

				// combine vertex with all previously read vertices
				for (K lowerVertexId : vertices) {
					outTriad.setSecondVertex(lowerVertexId);
					outTriad.setThirdVertex(higherVertexId);
					out.collect(outTriad);
				}
				vertices.add(higherVertexId);
			}
		}
	}

	/**
	 * Filters triads (three vertices connected by two edges) without a closing third edge.
	 */
	@SuppressWarnings("serial")
	private static final class TriadFilter<K> implements JoinFunction<Triad<K>, Edge<K, NullValue>, Tuple3<K, K, K>> {

		@Override
		public Tuple3<K, K, K> join(Triad<K> triad, Edge<K, NullValue> edge) throws Exception {
			return new Tuple3<>(triad.getFirstVertex(), triad.getSecondVertex(), triad.getThirdVertex());
		}
	}

	/**
	 * POJO storing two vertex IDs with degree.
	 *
	 * @param <K> vertex ID type
	 */
	@SuppressWarnings("serial")
	public static final class EdgeWithDegrees<K> extends Tuple4<K, K, Integer, Integer> {

		public static final int V1 = 0;
		public static final int V2 = 1;
		public static final int D1 = 2;
		public static final int D2 = 3;

		public K getFirstVertex() {
			return this.getField(V1);
		}

		public K getSecondVertex() {
			return this.getField(V2);
		}

		public Integer getFirstDegree() {
			return this.getField(D1);
		}

		public Integer getSecondDegree() {
			return this.getField(D2);
		}

		public void setFirstVertex(final K vertex1) {
			this.setField(vertex1, V1);
		}

		public void setSecondVertex(final K vertex2) {
			this.setField(vertex2, V2);
		}

		public void setFirstDegree(final Integer degree1) {
			this.setField(degree1, D1);
		}

		public void setSecondDegree(final Integer degree2) {
			this.setField(degree2, D2);
		}

		public void copyFrom(final EdgeWithDegrees<K> edge) {
			this.setFirstVertex(edge.getFirstVertex());
			this.setSecondVertex(edge.getSecondVertex());
			this.setFirstDegree(edge.getFirstDegree());
			this.setSecondDegree(edge.getSecondDegree());
		}
	}

	/**
	 * Tuple storing three vertex IDs.
	 *
	 * @param <K> vertex ID type
	 */
	public static final class Triad<K> extends Tuple3<K, K, K> {
		private static final long serialVersionUID = 1L;

		public static final int V1 = 0;
		public static final int V2 = 1;
		public static final int V3 = 2;

		public K getFirstVertex() {
			return this.getField(V1);
		}

		public K getSecondVertex() {
			return this.getField(V2);
		}

		public K getThirdVertex() {
			return this.getField(V3);
		}

		public void setFirstVertex(final K vertex1) {
			this.setField(vertex1, V1);
		}

		public void setSecondVertex(final K vertex2) {
			this.setField(vertex2, V2);
		}

		public void setThirdVertex(final K vertex3) {
			this.setField(vertex3, V3);
		}
	}
}
