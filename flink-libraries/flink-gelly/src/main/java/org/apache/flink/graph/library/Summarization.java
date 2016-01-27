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

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;

/**
 * The summarization algorithm computes a condensed version of the input graph<br>
 * by grouping vertices and edges based on their values. By doing this, the<br>
 * algorithm helps to uncover insights about patterns and distributions in the<br>
 * graph.
 * <p>
 * In the resulting graph, each vertex represents a group of vertices that share the<br>
 * same vertex value. An edge, that connects a vertex with itself, represents all edges<br>
 * with the same edge value that connect vertices inside that group. An edge between<br>
 * vertices in the output graph represents all edges with the same edge value between<br>
 * members of those groups in the input graph.
 * <p>
 * Consider the following example:
 * <p>
 * Input graph:
 * <p>
 * Vertices (id, value):<br>
 * (0, "A")<br>
 * (1, "A")<br>
 * (2, "B")<br>
 * (3, "B")<br>
 * <p>
 * Edges (source, target, value):
 * (0,1, null)<br>
 * (1,0, null)<br>
 * (1,2, null)<br>
 * (2,1, null)<br>
 * (2,3, null)<br>
 * (3,2, null)<br>
 * <p>
 * Output graph:
 * <p>
 * Vertices (id, (value, count)):<br>
 * (0, ("A", 2)) // 0 and 1 <br>
 * (2, ("B", 2)) // 2 and 3 <br>
 * <p>
 * Edges (source, target, (value, count)):<br>
 * (0, 0, (null, 2)) // (0,1) and (1,0) <br>
 * (2, 2, (null, 2)) // (2,3) and (3,2) <br>
 * (0, 2, (null, 1)) // (1,2) <br>
 * (2, 0, (null, 1)) // (2,1) <br>
 *
 * Note that this implementation is non-deterministic in the way that it assigns<br>
 * identifiers to summarized vertices. However, it is guaranteed that the identifier<br>
 * is one of the represented vertex identifiers.
 *
 * @param <K> 	vertex identifier type
 * @param <VV> 	vertex value type
 * @param <EV> 	edge value type
 */
public class Summarization<K, VV, EV>
		implements GraphAlgorithm<K, VV, EV,
		Graph<K, Summarization.VertexValue<VV>, Summarization.EdgeValue<EV>>> {

	@Override
	public Graph<K, VertexValue<VV>, EdgeValue<EV>> run(Graph<K, VV, EV> input) throws Exception {
		// -------------------------
		// build summarized vertices
		// -------------------------

		// group vertices by value
		UnsortedGrouping<Vertex<K, VV>> vertexUnsortedGrouping = input.getVertices()
				.groupBy(1);
		// reduce vertex group and create vertex group items
		GroupReduceOperator<Vertex<K, VV>, VertexGroupItem<K, VV>> vertexGroupItems = vertexUnsortedGrouping
				.reduceGroup(new VertexGroupReducer<K, VV>());
		// create summarized vertices
		DataSet<Vertex<K, VertexValue<VV>>> summarizedVertices = vertexGroupItems
				.filter(new VertexGroupItemToSummarizedVertexFilter<K, VV>())
				.map(new VertexGroupItemToSummarizedVertexMapper<K, VV>());
		// create mapping between vertices and their representative
		DataSet<VertexWithRepresentative<K>> vertexToRepresentativeMap = vertexGroupItems
				.filter(new VertexGroupItemToRepresentativeFilter<K, VV>())
				.map(new VertexGroupItemToVertexWithRepresentativeMapper<K, VV>());

		// -------------------------
		// build summarized edges
		// -------------------------

		// join edges with vertex representatives and update source and target identifiers
		DataSet<Edge<K, EV>> edgesForGrouping = input.getEdges()
				.join(vertexToRepresentativeMap)
				.where(0) 	// source vertex id
				.equalTo(0) // vertex id
				.with(new SourceVertexJoinFunction<K, EV>())
				.join(vertexToRepresentativeMap)
				.where(1) 	// target vertex id
				.equalTo(0) // vertex id
				.with(new TargetVertexJoinFunction<K, EV>());
		// create summarized edges
		DataSet<Edge<K, EdgeValue<EV>>> summarizedEdges = edgesForGrouping
				.groupBy(0, 1, 2) // group by source id (0), target id (1) and edge value (2)
				.reduceGroup(new EdgeGroupReducer<K, EV>());

		return Graph.fromDataSet(summarizedVertices, summarizedEdges, input.getContext());
	}

	// --------------------------------------------------------------------------------------------
	//  Tuple Types
	// --------------------------------------------------------------------------------------------

	/**
	 * Value that is stored at a summarized vertex.
	 *
	 * f0: vertex group value
	 * f1: vertex group count
	 *
	 * @param <VV> vertex value type
	 */
	@SuppressWarnings("serial")
	public static final class VertexValue<VV> extends Tuple2<VV, Long> {

		public VV getVertexGroupValue() {
			return f0;
		}

		public void setVertexGroupValue(VV vertexGroupValue) {
			f0 = vertexGroupValue;
		}

		public Long getVertexGroupCount() {
			return f1;
		}

		public void setVertexGroupCount(Long vertexGroupCount) {
			f1 = vertexGroupCount;
		}
	}

	/**
	 * Value that is stored at a summarized edge.
	 *
	 * f0: edge group value
	 * f1: edge group count
	 *
	 * @param <EV> edge value type
	 */
	@SuppressWarnings("serial")
	public static final class EdgeValue<EV> extends Tuple2<EV, Long> {

		public EV getEdgeGroupValue() {
			return f0;
		}

		public void setEdgeGroupValue(EV edgeGroupValue) {
			f0 = edgeGroupValue;
		}

		public Long getEdgeGroupCount() {
			return f1;
		}

		public void setEdgeGroupCount(Long edgeGroupCount) {
			f1 = edgeGroupCount;
		}
	}

	/**
	 * Represents a single vertex in a vertex group.
	 *
	 * f0: vertex identifier
	 * f1: vertex group representative identifier
	 * f2: vertex group value
	 * f3: vertex group count
	 *
	 * @param <K> 	vertex identifier type
	 * @param <VGV> vertex group value type
	 */
	@SuppressWarnings("serial")
	public static final class VertexGroupItem<K, VGV> extends Tuple4<K, K, VGV, Long> {

		public VertexGroupItem() {
			setVertexGroupCount(0L);
		}

		public K getVertexId() {
			return f0;
		}

		public void setVertexId(K vertexId) {
			f0 = vertexId;
		}

		public K getGroupRepresentativeId() {
			return f1;
		}

		public void setGroupRepresentativeId(K groupRepresentativeId) {
			f1 = groupRepresentativeId;
		}

		public VGV getVertexGroupValue() {
			return f2;
		}

		public void setVertexGroupValue(VGV vertexGroupValue) {
			f2 = vertexGroupValue;
		}

		public Long getVertexGroupCount() {
			return f3;
		}

		public void setVertexGroupCount(Long vertexGroupCount) {
			f3 = vertexGroupCount;
		}

		/**
		 * Resets the fields to initial values. This is necessary if the tuples are reused and not all fields were modified.
		 */
		public void reset() {
			f0 = null;
			f1 = null;
			f2 = null;
			f3 = 0L;
		}
	}

	/**
	 * Represents a vertex identifier and its corresponding vertex group identifier.
	 *
	 * @param <K> vertex identifier type
	 */
	@SuppressWarnings("serial")
	public static final class VertexWithRepresentative<K> extends Tuple2<K, K> {

		public void setVertexId(K vertexId) {
			f0 = vertexId;
		}

		public K getGroupRepresentativeId() {
			return f1;
		}

		public void setGroupRepresentativeId(K groupRepresentativeId) {
			f1 = groupRepresentativeId;
		}
	}


	// --------------------------------------------------------------------------------------------
	//  Functions
	// --------------------------------------------------------------------------------------------

	/**
	 * Creates one {@link VertexGroupItem} for each group element containing the vertex identifier and the identifier
	 * of the group representative which is the first vertex in the reduce input iterable.
	 *
	 * Creates one {@link VertexGroupItem} representing the whole group that contains the vertex identifier of the
	 * group representative, the vertex group value and the total number of group elements.
	 *
	 * @param <K> 	vertex identifier type
	 * @param <VV> 	vertex value type
	 */
	@SuppressWarnings("serial")
	private static final class VertexGroupReducer<K, VV>
			implements GroupReduceFunction<Vertex<K, VV>, VertexGroupItem<K, VV>> {

		private final VertexGroupItem<K, VV> reuseVertexGroupItem;

		private VertexGroupReducer() {
			this.reuseVertexGroupItem = new VertexGroupItem<>();
		}

		@Override
		public void reduce(Iterable<Vertex<K, VV>> values, Collector<VertexGroupItem<K, VV>> out) throws Exception {
			K vertexGroupRepresentativeID = null;
			long vertexGroupCount = 0L;
			VV vertexGroupValue = null;
			boolean isFirstElement = true;

			for (Vertex<K, VV> vertex : values) {
				if (isFirstElement) {
					// take final group representative vertex id from first tuple
					vertexGroupRepresentativeID = vertex.getId();
					vertexGroupValue = vertex.getValue();
					isFirstElement = false;
				}
				// no need to set group value for those tuples
				reuseVertexGroupItem.setVertexId(vertex.getId());
				reuseVertexGroupItem.setGroupRepresentativeId(vertexGroupRepresentativeID);
				out.collect(reuseVertexGroupItem);
				vertexGroupCount++;
			}

			createGroupRepresentativeTuple(vertexGroupRepresentativeID, vertexGroupValue, vertexGroupCount);
			out.collect(reuseVertexGroupItem);
			reuseVertexGroupItem.reset();
		}

		/**
		 * Creates one tuple representing the whole group. This tuple is later used to create a summarized vertex for each
		 * group.
		 *
		 * @param vertexGroupRepresentativeId group representative vertex identifier
		 * @param vertexGroupValue  					group property value
		 * @param vertexGroupCount          	total group count
		 */
		private void createGroupRepresentativeTuple(K vertexGroupRepresentativeId,
																								VV vertexGroupValue,
																								Long vertexGroupCount) {
			reuseVertexGroupItem.setVertexId(vertexGroupRepresentativeId);
			reuseVertexGroupItem.setVertexGroupValue(vertexGroupValue);
			reuseVertexGroupItem.setVertexGroupCount(vertexGroupCount);
		}
	}

	/**
	 * Creates a summarized edge from a group of edges. Counts the number of elements in the group.
	 *
	 * @param <K> vertex identifier type
	 * @param <EV> edge group value type
	 */
	@SuppressWarnings("serial")
	private static final class EdgeGroupReducer<K, EV>
			implements GroupReduceFunction<Edge<K, EV>, Edge<K, EdgeValue<EV>>> {

		private final Edge<K, EdgeValue<EV>> reuseEdge;

		private final EdgeValue<EV> reuseEdgeValue;

		private EdgeGroupReducer() {
			reuseEdge = new Edge<>();
			reuseEdgeValue = new EdgeValue<>();
		}

		@Override
		public void reduce(Iterable<Edge<K, EV>> values, Collector<Edge<K, EdgeValue<EV>>> out) throws Exception {
			K sourceVertexId = null;
			K targetVertexId = null;
			EV edgeGroupValue = null;
			Long edgeGroupCount = 0L;
			boolean isFirstElement = true;

			for (Edge<K, EV> edge : values) {
				if (isFirstElement) {
					sourceVertexId = edge.getSource();
					targetVertexId = edge.getTarget();
					edgeGroupValue = edge.getValue();
					isFirstElement = false;
				}
				edgeGroupCount++;
			}
			reuseEdgeValue.setEdgeGroupValue(edgeGroupValue);
			reuseEdgeValue.setEdgeGroupCount(edgeGroupCount);
			reuseEdge.setSource(sourceVertexId);
			reuseEdge.setTarget(targetVertexId);
			reuseEdge.setValue(reuseEdgeValue);
			out.collect(reuseEdge);
		}
	}

	/**
	 * Filter tuples that are representing a vertex group. They are used to create new summarized vertices and have a
	 * group count greater than zero.
	 *
	 * @param <K> 	vertex identifier type
	 * @param <VV> 	vertex value type
	 */
	@SuppressWarnings("serial")
	@FunctionAnnotation.ForwardedFields("*->*")
	private static final class VertexGroupItemToSummarizedVertexFilter<K, VV>
			implements FilterFunction<VertexGroupItem<K, VV>> {

		@Override
		public boolean filter(VertexGroupItem<K, VV> vertexGroupItem) throws Exception {
			return !vertexGroupItem.getVertexGroupCount().equals(0L);
		}
	}

	/**
	 * Filter tuples that are representing a single vertex. They are used to update the source and target vertex
	 * identifiers at the edges.
	 *
	 * @param <K> 	vertex identifier type
	 * @param <VV> 	vertex value type
	 */
	@SuppressWarnings("serial")
	@FunctionAnnotation.ForwardedFields("*->*")
	private static final class VertexGroupItemToRepresentativeFilter<K, VV>
			implements FilterFunction<VertexGroupItem<K, VV>> {

		@Override
		public boolean filter(VertexGroupItem<K, VV> vertexGroupItem) throws Exception {
			return vertexGroupItem.getVertexGroupCount().equals(0L);
		}
	}

	/**
	 * Creates a new vertex representing a vertex group. The vertex stores the group value and the number of vertices in
	 * the group.
	 *
	 * @param <K> 	vertex identifier type
	 * @param <VV> 	vertex value type
	 */
	@SuppressWarnings("serial")
	private static final class VertexGroupItemToSummarizedVertexMapper<K, VV>
			implements MapFunction<VertexGroupItem<K, VV>, Vertex<K, VertexValue<VV>>> {

		private final VertexValue<VV> reuseSummarizedVertexValue;

		private VertexGroupItemToSummarizedVertexMapper() {
			reuseSummarizedVertexValue = new VertexValue<>();
		}

		@Override
		public Vertex<K, VertexValue<VV>> map(VertexGroupItem<K, VV> value) throws Exception {
			K vertexId = value.getVertexId();
			reuseSummarizedVertexValue.setVertexGroupValue(value.getVertexGroupValue());
			reuseSummarizedVertexValue.setVertexGroupCount(value.getVertexGroupCount());
			return new Vertex<>(vertexId, reuseSummarizedVertexValue);
		}
	}

	/**
	 * Creates a {@link VertexWithRepresentative} from a {@link VertexGroupItem}.
	 *
	 * @param <K> 	vertex identifier type
	 * @param <VV> 	vertex value type
	 */
	@SuppressWarnings("serial")
	@FunctionAnnotation.ForwardedFields("f0;f1")
	private static final class VertexGroupItemToVertexWithRepresentativeMapper<K, VV>
			implements MapFunction<VertexGroupItem<K, VV>, VertexWithRepresentative<K>> {

		private final VertexWithRepresentative<K> reuseVertexWithRepresentative;

		private VertexGroupItemToVertexWithRepresentativeMapper() {
			reuseVertexWithRepresentative = new VertexWithRepresentative<>();
		}

		@Override
		public VertexWithRepresentative<K> map(VertexGroupItem<K, VV> vertexGroupItem) throws Exception {
			reuseVertexWithRepresentative.setVertexId(vertexGroupItem.getVertexId());
			reuseVertexWithRepresentative.setGroupRepresentativeId(vertexGroupItem.getGroupRepresentativeId());
			return reuseVertexWithRepresentative;
		}
	}

	/**
	 * Replaces the source vertex id with the vertex group representative id and adds the edge group value.
	 *
	 * @param <K> 	vertex identifier type
	 * @param <EV> 	edge value type
	 */
	@SuppressWarnings("serial")
	@FunctionAnnotation.ForwardedFieldsFirst("f1") 			// edge target id
	@FunctionAnnotation.ForwardedFieldsSecond("f1->f0") // vertex group id -> edge source id
	private static final class SourceVertexJoinFunction<K, EV>
			implements JoinFunction<Edge<K, EV>, VertexWithRepresentative<K>, Edge<K, EV>> {

		private final Edge<K, EV> reuseEdge;

		private SourceVertexJoinFunction() {
			this.reuseEdge = new Edge<>();
		}

		@Override
		public Edge<K, EV> join(Edge<K, EV> edge, VertexWithRepresentative<K> vertex) throws Exception {
			reuseEdge.setSource(vertex.getGroupRepresentativeId());
			reuseEdge.setTarget(edge.getTarget());
			reuseEdge.setValue(edge.getValue());
			return reuseEdge;
		}
	}

	/**
	 * Replaces the target vertex id with the vertex group identifier.
	 *
	 * @param <K> 	vertex identifier type
	 * @param <EV> 	edge group value type
	 */
	@SuppressWarnings("serial")
	@FunctionAnnotation.ForwardedFieldsFirst("f0;f2") // source vertex id, edge group value
	@FunctionAnnotation.ForwardedFieldsSecond("f1") 	// vertex group id -> edge target id
	private static final class TargetVertexJoinFunction<K, EV>
			implements JoinFunction<Edge<K, EV>, VertexWithRepresentative<K>, Edge<K, EV>> {
		@Override
		public Edge<K, EV> join(Edge<K, EV> edge,
														VertexWithRepresentative<K> vertexRepresentative) throws Exception {
			edge.setTarget(vertexRepresentative.getGroupRepresentativeId());
			return edge;
		}
	}
}
