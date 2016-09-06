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

package org.apache.flink.graph.asm.degree.annotate.undirected;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.base.ReduceOperatorBase.CombineHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.utils.proxy.GraphAlgorithmDelegatingDataSet;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.utils.proxy.OptionalBoolean;
import org.apache.flink.graph.asm.degree.annotate.DegreeAnnotationFunctions.DegreeCount;
import org.apache.flink.graph.asm.degree.annotate.DegreeAnnotationFunctions.JoinVertexWithVertexDegree;
import org.apache.flink.graph.asm.degree.annotate.DegreeAnnotationFunctions.MapEdgeToSourceId;
import org.apache.flink.graph.asm.degree.annotate.DegreeAnnotationFunctions.MapEdgeToTargetId;
import org.apache.flink.types.LongValue;
import org.apache.flink.util.Preconditions;

import static org.apache.flink.api.common.ExecutionConfig.PARALLELISM_DEFAULT;

/**
 * Annotates vertices of an undirected graph with the degree.
 *
 * @param <K> ID type
 * @param <VV> vertex value type
 * @param <EV> edge value type
 */
public class VertexDegree<K, VV, EV>
extends GraphAlgorithmDelegatingDataSet<K, VV, EV, Vertex<K, LongValue>> {

	// Optional configuration
	private OptionalBoolean includeZeroDegreeVertices = new OptionalBoolean(false, true);

	private OptionalBoolean reduceOnTargetId = new OptionalBoolean(false, false);

	private int parallelism = PARALLELISM_DEFAULT;

	/**
	 * By default only the edge set is processed for the computation of degree.
	 * When this flag is set an additional join is performed against the vertex
	 * set in order to output vertices with a degree of zero.
	 *
	 * @param includeZeroDegreeVertices whether to output vertices with a
	 *                                  degree of zero
	 * @return this
	 */
	public VertexDegree<K, VV, EV> setIncludeZeroDegreeVertices(boolean includeZeroDegreeVertices) {
		this.includeZeroDegreeVertices.set(includeZeroDegreeVertices);

		return this;
	}

	/**
	 * The degree can be counted from either the edge source or target IDs.
	 * By default the source IDs are counted. Reducing on target IDs may
	 * optimize the algorithm if the input edge list is sorted by target ID.
	 *
	 * @param reduceOnTargetId set to {@code true} if the input edge list
	 *                         is sorted by target ID
	 * @return this
	 */
	public VertexDegree<K, VV, EV> setReduceOnTargetId(boolean reduceOnTargetId) {
		this.reduceOnTargetId.set(reduceOnTargetId);

		return this;
	}

	/**
	 * Override the operator parallelism.
	 *
	 * @param parallelism operator parallelism
	 * @return this
	 */
	public VertexDegree<K, VV, EV> setParallelism(int parallelism) {
		Preconditions.checkArgument(parallelism > 0 || parallelism == PARALLELISM_DEFAULT,
			"The parallelism must be greater than zero.");

		this.parallelism = parallelism;

		return this;
	}

	@Override
	protected String getAlgorithmName() {
		return VertexDegree.class.getName();
	}

	@Override
	protected boolean mergeConfiguration(GraphAlgorithmDelegatingDataSet other) {
		Preconditions.checkNotNull(other);

		if (! VertexDegree.class.isAssignableFrom(other.getClass())) {
			return false;
		}

		VertexDegree rhs = (VertexDegree) other;

		// verify that configurations can be merged

		if (includeZeroDegreeVertices.conflictsWith(rhs.includeZeroDegreeVertices)) {
			return false;
		}

		// merge configurations

		includeZeroDegreeVertices.mergeWith(rhs.includeZeroDegreeVertices);
		reduceOnTargetId.mergeWith(rhs.reduceOnTargetId);
		parallelism = Math.min(parallelism, rhs.parallelism);

		return true;
	}

	@Override
	public DataSet<Vertex<K, LongValue>> runInternal(Graph<K, VV, EV> input)
			throws Exception {
		MapFunction<Edge<K, EV>, Vertex<K, LongValue>> mapEdgeToId = reduceOnTargetId.get() ?
			new MapEdgeToTargetId<K, EV>() : new MapEdgeToSourceId<K, EV>();

		// v
		DataSet<Vertex<K, LongValue>> vertexIds = input
			.getEdges()
			.map(mapEdgeToId)
				.setParallelism(parallelism)
				.name("Map edge to vertex ID");

		// v, deg(v)
		DataSet<Vertex<K, LongValue>> degree = vertexIds
			.groupBy(0)
			.reduce(new DegreeCount<K>())
				.setCombineHint(CombineHint.HASH)
				.setParallelism(parallelism)
				.name("Degree count");

		if (includeZeroDegreeVertices.get()) {
			degree = input
				.getVertices()
				.leftOuterJoin(degree)
				.where(0)
				.equalTo(0)
				.with(new JoinVertexWithVertexDegree<K, VV>())
					.setParallelism(parallelism)
					.name("Join zero degree vertices");
		}

		return degree;
	}
}
