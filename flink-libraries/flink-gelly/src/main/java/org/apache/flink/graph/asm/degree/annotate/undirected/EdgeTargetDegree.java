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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.asm.degree.annotate.DegreeAnnotationFunctions.JoinEdgeWithVertexDegree;
import org.apache.flink.types.LongValue;

/**
 * Annotates edges of an undirected graph with degree of the target ID.
 *
 * @param <K> ID type
 * @param <VV> vertex value type
 * @param <EV> edge value type
 */
public class EdgeTargetDegree<K, VV, EV>
implements GraphAlgorithm<K, VV, EV, DataSet<Edge<K, Tuple2<EV, LongValue>>>> {

	// Optional configuration
	private boolean reduceOnSourceId = false;

	private int parallelism = ExecutionConfig.PARALLELISM_UNKNOWN;

	/**
	 * The degree can be counted from either the edge source or target IDs.
	 * By default the target IDs are counted. Reducing on source IDs may
	 * optimize the algorithm if the input edge list is sorted by source ID.
	 *
	 * @param reduceOnSourceId set to {@code true} if the input edge list
	 *                         is sorted by target ID
	 * @return this
	 */
	public EdgeTargetDegree<K, VV, EV> setReduceOnSourceId(boolean reduceOnSourceId) {
		this.reduceOnSourceId = reduceOnSourceId;

		return this;
	}

	/**
	 * Override the operator parallelism.
	 *
	 * @param parallelism operator parallelism
	 * @return this
	 */
	public EdgeTargetDegree<K, VV, EV> setParallelism(int parallelism) {
		this.parallelism = parallelism;

		return this;
	}

	@Override
	public DataSet<Edge<K, Tuple2<EV, LongValue>>> run(Graph<K, VV, EV> input)
			throws Exception {
		DataSet<Vertex<K, LongValue>> vertexDegrees;

		if (reduceOnSourceId) {
			// s, d(s)
			vertexDegrees = input
				.run(new VertexDegree<K, VV, EV>()
					.setParallelism(parallelism));
		} else {
			// t, d(t)
			vertexDegrees = input
				.run(new VertexDegree<K, VV, EV>()
					.setReduceOnTargetId(true)
					.setParallelism(parallelism));
		}

		// s, t, d(t)
		return input.getEdges()
			.join(vertexDegrees, JoinHint.REPARTITION_HASH_SECOND)
			.where(1)
			.equalTo(0)
			.with(new JoinEdgeWithVertexDegree<K, EV>())
				.setParallelism(parallelism)
				.name("Edge target degree");
	}
}
