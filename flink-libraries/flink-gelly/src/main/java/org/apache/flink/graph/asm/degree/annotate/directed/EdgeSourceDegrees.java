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

package org.apache.flink.graph.asm.degree.annotate.directed;

import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.asm.degree.annotate.DegreeAnnotationFunctions.JoinEdgeWithVertexDegree;
import org.apache.flink.graph.asm.degree.annotate.directed.VertexDegrees.Degrees;
import org.apache.flink.graph.utils.proxy.GraphAlgorithmWrappingDataSet;

/**
 * Annotates edges of a directed graph with the degree, out-degree, and
 * in-degree of the source vertex.
 *
 * @param <K> ID type
 * @param <VV> vertex value type
 * @param <EV> edge value type
 */
public class EdgeSourceDegrees<K, VV, EV>
extends GraphAlgorithmWrappingDataSet<K, VV, EV, Edge<K, Tuple2<EV, Degrees>>> {

	@Override
	public DataSet<Edge<K, Tuple2<EV, Degrees>>> runInternal(Graph<K, VV, EV> input)
			throws Exception {
		// s, d(s)
		DataSet<Vertex<K, Degrees>> vertexDegrees = input
			.run(new VertexDegrees<K, VV, EV>()
				.setParallelism(parallelism));

		// s, t, d(s)
		return input.getEdges()
			.join(vertexDegrees, JoinHint.REPARTITION_HASH_SECOND)
			.where(0)
			.equalTo(0)
			.with(new JoinEdgeWithVertexDegree<K, EV, Degrees>())
				.setParallelism(parallelism)
				.name("Edge source degrees");
	}
}
