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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.CachingGraphAlgorithm;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.asm.degree.annotate.DegreeAnnotationFunctions.JoinVertexDegreeWithVertexDegree;
import org.apache.flink.types.LongValue;

/**
 * Annotates vertices of a directed graph with both the out-degree and
 * in-degree count.
 *
 * @param <K> graph label type
 * @param <VV> vertex value type
 * @param <EV> edge value type
 */
public class VertexDegreePair<K, VV, EV>
extends CachingGraphAlgorithm<K, VV, EV, DataSet<Vertex<K,Tuple2<LongValue,LongValue>>>> {

	// Optional configuration
	private int parallelism = ExecutionConfig.PARALLELISM_UNKNOWN;

	/**
	 * Override the operator parallelism.
	 *
	 * @param parallelism operator parallelism
	 * @return this
	 */
	public VertexDegreePair<K,VV,EV> setParallelism(int parallelism) {
		this.parallelism = parallelism;

		return this;
	}

	@Override
	protected String getAlgorithmName() {
		return VertexDegreePair.class.getCanonicalName();
	}

	@Override
	public DataSet<Vertex<K,Tuple2<LongValue,LongValue>>> runInternal(Graph<K,VV,EV> input)
			throws Exception {
		// s, deg(s)
		DataSet<Vertex<K,LongValue>> outDegree = input
			.run(new VertexOutDegree<K,VV,EV>());

		// t, deg(t)
		DataSet<Vertex<K,LongValue>> inDegree = input
			.run(new VertexInDegree<K,VV,EV>());

		return outDegree
			.fullOuterJoin(inDegree)
			.where(0)
			.equalTo(0)
			.with(new JoinVertexDegreeWithVertexDegree<K>())
				.setParallelism(parallelism)
				.name("Join out- and in-degree");
	}
}
