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

package org.apache.flink.graph.asm.translate;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.utils.proxy.GraphAlgorithmWrappingBase;
import org.apache.flink.graph.utils.proxy.GraphAlgorithmWrappingGraph;
import org.apache.flink.util.Preconditions;

import static org.apache.flink.graph.asm.translate.Translate.translateEdgeIds;
import static org.apache.flink.graph.asm.translate.Translate.translateVertexIds;

/**
 * Translate {@link Vertex} and {@link Edge} IDs of a {@link Graph} using the given {@link TranslateFunction}.
 *
 * @param <OLD> old graph ID type
 * @param <NEW> new graph ID type
 * @param <VV> vertex value type
 * @param <EV> edge value type
 */
public class TranslateGraphIds<OLD, NEW, VV, EV>
extends GraphAlgorithmWrappingGraph<OLD, VV, EV, NEW, VV, EV> {

	// Required configuration
	private TranslateFunction<OLD, NEW> translator;

	/**
	 * Translate {@link Vertex} and {@link Edge} IDs of a {@link Graph} using the given {@link TranslateFunction}.
	 *
	 * @param translator implements conversion from {@code OLD} to {@code NEW}
	 */
	public TranslateGraphIds(TranslateFunction<OLD, NEW> translator) {
		Preconditions.checkNotNull(translator);

		this.translator = translator;
	}

	@Override
	protected boolean canMergeConfigurationWith(GraphAlgorithmWrappingBase other) {
		if (!super.canMergeConfigurationWith(other)) {
			return false;
		}

		TranslateGraphIds rhs = (TranslateGraphIds) other;

		return translator == rhs.translator;
	}

	@Override
	public Graph<NEW, VV, EV> runInternal(Graph<OLD, VV, EV> input)
			throws Exception {
		// Vertices
		DataSet<Vertex<NEW, VV>> translatedVertices = translateVertexIds(input.getVertices(), translator, parallelism);

		// Edges
		DataSet<Edge<NEW, EV>> translatedEdges = translateEdgeIds(input.getEdges(), translator, parallelism);

		// Graph
		return Graph.fromDataSet(translatedVertices, translatedEdges, input.getContext());
	}
}
