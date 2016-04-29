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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Preconditions;

import static org.apache.flink.api.common.ExecutionConfig.PARALLELISM_DEFAULT;
import static org.apache.flink.api.common.ExecutionConfig.PARALLELISM_UNKNOWN;
import static org.apache.flink.graph.asm.translate.Translate.translateEdgeIds;
import static org.apache.flink.graph.asm.translate.Translate.translateVertexIds;

/**
 * Translate {@link Vertex} and {@link Edge} IDs of a {@link Graph} using the given {@link MapFunction}
 *
 * @param <OLD> old graph ID type
 * @param <NEW> new graph ID type
 * @param <VV> vertex value type
 * @param <EV> edge value type
 */
public class TranslateGraphIds<OLD, NEW, VV, EV>
implements GraphAlgorithm<OLD, VV, EV, Graph<NEW, VV, EV>> {

	// Required configuration
	private MapFunction<OLD,NEW> translator;

	// Optional configuration
	private int parallelism = PARALLELISM_UNKNOWN;

	/**
	 * Translate {@link Vertex} and {@link Edge} IDs of a {@link Graph} using the given {@link MapFunction}
	 *
	 * @param translator implements conversion from {@code OLD} to {@code NEW}
	 */
	public TranslateGraphIds(MapFunction<OLD, NEW> translator) {
		Preconditions.checkNotNull(translator);

		this.translator = translator;
	}

	/**
	 * Override the operator parallelism.
	 *
	 * @param parallelism operator parallelism
	 * @return this
	 */
	public TranslateGraphIds<OLD, NEW, VV, EV> setParallelism(int parallelism) {
		Preconditions.checkArgument(parallelism > 0 || parallelism == PARALLELISM_DEFAULT || parallelism == PARALLELISM_UNKNOWN,
			"The parallelism must be greater than zero.");

		this.parallelism = parallelism;

		return this;
	}

	@Override
	public Graph<NEW, VV, EV> run(Graph<OLD, VV, EV> input) throws Exception {
		// Vertices
		DataSet<Vertex<NEW, VV>> translatedVertices = translateVertexIds(input.getVertices(), translator, parallelism);

		// Edges
		DataSet<Edge<NEW, EV>> translatedEdges = translateEdgeIds(input.getEdges(), translator, parallelism);

		// Graph
		return Graph.fromDataSet(translatedVertices, translatedEdges, input.getContext());
	}
}
