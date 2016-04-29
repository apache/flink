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
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Preconditions;

import static org.apache.flink.api.common.ExecutionConfig.PARALLELISM_DEFAULT;
import static org.apache.flink.api.common.ExecutionConfig.PARALLELISM_UNKNOWN;
import static org.apache.flink.graph.asm.translate.Translate.translateVertexValues;

/**
 * Translate {@link Vertex} values using the given {@link MapFunction}.
 *
 * @param <K> vertex ID type
 * @param <OLD> old vertex value type
 * @param <NEW> new vertex value type
 * @param <EV> edge value type
 */
public class TranslateVertexValues<K, OLD, NEW, EV>
implements GraphAlgorithm<K, OLD, EV, Graph<K, NEW, EV>> {

	// Required configuration
	private MapFunction<OLD, NEW> translator;

	// Optional configuration
	private int parallelism = PARALLELISM_UNKNOWN;

	/**
	 * Translate {@link Vertex} values using the given {@link MapFunction}.
	 *
	 * @param translator implements conversion from {@code OLD} to {@code NEW}
	 */
	public TranslateVertexValues(MapFunction<OLD, NEW> translator) {
		Preconditions.checkNotNull(translator);

		this.translator = translator;
	}

	/**
	 * Override the operator parallelism.
	 *
	 * @param parallelism operator parallelism
	 * @return this
	 */
	public TranslateVertexValues<K, OLD, NEW, EV> setParallelism(int parallelism) {
		Preconditions.checkArgument(parallelism > 0 || parallelism == PARALLELISM_DEFAULT || parallelism == PARALLELISM_UNKNOWN,
			"The parallelism must be greater than zero.");

		this.parallelism = parallelism;

		return this;
	}

	@Override
	public Graph<K, NEW, EV> run(Graph<K, OLD, EV> input) throws Exception {
		DataSet<Vertex<K, NEW>> translatedVertices = translateVertexValues(input.getVertices(), translator, parallelism);

		return Graph.fromDataSet(translatedVertices, input.getEdges(), input.getContext());
	}
}
