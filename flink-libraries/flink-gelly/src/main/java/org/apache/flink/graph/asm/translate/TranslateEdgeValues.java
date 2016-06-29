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
import org.apache.flink.graph.utils.proxy.GraphAlgorithmDelegatingGraph;
import org.apache.flink.util.Preconditions;

import static org.apache.flink.api.common.ExecutionConfig.PARALLELISM_DEFAULT;
import static org.apache.flink.graph.asm.translate.Translate.translateEdgeValues;

/**
 * Translate {@link Edge} values using the given {@link TranslateFunction}.
 *
 * @param <K> vertex ID type
 * @param <VV> vertex value type
 * @param <OLD> old edge value type
 * @param <NEW> new edge value type
 */
public class TranslateEdgeValues<K, VV, OLD, NEW>
extends GraphAlgorithmDelegatingGraph<K, VV, OLD, K, VV, NEW> {

	// Required configuration
	private TranslateFunction<OLD,NEW> translator;

	// Optional configuration
	private int parallelism = PARALLELISM_DEFAULT;

	/**
	 * Translate {@link Edge} values using the given {@link TranslateFunction}.
	 *
	 * @param translator implements conversion from {@code OLD} to {@code NEW}
	 */
	public TranslateEdgeValues(TranslateFunction<OLD, NEW> translator) {
		Preconditions.checkNotNull(translator);

		this.translator = translator;
	}

	/**
	 * Override the operator parallelism.
	 *
	 * @param parallelism operator parallelism
	 * @return this
	 */
	public TranslateEdgeValues<K, VV, OLD, NEW> setParallelism(int parallelism) {
		Preconditions.checkArgument(parallelism > 0 || parallelism == PARALLELISM_DEFAULT,
			"The parallelism must be greater than zero.");

		this.parallelism = parallelism;

		return this;
	}

	@Override
	protected String getAlgorithmName() {
		return TranslateEdgeValues.class.getName();
	}

	@Override
	protected boolean mergeConfiguration(GraphAlgorithmDelegatingGraph other) {
		Preconditions.checkNotNull(other);

		if (! TranslateEdgeValues.class.isAssignableFrom(other.getClass())) {
			return false;
		}

		TranslateEdgeValues rhs = (TranslateEdgeValues) other;

		// verify that configurations can be merged

		if (translator != rhs.translator) {
			return false;
		}

		// merge configurations

		parallelism = Math.min(parallelism, rhs.parallelism);

		return true;
	}

	@Override
	public Graph<K, VV, NEW> runInternal(Graph<K, VV, OLD> input)
			throws Exception {
		DataSet<Edge<K, NEW>> translatedEdges = translateEdgeValues(input.getEdges(), translator, parallelism);

		return Graph.fromDataSet(input.getVertices(), translatedEdges, input.getContext());
	}
}
