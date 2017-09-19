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

package org.apache.flink.graph.generator;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.base.ReduceOperatorBase.CombineHint;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.apache.flink.util.LongValueSequenceIterator;
import org.apache.flink.util.Preconditions;

import java.util.Collections;

/**
 * Utilities for graph generators.
 */
public class GraphGeneratorUtils {

	private GraphGeneratorUtils() {}

	/**
	 * Generates {@link Vertex Vertices} with sequential, numerical labels.
	 *
	 * @param env the Flink execution environment.
	 * @param parallelism operator parallelism
	 * @param vertexCount number of sequential vertex labels
	 * @return {@link DataSet} of sequentially labeled {@link Vertex vertices}
	 */
	public static DataSet<Vertex<LongValue, NullValue>> vertexSequence(ExecutionEnvironment env, int parallelism, long vertexCount) {
		Preconditions.checkArgument(vertexCount >= 0, "Vertex count must be non-negative");

		if (vertexCount == 0) {
			return env
				.fromCollection(Collections.emptyList(), TypeInformation.of(new TypeHint<Vertex<LongValue, NullValue>>(){}))
					.setParallelism(parallelism)
					.name("Empty vertex set");
		} else {
			LongValueSequenceIterator iterator = new LongValueSequenceIterator(0, vertexCount - 1);

			DataSource<LongValue> vertexLabels = env
				.fromParallelCollection(iterator, LongValue.class)
					.setParallelism(parallelism)
					.name("Vertex indices");

			return vertexLabels
				.map(new CreateVertex())
					.setParallelism(parallelism)
					.name("Vertex sequence");
		}
	}

	@ForwardedFields("*->f0")
	private static class CreateVertex
	implements MapFunction<LongValue, Vertex<LongValue, NullValue>> {
		private Vertex<LongValue, NullValue> vertex = new Vertex<>(null, NullValue.getInstance());

		@Override
		public Vertex<LongValue, NullValue> map(LongValue value)
				throws Exception {
			vertex.f0 = value;

			return vertex;
		}
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Generates {@link Vertex vertices} present in the given set of {@link Edge}s.
	 *
	 * @param edges source {@link DataSet} of {@link Edge}s
	 * @param parallelism operator parallelism
	 * @param <K> label type
	 * @param <EV> edge value type
	 * @return {@link DataSet} of discovered {@link Vertex vertices}
	 *
	 * @see Graph#fromDataSet(DataSet, DataSet, ExecutionEnvironment)
	 */
	public static <K, EV> DataSet<Vertex<K, NullValue>> vertexSet(DataSet<Edge<K, EV>> edges, int parallelism) {
		DataSet<Vertex<K, NullValue>> vertexSet = edges
			.flatMap(new EmitSrcAndTarget<>())
				.setParallelism(parallelism)
				.name("Emit source and target labels");

		return vertexSet
			.distinct()
			.setCombineHint(CombineHint.HASH)
				.setParallelism(parallelism)
				.name("Emit vertex labels");
	}

	/**
	 * @see Graph.EmitSrcAndTarget
	 */
	private static final class EmitSrcAndTarget<K, EV>
	implements FlatMapFunction<Edge<K, EV>, Vertex<K, NullValue>> {
		private Vertex<K, NullValue> output = new Vertex<>(null, new NullValue());

		@Override
		public void flatMap(Edge<K, EV> value, Collector<Vertex<K, NullValue>> out) throws Exception {
			output.f0 = value.f0;
			out.collect(output);
			output.f0 = value.f1;
			out.collect(output);
		}
	}
}
