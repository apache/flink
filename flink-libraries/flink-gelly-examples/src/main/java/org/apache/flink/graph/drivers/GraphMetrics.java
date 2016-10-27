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

package org.apache.flink.graph.drivers;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.text.StrBuilder;
import org.apache.commons.lang3.text.WordUtils;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvOutputFormat;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.client.program.ProgramParametrizationException;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAnalytic;
import org.apache.flink.graph.GraphCsvReader;
import org.apache.flink.graph.asm.translate.TranslateGraphIds;
import org.apache.flink.graph.asm.translate.translators.LongValueToUnsignedIntValue;
import org.apache.flink.graph.generator.RMatGraph;
import org.apache.flink.graph.generator.random.JDKRandomGeneratorFactory;
import org.apache.flink.graph.generator.random.RandomGenerableFactory;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;
import org.apache.flink.types.StringValue;

import java.text.NumberFormat;

/**
 * Computes vertex and edge metrics on a directed or undirected graph.
 *
 * @see org.apache.flink.graph.library.metric.directed.EdgeMetrics
 * @see org.apache.flink.graph.library.metric.directed.VertexMetrics
 * @see org.apache.flink.graph.library.metric.undirected.EdgeMetrics
 * @see org.apache.flink.graph.library.metric.undirected.VertexMetrics
 */
public class GraphMetrics {

	private static final int DEFAULT_SCALE = 10;

	private static final int DEFAULT_EDGE_FACTOR = 16;

	private static final boolean DEFAULT_CLIP_AND_FLIP = true;

	private static String getUsage(String message) {
		return new StrBuilder()
			.appendNewLine()
			.appendln(WordUtils.wrap("Computes vertex and edge metrics on a directed or undirected graph.", 80))
			.appendNewLine()
			.appendln("usage: GraphMetrics --directed <true | false> --input <csv | rmat [options]>")
			.appendNewLine()
			.appendln("options:")
			.appendln("  --input csv --type <integer | string> [--simplify <true | false>] --input_filename FILENAME [--input_line_delimiter LINE_DELIMITER] [--input_field_delimiter FIELD_DELIMITER]")
			.appendln("  --input rmat [--scale SCALE] [--edge_factor EDGE_FACTOR]")
			.appendNewLine()
			.appendln("Usage error: " + message)
			.toString();
	}

	public static void main(String[] args) throws Exception {
		// Set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().enableObjectReuse();

		ParameterTool parameters = ParameterTool.fromArgs(args);
		env.getConfig().setGlobalJobParameters(parameters);

		if (! parameters.has("directed")) {
			throw new ProgramParametrizationException(getUsage("must declare execution mode as '--directed true' or '--directed false'"));
		}
		boolean directedAlgorithm = parameters.getBoolean("directed");

		GraphAnalytic vm;
		GraphAnalytic em;

		switch (parameters.get("input", "")) {
			case "csv": {
				String lineDelimiter = StringEscapeUtils.unescapeJava(
					parameters.get("input_line_delimiter", CsvOutputFormat.DEFAULT_LINE_DELIMITER));

				String fieldDelimiter = StringEscapeUtils.unescapeJava(
					parameters.get("input_field_delimiter", CsvOutputFormat.DEFAULT_FIELD_DELIMITER));

				GraphCsvReader reader = Graph
					.fromCsvReader(parameters.get("input_filename"), env)
						.ignoreCommentsEdges("#")
						.lineDelimiterEdges(lineDelimiter)
						.fieldDelimiterEdges(fieldDelimiter);

				switch (parameters.get("type", "")) {
					case "integer": {
						Graph<LongValue, NullValue, NullValue> graph = reader
							.keyType(LongValue.class);

						if (directedAlgorithm) {
							if (parameters.getBoolean("simplify", false)) {
								graph = graph
									.run(new org.apache.flink.graph.asm.simple.directed.Simplify<LongValue, NullValue, NullValue>());
							}

							vm = graph
								.run(new org.apache.flink.graph.library.metric.directed.VertexMetrics<LongValue, NullValue, NullValue>());
							em = graph
								.run(new org.apache.flink.graph.library.metric.directed.EdgeMetrics<LongValue, NullValue, NullValue>());
						} else {
							if (parameters.getBoolean("simplify", false)) {
								graph = graph
									.run(new org.apache.flink.graph.asm.simple.undirected.Simplify<LongValue, NullValue, NullValue>(false));
							}

							vm = graph
								.run(new org.apache.flink.graph.library.metric.undirected.VertexMetrics<LongValue, NullValue, NullValue>());
							em = graph
								.run(new org.apache.flink.graph.library.metric.undirected.EdgeMetrics<LongValue, NullValue, NullValue>());
						}
					} break;

					case "string": {
						Graph<StringValue, NullValue, NullValue> graph = reader
							.keyType(StringValue.class);

						if (directedAlgorithm) {
							if (parameters.getBoolean("simplify", false)) {
								graph = graph
									.run(new org.apache.flink.graph.asm.simple.directed.Simplify<StringValue, NullValue, NullValue>());
							}

							vm = graph
								.run(new org.apache.flink.graph.library.metric.directed.VertexMetrics<StringValue, NullValue, NullValue>());
							em = graph
								.run(new org.apache.flink.graph.library.metric.directed.EdgeMetrics<StringValue, NullValue, NullValue>());
						} else {
							if (parameters.getBoolean("simplify", false)) {
								graph = graph
									.run(new org.apache.flink.graph.asm.simple.undirected.Simplify<StringValue, NullValue, NullValue>(false));
							}

							vm = graph
								.run(new org.apache.flink.graph.library.metric.undirected.VertexMetrics<StringValue, NullValue, NullValue>());
							em = graph
								.run(new org.apache.flink.graph.library.metric.undirected.EdgeMetrics<StringValue, NullValue, NullValue>());
						}
					} break;

					default:
						throw new ProgramParametrizationException(getUsage("invalid CSV type"));
				}
				} break;

			case "rmat": {
				int scale = parameters.getInt("scale", DEFAULT_SCALE);
				int edgeFactor = parameters.getInt("edge_factor", DEFAULT_EDGE_FACTOR);

				RandomGenerableFactory<JDKRandomGenerator> rnd = new JDKRandomGeneratorFactory();

				long vertexCount = 1L << scale;
				long edgeCount = vertexCount * edgeFactor;


				Graph<LongValue, NullValue, NullValue> graph = new RMatGraph<>(env, rnd, vertexCount, edgeCount)
					.generate();

				if (directedAlgorithm) {
					if (scale > 32) {
						Graph<LongValue, NullValue, NullValue> newGraph = graph
							.run(new org.apache.flink.graph.asm.simple.directed.Simplify<LongValue, NullValue, NullValue>());

						vm = newGraph
							.run(new org.apache.flink.graph.library.metric.directed.VertexMetrics<LongValue, NullValue, NullValue>());
						em = newGraph
							.run(new org.apache.flink.graph.library.metric.directed.EdgeMetrics<LongValue, NullValue, NullValue>());
					} else {
						Graph<IntValue, NullValue, NullValue> newGraph = graph
							.run(new TranslateGraphIds<LongValue, IntValue, NullValue, NullValue>(new LongValueToUnsignedIntValue()))
							.run(new org.apache.flink.graph.asm.simple.directed.Simplify<IntValue, NullValue, NullValue>());

						vm = newGraph
							.run(new org.apache.flink.graph.library.metric.directed.VertexMetrics<IntValue, NullValue, NullValue>());
						em = newGraph
							.run(new org.apache.flink.graph.library.metric.directed.EdgeMetrics<IntValue, NullValue, NullValue>());
					}
				} else {
					boolean clipAndFlip = parameters.getBoolean("clip_and_flip", DEFAULT_CLIP_AND_FLIP);

					if (scale > 32) {
						Graph<LongValue, NullValue, NullValue> newGraph = graph
							.run(new org.apache.flink.graph.asm.simple.undirected.Simplify<LongValue, NullValue, NullValue>(clipAndFlip));

						vm = newGraph
							.run(new org.apache.flink.graph.library.metric.undirected.VertexMetrics<LongValue, NullValue, NullValue>());
						em = newGraph
							.run(new org.apache.flink.graph.library.metric.undirected.EdgeMetrics<LongValue, NullValue, NullValue>());
					} else {
						Graph<IntValue, NullValue, NullValue> newGraph = graph
							.run(new TranslateGraphIds<LongValue, IntValue, NullValue, NullValue>(new LongValueToUnsignedIntValue()))
							.run(new org.apache.flink.graph.asm.simple.undirected.Simplify<IntValue, NullValue, NullValue>(clipAndFlip));

						vm = newGraph
							.run(new org.apache.flink.graph.library.metric.undirected.VertexMetrics<IntValue, NullValue, NullValue>());
						em = newGraph
							.run(new org.apache.flink.graph.library.metric.undirected.EdgeMetrics<IntValue, NullValue, NullValue>());
					}
				}
				} break;

			default:
				throw new ProgramParametrizationException(getUsage("invalid input type"));
		}

		env.execute("Graph Metrics");

		System.out.print("Vertex metrics:\n  ");
		System.out.println(vm.getResult().toString().replace(";", "\n "));
		System.out.print("\nEdge metrics:\n  ");
		System.out.println(em.getResult().toString().replace(";", "\n "));

		JobExecutionResult result = env.getLastJobExecutionResult();

		NumberFormat nf = NumberFormat.getInstance();
		System.out.println("\nExecution runtime: " + nf.format(result.getNetRuntime()) + " ms");
	}
}
