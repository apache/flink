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
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvOutputFormat;
import org.apache.flink.api.java.utils.DataSetUtils;
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

import static org.apache.flink.api.common.ExecutionConfig.PARALLELISM_DEFAULT;

/**
 * Driver for the library implementations of Global and Local Clustering Coefficient.
 *
 * This example reads a simple directed or undirected graph from a CSV file or
 * generates an RMat graph with the given scale and edge factor then calculates
 * the local clustering coefficient for each vertex and the global clustering
 * coefficient for the graph.
 *
 * @see org.apache.flink.graph.library.clustering.directed.GlobalClusteringCoefficient
 * @see org.apache.flink.graph.library.clustering.directed.LocalClusteringCoefficient
 * @see org.apache.flink.graph.library.clustering.undirected.GlobalClusteringCoefficient
 * @see org.apache.flink.graph.library.clustering.undirected.LocalClusteringCoefficient
 */
public class ClusteringCoefficient {

	private static final int DEFAULT_SCALE = 10;

	private static final int DEFAULT_EDGE_FACTOR = 16;

	private static final boolean DEFAULT_CLIP_AND_FLIP = true;

	private static String getUsage(String message) {
		return new StrBuilder()
			.appendNewLine()
			.appendln(WordUtils.wrap("The local clustering coefficient measures the connectedness of each" +
				" vertex's neighborhood and the global clustering coefficient measures the connectedness of the graph." +
				" Scores range from 0.0 (no edges between neighbors or vertices) to 1.0 (neighborhood or graph" +
				" is a clique).", 80))
			.appendNewLine()
			.appendln(WordUtils.wrap("This algorithm returns tuples containing the vertex ID, the degree of" +
				" the vertex, and the number of edges between vertex neighbors.", 80))
			.appendNewLine()
			.appendln("usage: ClusteringCoefficient --directed <true | false> --input <csv | rmat [options]> --output <print | hash | csv [options]>")
			.appendNewLine()
			.appendln("options:")
			.appendln("  --input csv --type <integer | string> [--simplify <true | false>] --input_filename FILENAME [--input_line_delimiter LINE_DELIMITER] [--input_field_delimiter FIELD_DELIMITER]")
			.appendln("  --input rmat [--scale SCALE] [--edge_factor EDGE_FACTOR]")
			.appendNewLine()
			.appendln("  --output print")
			.appendln("  --output hash")
			.appendln("  --output csv --output_filename FILENAME [--output_line_delimiter LINE_DELIMITER] [--output_field_delimiter FIELD_DELIMITER]")
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

		int little_parallelism = parameters.getInt("little_parallelism", PARALLELISM_DEFAULT);

		// global and local clustering coefficient results
		GraphAnalytic gcc;
		GraphAnalytic acc;
		DataSet lcc;

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
									.run(new org.apache.flink.graph.asm.simple.directed.Simplify<LongValue, NullValue, NullValue>()
										.setParallelism(little_parallelism));
							}

							gcc = graph
								.run(new org.apache.flink.graph.library.clustering.directed.GlobalClusteringCoefficient<LongValue, NullValue, NullValue>()
									.setLittleParallelism(little_parallelism));
							acc = graph
								.run(new org.apache.flink.graph.library.clustering.directed.AverageClusteringCoefficient<LongValue, NullValue, NullValue>()
									.setLittleParallelism(little_parallelism));
							lcc = graph
								.run(new org.apache.flink.graph.library.clustering.directed.LocalClusteringCoefficient<LongValue, NullValue, NullValue>()
									.setLittleParallelism(little_parallelism));
						} else {
							if (parameters.getBoolean("simplify", false)) {
								graph = graph
									.run(new org.apache.flink.graph.asm.simple.undirected.Simplify<LongValue, NullValue, NullValue>(false)
										.setParallelism(little_parallelism));
							}

							gcc = graph
								.run(new org.apache.flink.graph.library.clustering.undirected.GlobalClusteringCoefficient<LongValue, NullValue, NullValue>()
									.setLittleParallelism(little_parallelism));
							acc = graph
								.run(new org.apache.flink.graph.library.clustering.undirected.AverageClusteringCoefficient<LongValue, NullValue, NullValue>()
									.setLittleParallelism(little_parallelism));
							lcc = graph
								.run(new org.apache.flink.graph.library.clustering.undirected.LocalClusteringCoefficient<LongValue, NullValue, NullValue>()
									.setLittleParallelism(little_parallelism));
						}
					} break;

					case "string": {
						Graph<StringValue, NullValue, NullValue> graph = reader
							.keyType(StringValue.class);

						if (directedAlgorithm) {
							if (parameters.getBoolean("simplify", false)) {
								graph = graph
									.run(new org.apache.flink.graph.asm.simple.directed.Simplify<StringValue, NullValue, NullValue>()
										.setParallelism(little_parallelism));
							}

							gcc = graph
								.run(new org.apache.flink.graph.library.clustering.directed.GlobalClusteringCoefficient<StringValue, NullValue, NullValue>()
									.setLittleParallelism(little_parallelism));
							acc = graph
								.run(new org.apache.flink.graph.library.clustering.directed.AverageClusteringCoefficient<StringValue, NullValue, NullValue>()
									.setLittleParallelism(little_parallelism));
							lcc = graph
								.run(new org.apache.flink.graph.library.clustering.directed.LocalClusteringCoefficient<StringValue, NullValue, NullValue>()
									.setLittleParallelism(little_parallelism));
						} else {
							if (parameters.getBoolean("simplify", false)) {
								graph = graph
									.run(new org.apache.flink.graph.asm.simple.undirected.Simplify<StringValue, NullValue, NullValue>(false)
										.setParallelism(little_parallelism));
							}

							gcc = graph
								.run(new org.apache.flink.graph.library.clustering.undirected.GlobalClusteringCoefficient<StringValue, NullValue, NullValue>()
									.setLittleParallelism(little_parallelism));
							acc = graph
								.run(new org.apache.flink.graph.library.clustering.undirected.AverageClusteringCoefficient<StringValue, NullValue, NullValue>()
									.setLittleParallelism(little_parallelism));
							lcc = graph
								.run(new org.apache.flink.graph.library.clustering.undirected.LocalClusteringCoefficient<StringValue, NullValue, NullValue>()
									.setLittleParallelism(little_parallelism));
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
					.setParallelism(little_parallelism)
					.generate();

				if (directedAlgorithm) {
					if (scale > 32) {
						Graph<LongValue, NullValue, NullValue> newGraph = graph
							.run(new org.apache.flink.graph.asm.simple.directed.Simplify<LongValue, NullValue, NullValue>()
								.setParallelism(little_parallelism));

						gcc = newGraph
							.run(new org.apache.flink.graph.library.clustering.directed.GlobalClusteringCoefficient<LongValue, NullValue, NullValue>()
								.setLittleParallelism(little_parallelism));
						acc = newGraph
							.run(new org.apache.flink.graph.library.clustering.directed.AverageClusteringCoefficient<LongValue, NullValue, NullValue>()
								.setLittleParallelism(little_parallelism));
						lcc = newGraph
							.run(new org.apache.flink.graph.library.clustering.directed.LocalClusteringCoefficient<LongValue, NullValue, NullValue>()
								.setIncludeZeroDegreeVertices(false)
								.setLittleParallelism(little_parallelism));
					} else {
						Graph<IntValue, NullValue, NullValue> newGraph = graph
							.run(new TranslateGraphIds<LongValue, IntValue, NullValue, NullValue>(new LongValueToUnsignedIntValue())
								.setParallelism(little_parallelism))
							.run(new org.apache.flink.graph.asm.simple.directed.Simplify<IntValue, NullValue, NullValue>()
								.setParallelism(little_parallelism));

						gcc = newGraph
							.run(new org.apache.flink.graph.library.clustering.directed.GlobalClusteringCoefficient<IntValue, NullValue, NullValue>()
								.setLittleParallelism(little_parallelism));
						acc = newGraph
							.run(new org.apache.flink.graph.library.clustering.directed.AverageClusteringCoefficient<IntValue, NullValue, NullValue>()
								.setLittleParallelism(little_parallelism));
						lcc = newGraph
							.run(new org.apache.flink.graph.library.clustering.directed.LocalClusteringCoefficient<IntValue, NullValue, NullValue>()
								.setIncludeZeroDegreeVertices(false)
								.setLittleParallelism(little_parallelism));
					}
				} else {
					boolean clipAndFlip = parameters.getBoolean("clip_and_flip", DEFAULT_CLIP_AND_FLIP);

					if (scale > 32) {
						Graph<LongValue, NullValue, NullValue> newGraph = graph
							.run(new org.apache.flink.graph.asm.simple.undirected.Simplify<LongValue, NullValue, NullValue>(clipAndFlip)
								.setParallelism(little_parallelism));

						gcc = newGraph
							.run(new org.apache.flink.graph.library.clustering.undirected.GlobalClusteringCoefficient<LongValue, NullValue, NullValue>()
								.setLittleParallelism(little_parallelism));
						acc = newGraph
							.run(new org.apache.flink.graph.library.clustering.undirected.AverageClusteringCoefficient<LongValue, NullValue, NullValue>()
								.setLittleParallelism(little_parallelism));
						lcc = newGraph
							.run(new org.apache.flink.graph.library.clustering.undirected.LocalClusteringCoefficient<LongValue, NullValue, NullValue>()
								.setIncludeZeroDegreeVertices(false)
								.setLittleParallelism(little_parallelism));
					} else {
						Graph<IntValue, NullValue, NullValue> newGraph = graph
							.run(new TranslateGraphIds<LongValue, IntValue, NullValue, NullValue>(new LongValueToUnsignedIntValue())
								.setParallelism(little_parallelism))
							.run(new org.apache.flink.graph.asm.simple.undirected.Simplify<IntValue, NullValue, NullValue>(clipAndFlip)
								.setParallelism(little_parallelism));

						gcc = newGraph
							.run(new org.apache.flink.graph.library.clustering.undirected.GlobalClusteringCoefficient<IntValue, NullValue, NullValue>()
								.setLittleParallelism(little_parallelism));
						acc = newGraph
							.run(new org.apache.flink.graph.library.clustering.undirected.AverageClusteringCoefficient<IntValue, NullValue, NullValue>()
								.setLittleParallelism(little_parallelism));
						lcc = newGraph
							.run(new org.apache.flink.graph.library.clustering.undirected.LocalClusteringCoefficient<IntValue, NullValue, NullValue>()
								.setIncludeZeroDegreeVertices(false)
								.setLittleParallelism(little_parallelism));
					}
				}
			} break;

			default:
				throw new ProgramParametrizationException(getUsage("invalid input type"));
		}

		switch (parameters.get("output", "")) {
			case "print":
				if (directedAlgorithm) {
					for (Object e: lcc.collect()) {
						org.apache.flink.graph.library.clustering.directed.LocalClusteringCoefficient.Result result =
							(org.apache.flink.graph.library.clustering.directed.LocalClusteringCoefficient.Result)e;
						System.out.println(result.toVerboseString());
					}
				} else {
					for (Object e: lcc.collect()) {
						org.apache.flink.graph.library.clustering.undirected.LocalClusteringCoefficient.Result result =
							(org.apache.flink.graph.library.clustering.undirected.LocalClusteringCoefficient.Result)e;
						System.out.println(result.toVerboseString());
					}
				}
				break;

			case "hash":
				System.out.println(DataSetUtils.checksumHashCode(lcc));
				break;

			case "csv":
				String filename = parameters.get("output_filename");

				String lineDelimiter = StringEscapeUtils.unescapeJava(
					parameters.get("output_line_delimiter", CsvOutputFormat.DEFAULT_LINE_DELIMITER));

				String fieldDelimiter = StringEscapeUtils.unescapeJava(
					parameters.get("output_field_delimiter", CsvOutputFormat.DEFAULT_FIELD_DELIMITER));

				lcc.writeAsCsv(filename, lineDelimiter, fieldDelimiter);

				env.execute("Clustering Coefficient");
				break;

			default:
				throw new ProgramParametrizationException(getUsage("invalid output type"));
		}

		System.out.println(gcc.getResult());
		System.out.println(acc.getResult());

		JobExecutionResult result = env.getLastJobExecutionResult();

		NumberFormat nf = NumberFormat.getInstance();
		System.out.println("Execution runtime: " + nf.format(result.getNetRuntime()) + " ms");
	}
}
