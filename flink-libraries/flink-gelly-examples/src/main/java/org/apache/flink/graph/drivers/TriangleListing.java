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
 * Driver for the library implementation of Triangle Listing.
 *
 * This example reads a simple directed or undirected graph from a CSV file or
 * generates an RMat graph with the given scale and edge factor then lists
 * all triangles.
 *
 * @see org.apache.flink.graph.library.clustering.directed.TriangleListing
 * @see org.apache.flink.graph.library.clustering.undirected.TriangleListing
 */
public class TriangleListing {

	private static final int DEFAULT_SCALE = 10;

	private static final int DEFAULT_EDGE_FACTOR = 16;

	private static final boolean DEFAULT_TRIADIC_CENSUS = true;

	private static final boolean DEFAULT_CLIP_AND_FLIP = true;

	private static String getUsage(String message) {
		return new StrBuilder()
			.appendNewLine()
			.appendln(WordUtils.wrap("Lists all triangles in a graph.", 80))
			.appendNewLine()
			.appendln(WordUtils.wrap("This algorithm returns tuples containing the vertex IDs for each triangle and" +
				" for directed graphs a bitmask indicating the presence of the six potential connecting edges.", 80))
			.appendNewLine()
			.appendln("usage: TriangleListing --directed <true | false> [--triadic_census <true | false>] --input <csv | rmat> --output <print | hash | csv>")
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
		boolean triadic_census = parameters.getBoolean("triadic_census", DEFAULT_TRIADIC_CENSUS);

		GraphAnalytic tc = null;
		DataSet tl;

		switch (parameters.get("input", "")) {
			case "csv": {
				String lineDelimiter = StringEscapeUtils.unescapeJava(
					parameters.get("input_line_delimiter", CsvOutputFormat.DEFAULT_LINE_DELIMITER));

				String fieldDelimiter = StringEscapeUtils.unescapeJava(
					parameters.get("input_field_delimiter", CsvOutputFormat.DEFAULT_FIELD_DELIMITER));

				GraphCsvReader reader = Graph
					.fromCsvReader(parameters.getRequired("input_filename"), env)
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

							if (triadic_census) {
								tc = graph
									.run(new org.apache.flink.graph.library.clustering.directed.TriadicCensus<LongValue, NullValue, NullValue>()
										.setLittleParallelism(little_parallelism));
							}
							tl = graph
								.run(new org.apache.flink.graph.library.clustering.directed.TriangleListing<LongValue, NullValue, NullValue>()
									.setLittleParallelism(little_parallelism));
						} else {
							if (parameters.getBoolean("simplify", false)) {
								graph = graph
									.run(new org.apache.flink.graph.asm.simple.undirected.Simplify<LongValue, NullValue, NullValue>(false)
										.setParallelism(little_parallelism));
							}

							if (triadic_census) {
								tc = graph
									.run(new org.apache.flink.graph.library.clustering.undirected.TriadicCensus<LongValue, NullValue, NullValue>()
										.setLittleParallelism(little_parallelism));
							}
							tl = graph
								.run(new org.apache.flink.graph.library.clustering.undirected.TriangleListing<LongValue, NullValue, NullValue>()
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

							if (triadic_census) {
								tc = graph
									.run(new org.apache.flink.graph.library.clustering.directed.TriadicCensus<StringValue, NullValue, NullValue>()
										.setLittleParallelism(little_parallelism));
							}
							tl = graph
								.run(new org.apache.flink.graph.library.clustering.directed.TriangleListing<StringValue, NullValue, NullValue>()
									.setLittleParallelism(little_parallelism));
						} else {
							if (parameters.getBoolean("simplify", false)) {
								graph = graph
									.run(new org.apache.flink.graph.asm.simple.undirected.Simplify<StringValue, NullValue, NullValue>(false)
										.setParallelism(little_parallelism));
							}

							if (triadic_census) {
								tc = graph
									.run(new org.apache.flink.graph.library.clustering.undirected.TriadicCensus<StringValue, NullValue, NullValue>()
										.setLittleParallelism(little_parallelism));
							}
							tl = graph
								.run(new org.apache.flink.graph.library.clustering.undirected.TriangleListing<StringValue, NullValue, NullValue>()
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
					.generate();

				if (directedAlgorithm) {
					if (scale > 32) {
						Graph<LongValue, NullValue, NullValue> simpleGraph = graph
							.run(new org.apache.flink.graph.asm.simple.directed.Simplify<LongValue, NullValue, NullValue>()
								.setParallelism(little_parallelism));

						if (triadic_census) {
							tc = simpleGraph
								.run(new org.apache.flink.graph.library.clustering.directed.TriadicCensus<LongValue, NullValue, NullValue>()
									.setLittleParallelism(little_parallelism));
						}
						tl = simpleGraph
							.run(new org.apache.flink.graph.library.clustering.directed.TriangleListing<LongValue, NullValue, NullValue>()
								.setLittleParallelism(little_parallelism));
					} else {
						Graph<LongValue, NullValue, NullValue> simpleGraph = graph
							.run(new org.apache.flink.graph.asm.simple.directed.Simplify<LongValue, NullValue, NullValue>()
								.setParallelism(little_parallelism));

						if (triadic_census) {
							tc = simpleGraph
								.run(new org.apache.flink.graph.library.clustering.directed.TriadicCensus<LongValue, NullValue, NullValue>()
									.setLittleParallelism(little_parallelism));
						}
						tl = simpleGraph
							.run(new org.apache.flink.graph.library.clustering.directed.TriangleListing<LongValue, NullValue, NullValue>()
								.setLittleParallelism(little_parallelism));
					}
				} else {
					boolean clipAndFlip = parameters.getBoolean("clip_and_flip", DEFAULT_CLIP_AND_FLIP);

					if (scale > 32) {
						Graph<LongValue, NullValue, NullValue> simpleGraph = graph
							.run(new org.apache.flink.graph.asm.simple.undirected.Simplify<LongValue, NullValue, NullValue>(clipAndFlip)
								.setParallelism(little_parallelism));

						if (triadic_census) {
							tc = simpleGraph
								.run(new org.apache.flink.graph.library.clustering.undirected.TriadicCensus<LongValue, NullValue, NullValue>()
									.setLittleParallelism(little_parallelism));
						}
						tl = simpleGraph
							.run(new org.apache.flink.graph.library.clustering.undirected.TriangleListing<LongValue, NullValue, NullValue>()
								.setLittleParallelism(little_parallelism));
					} else {
						Graph<IntValue, NullValue, NullValue> simpleGraph = graph
							.run(new TranslateGraphIds<LongValue, IntValue, NullValue, NullValue>(new LongValueToUnsignedIntValue())
								.setParallelism(little_parallelism))
							.run(new org.apache.flink.graph.asm.simple.undirected.Simplify<IntValue, NullValue, NullValue>(clipAndFlip)
								.setParallelism(little_parallelism));

						if (triadic_census) {
							tc = simpleGraph
								.run(new org.apache.flink.graph.library.clustering.undirected.TriadicCensus<IntValue, NullValue, NullValue>()
									.setLittleParallelism(little_parallelism));
						}
						tl = simpleGraph
							.run(new org.apache.flink.graph.library.clustering.undirected.TriangleListing<IntValue, NullValue, NullValue>()
								.setLittleParallelism(little_parallelism));
					}
				}
			} break;

			default:
				throw new ProgramParametrizationException(getUsage("invalid input type"));
		}

		switch (parameters.get("output", "")) {
			case "print":
				System.out.println();
				if (directedAlgorithm) {
					for (Object e: tl.collect()) {
						org.apache.flink.graph.library.clustering.directed.TriangleListing.Result result =
							(org.apache.flink.graph.library.clustering.directed.TriangleListing.Result) e;
						System.out.println(result.toPrintableString());
					}
				} else {
					tl.print();
				}
				break;

			case "hash":
				System.out.println();
				System.out.println(DataSetUtils.checksumHashCode(tl));
				break;

			case "csv":
				String filename = parameters.getRequired("output_filename");

				String lineDelimiter = StringEscapeUtils.unescapeJava(
					parameters.get("output_line_delimiter", CsvOutputFormat.DEFAULT_LINE_DELIMITER));

				String fieldDelimiter = StringEscapeUtils.unescapeJava(
					parameters.get("output_field_delimiter", CsvOutputFormat.DEFAULT_FIELD_DELIMITER));

				tl.writeAsCsv(filename, lineDelimiter, fieldDelimiter);

				env.execute();
				break;
			default:
				throw new ProgramParametrizationException(getUsage("invalid output type"));
		}

		if (tc != null) {
			System.out.print("Triadic census:\n  ");
			System.out.println(tc.getResult().toString().replace(";", "\n "));
		}

		JobExecutionResult result = env.getLastJobExecutionResult();

		NumberFormat nf = NumberFormat.getInstance();
		System.out.println();
		System.out.println("Execution runtime: " + nf.format(result.getNetRuntime()) + " ms");
	}
}
