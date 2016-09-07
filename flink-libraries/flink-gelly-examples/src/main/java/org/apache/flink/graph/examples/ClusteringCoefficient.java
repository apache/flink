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

package org.apache.flink.graph.examples;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.text.WordUtils;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvOutputFormat;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAnalytic;
import org.apache.flink.graph.GraphCsvReader;
import org.apache.flink.graph.asm.translate.LongValueToIntValue;
import org.apache.flink.graph.asm.translate.TranslateGraphIds;
import org.apache.flink.graph.generator.RMatGraph;
import org.apache.flink.graph.generator.random.JDKRandomGeneratorFactory;
import org.apache.flink.graph.generator.random.RandomGenerableFactory;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;
import org.apache.flink.types.StringValue;

import java.text.NumberFormat;

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

	public static final int DEFAULT_SCALE = 10;

	public static final int DEFAULT_EDGE_FACTOR = 16;

	public static final boolean DEFAULT_CLIP_AND_FLIP = true;

	private static void printUsage() {
		System.out.println(WordUtils.wrap("The local clustering coefficient measures the connectedness of each" +
			" vertex's neighborhood and the global clustering coefficient measures the connectedness of the graph." +
			" Scores range from 0.0 (no edges between neighbors or vertices) to 1.0 (neighborhood or graph" +
			" is a clique).", 80));
		System.out.println();
		System.out.println(WordUtils.wrap("This algorithm returns tuples containing the vertex ID, the degree of" +
			" the vertex, and the number of edges between vertex neighbors.", 80));
		System.out.println();
		System.out.println("usage: ClusteringCoefficient --directed <true | false> --input <csv | rmat [options]> --output <print | hash | csv [options]>");
		System.out.println();
		System.out.println("options:");
		System.out.println("  --input csv --type <integer | string> --input_filename FILENAME [--input_line_delimiter LINE_DELIMITER] [--input_field_delimiter FIELD_DELIMITER]");
		System.out.println("  --input rmat [--scale SCALE] [--edge_factor EDGE_FACTOR]");
		System.out.println();
		System.out.println("  --output print");
		System.out.println("  --output hash");
		System.out.println("  --output csv --output_filename FILENAME [--output_line_delimiter LINE_DELIMITER] [--output_field_delimiter FIELD_DELIMITER]");
	}

	public static void main(String[] args) throws Exception {
		// Set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().enableObjectReuse();

		ParameterTool parameters = ParameterTool.fromArgs(args);
		if (! parameters.has("directed")) {
			printUsage();
			return;
		}
		boolean directedAlgorithm = parameters.getBoolean("directed");

		// global and local clustering coefficient results
		GraphAnalytic gcc;
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
							gcc = graph
								.run(new org.apache.flink.graph.library.clustering.directed.GlobalClusteringCoefficient<LongValue, NullValue, NullValue>());
							lcc = graph
								.run(new org.apache.flink.graph.library.clustering.directed.LocalClusteringCoefficient<LongValue, NullValue, NullValue>());
						} else {
							gcc = graph
								.run(new org.apache.flink.graph.library.clustering.undirected.GlobalClusteringCoefficient<LongValue, NullValue, NullValue>());
							lcc = graph
								.run(new org.apache.flink.graph.library.clustering.undirected.LocalClusteringCoefficient<LongValue, NullValue, NullValue>());
						}
					} break;

					case "string": {
						Graph<StringValue, NullValue, NullValue> graph = reader
							.keyType(StringValue.class);

						if (directedAlgorithm) {
							gcc = graph
								.run(new org.apache.flink.graph.library.clustering.directed.GlobalClusteringCoefficient<StringValue, NullValue, NullValue>());
							lcc = graph
								.run(new org.apache.flink.graph.library.clustering.directed.LocalClusteringCoefficient<StringValue, NullValue, NullValue>());
						} else {
							gcc = graph
								.run(new org.apache.flink.graph.library.clustering.undirected.GlobalClusteringCoefficient<StringValue, NullValue, NullValue>());
							lcc = graph
								.run(new org.apache.flink.graph.library.clustering.undirected.LocalClusteringCoefficient<StringValue, NullValue, NullValue>());
						}
					} break;

					default:
						printUsage();
						return;
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

						gcc = newGraph
							.run(new org.apache.flink.graph.library.clustering.directed.GlobalClusteringCoefficient<LongValue, NullValue, NullValue>());
						lcc = newGraph
							.run(new org.apache.flink.graph.library.clustering.directed.LocalClusteringCoefficient<LongValue, NullValue, NullValue>()
								.setIncludeZeroDegreeVertices(false));
					} else {
						Graph<IntValue, NullValue, NullValue> newGraph = graph
							.run(new TranslateGraphIds<LongValue, IntValue, NullValue, NullValue>(new LongValueToIntValue()))
							.run(new org.apache.flink.graph.asm.simple.directed.Simplify<IntValue, NullValue, NullValue>());

						gcc = newGraph
							.run(new org.apache.flink.graph.library.clustering.directed.GlobalClusteringCoefficient<IntValue, NullValue, NullValue>());
						lcc = newGraph
							.run(new org.apache.flink.graph.library.clustering.directed.LocalClusteringCoefficient<IntValue, NullValue, NullValue>()
								.setIncludeZeroDegreeVertices(false));
					}
				} else {
					boolean clipAndFlip = parameters.getBoolean("clip_and_flip", DEFAULT_CLIP_AND_FLIP);

					if (scale > 32) {
						Graph<LongValue, NullValue, NullValue> newGraph = graph
							.run(new org.apache.flink.graph.asm.simple.undirected.Simplify<LongValue, NullValue, NullValue>(clipAndFlip));

						gcc = newGraph
							.run(new org.apache.flink.graph.library.clustering.undirected.GlobalClusteringCoefficient<LongValue, NullValue, NullValue>());
						lcc = newGraph
							.run(new org.apache.flink.graph.library.clustering.undirected.LocalClusteringCoefficient<LongValue, NullValue, NullValue>()
								.setIncludeZeroDegreeVertices(false));
					} else {
						Graph<IntValue, NullValue, NullValue> newGraph = graph
							.run(new TranslateGraphIds<LongValue, IntValue, NullValue, NullValue>(new LongValueToIntValue()))
							.run(new org.apache.flink.graph.asm.simple.undirected.Simplify<IntValue, NullValue, NullValue>(clipAndFlip));

						gcc = newGraph
							.run(new org.apache.flink.graph.library.clustering.undirected.GlobalClusteringCoefficient<IntValue, NullValue, NullValue>());
						lcc = newGraph
							.run(new org.apache.flink.graph.library.clustering.undirected.LocalClusteringCoefficient<IntValue, NullValue, NullValue>()
								.setIncludeZeroDegreeVertices(false));
					}
				}
			} break;

			default:
				printUsage();
				return;
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
				System.out.println(gcc.getResult());
				break;

			case "hash":
				System.out.println(DataSetUtils.checksumHashCode(lcc));
				System.out.println(gcc.getResult());
				break;

			case "csv":
				String filename = parameters.get("output_filename");

				String lineDelimiter = StringEscapeUtils.unescapeJava(
					parameters.get("output_line_delimiter", CsvOutputFormat.DEFAULT_LINE_DELIMITER));

				String fieldDelimiter = StringEscapeUtils.unescapeJava(
					parameters.get("output_field_delimiter", CsvOutputFormat.DEFAULT_FIELD_DELIMITER));

				lcc.writeAsCsv(filename, lineDelimiter, fieldDelimiter);

				System.out.println(gcc.execute());
				break;

			default:
				printUsage();
				return;
		}

		JobExecutionResult result = env.getLastJobExecutionResult();

		NumberFormat nf = NumberFormat.getInstance();
		System.out.println("Execution runtime: " + nf.format(result.getNetRuntime()) + " ms");
	}
}
