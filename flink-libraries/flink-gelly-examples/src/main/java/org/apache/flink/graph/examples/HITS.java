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
import org.apache.flink.graph.GraphCsvReader;
import org.apache.flink.graph.asm.simple.directed.Simplify;
import org.apache.flink.graph.asm.translate.LongValueToIntValue;
import org.apache.flink.graph.asm.translate.TranslateGraphIds;
import org.apache.flink.graph.generator.RMatGraph;
import org.apache.flink.graph.generator.random.JDKRandomGeneratorFactory;
import org.apache.flink.graph.generator.random.RandomGenerableFactory;
import org.apache.flink.graph.library.link_analysis.HITS.Result;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;
import org.apache.flink.types.StringValue;

import java.text.NumberFormat;

/**
 * Driver for the library implementation of HITS (Hubs and Authorities).
 *
 * This example reads a simple, undirected graph from a CSV file or generates
 * an undirected RMat graph with the given scale and edge factor then calculates
 * hub and authority scores for each vertex.
 *
 * @see org.apache.flink.graph.library.link_analysis.HITS
 */
public class HITS {

	public static final int DEFAULT_ITERATIONS = 10;

	public static final int DEFAULT_SCALE = 10;

	public static final int DEFAULT_EDGE_FACTOR = 16;

	private static void printUsage() {
		System.out.println(WordUtils.wrap("Hyperlink-Induced Topic Search computes two interdependent" +
			" scores for every vertex in a directed graph. A good \"hub\" links to good \"authorities\"" +
			" and good \"authorities\" are linked from good \"hubs\".", 80));
		System.out.println();
		System.out.println("usage: HITS --input <csv | rmat [options]> --output <print | hash | csv [options]");
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
		int iterations = parameters.getInt("iterations", DEFAULT_ITERATIONS);

		DataSet hits;

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
						hits = reader
							.keyType(LongValue.class)
							.run(new org.apache.flink.graph.library.link_analysis.HITS<LongValue, NullValue, NullValue>(iterations));
					} break;

					case "string": {
						hits = reader
							.keyType(StringValue.class)
							.run(new org.apache.flink.graph.library.link_analysis.HITS<StringValue, NullValue, NullValue>(iterations));
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

				if (scale > 32) {
					hits = graph
						.run(new Simplify<LongValue, NullValue, NullValue>())
						.run(new org.apache.flink.graph.library.link_analysis.HITS<LongValue, NullValue, NullValue>(iterations));
				} else {
					hits = graph
						.run(new TranslateGraphIds<LongValue, IntValue, NullValue, NullValue>(new LongValueToIntValue()))
						.run(new Simplify<IntValue, NullValue, NullValue>())
						.run(new org.apache.flink.graph.library.link_analysis.HITS<IntValue, NullValue, NullValue>(iterations));
				}
				} break;

			default:
				printUsage();
				return;
		}

		switch (parameters.get("output", "")) {
			case "print":
				for (Object e: hits.collect()) {
					System.out.println(((Result)e).toVerboseString());
				}
				break;

			case "hash":
				System.out.println(DataSetUtils.checksumHashCode(hits));
				break;

			case "csv":
				String filename = parameters.get("output_filename");

				String lineDelimiter = StringEscapeUtils.unescapeJava(
					parameters.get("output_line_delimiter", CsvOutputFormat.DEFAULT_LINE_DELIMITER));

				String fieldDelimiter = StringEscapeUtils.unescapeJava(
					parameters.get("output_field_delimiter", CsvOutputFormat.DEFAULT_FIELD_DELIMITER));

				hits.writeAsCsv(filename, lineDelimiter, fieldDelimiter);

				env.execute();
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
