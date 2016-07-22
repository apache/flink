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
import org.apache.flink.graph.asm.simple.undirected.Simplify;
import org.apache.flink.graph.asm.translate.LongValueToIntValue;
import org.apache.flink.graph.asm.translate.TranslateGraphIds;
import org.apache.flink.graph.generator.RMatGraph;
import org.apache.flink.graph.generator.random.JDKRandomGeneratorFactory;
import org.apache.flink.graph.generator.random.RandomGenerableFactory;
import org.apache.flink.graph.library.similarity.JaccardIndex.Result;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;
import org.apache.flink.types.StringValue;

import java.text.NumberFormat;

/**
 * Driver for the library implementation of Jaccard Index.
 *
 * This example reads a simple, undirected graph from a CSV file or generates
 * an undirected RMat graph with the given scale and edge factor then calculates
 * all non-zero Jaccard Index similarity scores between vertices.
 *
 * @see org.apache.flink.graph.library.similarity.JaccardIndex
 */
public class JaccardIndex {

	public static final int DEFAULT_SCALE = 10;

	public static final int DEFAULT_EDGE_FACTOR = 16;

	public static final boolean DEFAULT_CLIP_AND_FLIP = true;

	private static void printUsage() {
		System.out.println(WordUtils.wrap("The Jaccard Index measures the similarity between vertex" +
			" neighborhoods and is computed as the number of shared neighbors divided by the number of" +
			" distinct neighbors. Scores range from 0.0 (no shared neighbors) to 1.0 (all neighbors are" +
			" shared).", 80));
		System.out.println();
		System.out.println(WordUtils.wrap("This algorithm returns 4-tuples containing two vertex IDs, the" +
			" number of shared neighbors, and the number of distinct neighbors.", 80));
		System.out.println();
		System.out.println("usage: JaccardIndex --input <csv | rmat [options]> --output <print | hash | csv [options]");
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

		DataSet ji;

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
						ji = reader
							.keyType(LongValue.class)
							.run(new org.apache.flink.graph.library.similarity.JaccardIndex<LongValue, NullValue, NullValue>());
					} break;

					case "string": {
						ji = reader
							.keyType(StringValue.class)
							.run(new org.apache.flink.graph.library.similarity.JaccardIndex<StringValue, NullValue, NullValue>());
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

				boolean clipAndFlip = parameters.getBoolean("clip_and_flip", DEFAULT_CLIP_AND_FLIP);

				Graph<LongValue, NullValue, NullValue> graph = new RMatGraph<>(env, rnd, vertexCount, edgeCount)
					.generate();

				if (scale > 32) {
					ji = graph
						.run(new Simplify<LongValue, NullValue, NullValue>(clipAndFlip))
						.run(new org.apache.flink.graph.library.similarity.JaccardIndex<LongValue, NullValue, NullValue>());
				} else {
					ji = graph
						.run(new TranslateGraphIds<LongValue, IntValue, NullValue, NullValue>(new LongValueToIntValue()))
						.run(new Simplify<IntValue, NullValue, NullValue>(clipAndFlip))
						.run(new org.apache.flink.graph.library.similarity.JaccardIndex<IntValue, NullValue, NullValue>());
				}
				} break;

			default:
				printUsage();
				return;
		}

		switch (parameters.get("output", "")) {
			case "print":
				for (Object e: ji.collect()) {
					Result result = (Result)e;
					System.out.println(result.toVerboseString());
				}
				break;

			case "hash":
				System.out.println(DataSetUtils.checksumHashCode(ji));
				break;

			case "csv":
				String filename = parameters.get("output_filename");

				String lineDelimiter = StringEscapeUtils.unescapeJava(
					parameters.get("output_line_delimiter", CsvOutputFormat.DEFAULT_LINE_DELIMITER));

				String fieldDelimiter = StringEscapeUtils.unescapeJava(
					parameters.get("output_field_delimiter", CsvOutputFormat.DEFAULT_FIELD_DELIMITER));

				ji.writeAsCsv(filename, lineDelimiter, fieldDelimiter);

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
