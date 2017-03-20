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
import org.apache.flink.graph.GraphCsvReader;
import org.apache.flink.graph.asm.simple.undirected.Simplify;
import org.apache.flink.graph.asm.translate.TranslateGraphIds;
import org.apache.flink.graph.asm.translate.translators.LongValueToUnsignedIntValue;
import org.apache.flink.graph.generator.RMatGraph;
import org.apache.flink.graph.generator.random.JDKRandomGeneratorFactory;
import org.apache.flink.graph.generator.random.RandomGenerableFactory;
import org.apache.flink.graph.library.similarity.JaccardIndex.Result;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;
import org.apache.flink.types.StringValue;

import java.text.NumberFormat;

import static org.apache.flink.api.common.ExecutionConfig.PARALLELISM_DEFAULT;

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

	private static final int DEFAULT_SCALE = 10;

	private static final int DEFAULT_EDGE_FACTOR = 16;

	private static final boolean DEFAULT_CLIP_AND_FLIP = true;

	private static String getUsage(String message) {
		return new StrBuilder()
			.appendNewLine()
			.appendln(WordUtils.wrap("The Jaccard Index measures the similarity between vertex" +
				" neighborhoods and is computed as the number of shared neighbors divided by the number of" +
				" distinct neighbors. Scores range from 0.0 (no shared neighbors) to 1.0 (all neighbors are" +
				" shared).", 80))
			.appendNewLine()
			.appendln(WordUtils.wrap("This algorithm returns 4-tuples containing two vertex IDs, the" +
				" number of shared neighbors, and the number of distinct neighbors.", 80))
			.appendNewLine()
			.appendln("usage: JaccardIndex --input <csv | rmat> --output <print | hash | csv>")
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

		int little_parallelism = parameters.getInt("little_parallelism", PARALLELISM_DEFAULT);

		DataSet ji;

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

						if (parameters.getBoolean("simplify", false)) {
							graph = graph
								.run(new org.apache.flink.graph.asm.simple.undirected.Simplify<LongValue, NullValue, NullValue>(false)
									.setParallelism(little_parallelism));
						}

						ji = graph
							.run(new org.apache.flink.graph.library.similarity.JaccardIndex<LongValue, NullValue, NullValue>()
								.setLittleParallelism(little_parallelism));
					} break;

					case "string": {
						Graph<StringValue, NullValue, NullValue> graph = reader
							.keyType(StringValue.class);

						if (parameters.getBoolean("simplify", false)) {
							graph = graph
								.run(new org.apache.flink.graph.asm.simple.undirected.Simplify<StringValue, NullValue, NullValue>(false)
									.setParallelism(little_parallelism));
						}

						ji = graph
							.run(new org.apache.flink.graph.library.similarity.JaccardIndex<StringValue, NullValue, NullValue>()
								.setLittleParallelism(little_parallelism));
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

				boolean clipAndFlip = parameters.getBoolean("clip_and_flip", DEFAULT_CLIP_AND_FLIP);

				if (scale > 32) {
					ji = graph
						.run(new Simplify<LongValue, NullValue, NullValue>(clipAndFlip)
							.setParallelism(little_parallelism))
						.run(new org.apache.flink.graph.library.similarity.JaccardIndex<LongValue, NullValue, NullValue>()
							.setLittleParallelism(little_parallelism));
				} else {
					ji = graph
						.run(new TranslateGraphIds<LongValue, IntValue, NullValue, NullValue>(new LongValueToUnsignedIntValue())
							.setParallelism(little_parallelism))
						.run(new Simplify<IntValue, NullValue, NullValue>(clipAndFlip)
							.setParallelism(little_parallelism))
						.run(new org.apache.flink.graph.library.similarity.JaccardIndex<IntValue, NullValue, NullValue>()
							.setLittleParallelism(little_parallelism));
				}
				} break;

			default:
				throw new ProgramParametrizationException(getUsage("invalid input type"));
		}

		switch (parameters.get("output", "")) {
			case "print":
				System.out.println();
				for (Object e: ji.collect()) {
					Result result = (Result)e;
					System.out.println(result.toPrintableString());
				}
				break;

			case "hash":
				System.out.println();
				System.out.println(DataSetUtils.checksumHashCode(ji));
				break;

			case "csv":
				String filename = parameters.getRequired("output_filename");

				String lineDelimiter = StringEscapeUtils.unescapeJava(
					parameters.get("output_line_delimiter", CsvOutputFormat.DEFAULT_LINE_DELIMITER));

				String fieldDelimiter = StringEscapeUtils.unescapeJava(
					parameters.get("output_field_delimiter", CsvOutputFormat.DEFAULT_FIELD_DELIMITER));

				ji.writeAsCsv(filename, lineDelimiter, fieldDelimiter);

				env.execute("Jaccard Index");
				break;

			default:
				throw new ProgramParametrizationException(getUsage("invalid output type"));
		}

		JobExecutionResult result = env.getLastJobExecutionResult();

		NumberFormat nf = NumberFormat.getInstance();
		System.out.println();
		System.out.println("Execution runtime: " + nf.format(result.getNetRuntime()) + " ms");
	}
}
