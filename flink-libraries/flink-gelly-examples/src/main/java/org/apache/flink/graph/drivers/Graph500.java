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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.client.program.ProgramParametrizationException;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.asm.simple.undirected.Simplify;
import org.apache.flink.graph.generator.RMatGraph;
import org.apache.flink.graph.generator.random.JDKRandomGeneratorFactory;
import org.apache.flink.graph.generator.random.RandomGenerableFactory;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;

import java.text.NumberFormat;

/**
 * Generate an RMat graph for Graph 500.
 *
 * Note that this does not yet implement permutation of vertex labels or edges.
 *
 * @see <a href="http://www.graph500.org/specifications">Graph 500</a>
 */
public class Graph500 {

	private static final int DEFAULT_SCALE = 10;

	private static final int DEFAULT_EDGE_FACTOR = 16;

	private static final boolean DEFAULT_SIMPLIFY = false;

	private static final boolean DEFAULT_CLIP_AND_FLIP = true;

	private static String getUsage(String message) {
		return new StrBuilder()
			.appendNewLine()
			.appendln("A Graph500 generator using the Recursive Matrix (RMat) graph generator.")
			.appendNewLine()
			.appendln(WordUtils.wrap("The graph matrix contains 2^scale vertices although not every vertex will" +
				" be represented in an edge. The number of edges is edge_factor * 2^scale edges" +
				" although some edges may be duplicates.", 80))
			.appendNewLine()
			.appendln("Note: this does not yet implement permutation of vertex labels or edges.")
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

		// Generate RMat graph
		int scale = parameters.getInt("scale", DEFAULT_SCALE);
		int edgeFactor = parameters.getInt("edge_factor", DEFAULT_EDGE_FACTOR);

		RandomGenerableFactory<JDKRandomGenerator> rnd = new JDKRandomGeneratorFactory();

		long vertexCount = 1L << scale;
		long edgeCount = vertexCount * edgeFactor;

		boolean simplify = parameters.getBoolean("simplify", DEFAULT_SIMPLIFY);
		boolean clipAndFlip = parameters.getBoolean("clip_and_flip", DEFAULT_CLIP_AND_FLIP);

		Graph<LongValue, NullValue, NullValue> graph = new RMatGraph<>(env, rnd, vertexCount, edgeCount)
			.generate();

		if (simplify) {
			graph = graph.run(new Simplify<LongValue, NullValue, NullValue>(clipAndFlip));
		}

		DataSet<Tuple2<LongValue,LongValue>> edges = graph
			.getEdges()
			.project(0, 1);

		// Print, hash, or write RMat graph to disk
		switch (parameters.get("output", "")) {
		case "print":
			edges.print();
			break;

		case "hash":
			System.out.println(DataSetUtils.checksumHashCode(edges));
			break;

		case "csv":
			String filename = parameters.get("filename");

			String lineDelimiter = StringEscapeUtils.unescapeJava(
				parameters.get("output_line_delimiter", CsvOutputFormat.DEFAULT_LINE_DELIMITER));

			String fieldDelimiter = StringEscapeUtils.unescapeJava(
				parameters.get("output_field_delimiter", CsvOutputFormat.DEFAULT_FIELD_DELIMITER));

			edges.writeAsCsv(filename, lineDelimiter, fieldDelimiter);

			env.execute("Graph500");
			break;
		default:
			throw new ProgramParametrizationException(getUsage("invalid output type"));
		}

		JobExecutionResult result = env.getLastJobExecutionResult();

		NumberFormat nf = NumberFormat.getInstance();
		System.out.println("Execution runtime: " + nf.format(result.getNetRuntime()) + " ms");
	}
}
