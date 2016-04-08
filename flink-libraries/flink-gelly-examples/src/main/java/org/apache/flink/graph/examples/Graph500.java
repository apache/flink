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

import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.graph.generator.RMatGraph;
import org.apache.flink.graph.generator.random.JDKRandomGeneratorFactory;
import org.apache.flink.graph.generator.random.RandomGenerableFactory;
import org.apache.flink.types.LongValue;

import java.text.NumberFormat;

/**
 * Generate an RMat graph for Graph 500.
 *
 * Note that this does not yet implement permutation of vertex labels or edges.
 *
 * @see <a href="http://www.graph500.org/specifications">Graph 500</a>
 */
public class Graph500 {

	public static final int DEFAULT_SCALE = 10;

	public static final int DEFAULT_EDGE_FACTOR = 16;

	public static final boolean DEFAULT_SIMPLIFY = false;

	public static final boolean DEFAULT_CLIP_AND_FLIP = true;

	public static void main(String[] args) throws Exception {
		// Set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().enableObjectReuse();

		ParameterTool parameters = ParameterTool.fromArgs(args);

		// Generate RMat graph
		int scale = parameters.getInt("scale", DEFAULT_SCALE);
		int edgeFactor = parameters.getInt("edge_factor", DEFAULT_EDGE_FACTOR);

		RandomGenerableFactory<JDKRandomGenerator> rnd = new JDKRandomGeneratorFactory();

		long vertexCount = 1 << scale;
		long edgeCount = vertexCount * edgeFactor;

		boolean simplify = parameters.getBoolean("simplify", DEFAULT_SIMPLIFY);
		boolean clipAndFlip = parameters.getBoolean("clip_and_flip", DEFAULT_CLIP_AND_FLIP);

		DataSet<Tuple2<LongValue,LongValue>> edges = new RMatGraph<>(env, rnd, vertexCount, edgeCount)
			.setSimpleGraph(simplify, clipAndFlip)
			.generate()
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

			String row_delimiter = parameters.get("row_delimiter", CsvOutputFormat.DEFAULT_LINE_DELIMITER);
			String field_delimiter = parameters.get("field_delimiter", CsvOutputFormat.DEFAULT_FIELD_DELIMITER);

			edges.writeAsCsv(filename, row_delimiter, field_delimiter);

			env.execute();
			break;
		default:
			System.out.println("A Graph500 generator using the Recursive Matrix (RMat) graph generator.");
			System.out.println();
			System.out.println("The graph matrix contains 2^scale vertices although not every vertex will");
			System.out.println("be represented in an edge. The number of edges is edge_factor * 2^scale edges");
			System.out.println("although some edges may be duplicates.");
			System.out.println();
			System.out.println("Note: this does not yet implement permutation of vertex labels or edges.");
			System.out.println();
			System.out.println("usage:");
			System.out.println("  Graph500 [--scale SCALE] [--edge_factor EDGE_FACTOR] --output print");
			System.out.println("  Graph500 [--scale SCALE] [--edge_factor EDGE_FACTOR] --output hash");
			System.out.println("  Graph500 [--scale SCALE] [--edge_factor EDGE_FACTOR] --output csv" +
					" --filename FILENAME [--row_delimiter ROW_DELIMITER] [--field_delimiter FIELD_DELIMITER]");

			return;
		}

		JobExecutionResult result = env.getLastJobExecutionResult();

		NumberFormat nf = NumberFormat.getInstance();
		System.out.println("Execution runtime: " + nf.format(result.getNetRuntime()) + " ms");
	}
}
