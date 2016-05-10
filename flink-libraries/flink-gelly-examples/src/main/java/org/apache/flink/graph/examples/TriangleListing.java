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
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.asm.translate.LongValueToIntValue;
import org.apache.flink.graph.asm.translate.TranslateGraphIds;
import org.apache.flink.graph.generator.RMatGraph;
import org.apache.flink.graph.generator.random.JDKRandomGeneratorFactory;
import org.apache.flink.graph.generator.random.RandomGenerableFactory;
import org.apache.flink.graph.library.TriangleEnumerator;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;

import java.text.NumberFormat;

public class TriangleListing {

	public static final int DEFAULT_SCALE = 10;

	public static final int DEFAULT_EDGE_FACTOR = 16;

	public static final boolean DEFAULT_CLIP_AND_FLIP = true;

	public static void main(String[] args) throws Exception {
		// Set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		ParameterTool parameters = ParameterTool.fromArgs(args);

		if (parameters.has("listing")) {
			env.getConfig().enableObjectReuse();
		}

		// Generate RMat graph
		int scale = parameters.getInt("scale", DEFAULT_SCALE);
		int edgeFactor = parameters.getInt("edge_factor", DEFAULT_EDGE_FACTOR);

		RandomGenerableFactory<JDKRandomGenerator> rnd = new JDKRandomGeneratorFactory();

		long vertexCount = 1 << scale;
		long edgeCount = vertexCount * edgeFactor;

		boolean clipAndFlip = parameters.getBoolean("clip_and_flip", DEFAULT_CLIP_AND_FLIP);

		Graph<LongValue, NullValue, NullValue> graph = new RMatGraph<>(env, rnd, vertexCount, edgeCount)
			.setSimpleGraph(true, clipAndFlip)
			.generate();

		DataSet tl;

		if (scale > 32) {
			if (parameters.has("enumerator")) {
				tl = graph
					.run(new TriangleEnumerator<LongValue,NullValue,NullValue>());
			} else {
				tl = graph
					.run(new org.apache.flink.graph.library.asm.TriangleListing<LongValue, NullValue, NullValue>());
			}
		} else {
			if (parameters.has("enumerator")) {
				tl = graph
					.run(new TranslateGraphIds<LongValue, IntValue, NullValue, NullValue>(new LongValueToIntValue()))
					.run(new TriangleEnumerator<IntValue, NullValue, NullValue>());
			} else {
				tl = graph
					.run(new TranslateGraphIds<LongValue, IntValue, NullValue, NullValue>(new LongValueToIntValue()))
					.run(new org.apache.flink.graph.library.asm.TriangleListing<IntValue, NullValue, NullValue>());
			}
		}

		// Print, hash, or write RMat graph to disk
		switch (parameters.get("output", "")) {
		case "print":
			tl.print();
			break;

		case "hash":
			System.out.println(DataSetUtils.checksumHashCode(tl));
			break;

		case "csv":
			String filename = parameters.get("filename");

			String row_delimiter = parameters.get("row_delimiter", CsvOutputFormat.DEFAULT_LINE_DELIMITER);
			String field_delimiter = parameters.get("field_delimiter", CsvOutputFormat.DEFAULT_FIELD_DELIMITER);

			tl.writeAsCsv(filename, row_delimiter, field_delimiter);

			env.execute();
			break;
		default:
			System.out.println(" Generates a listing of distinct triangles from the input graph.");
			System.out.println("");
			System.out.println("usage:");
			System.out.println("  TriangleListing [--scale SCALE] [--edge_factor EDGE_FACTOR] --output print");
			System.out.println("  TriangleListing [--scale SCALE] [--edge_factor EDGE_FACTOR] --output hash");
			System.out.println("  TriangleListing [--scale SCALE] [--edge_factor EDGE_FACTOR] --output csv" +
					" --filename FILENAME [--row_delimiter ROW_DELIMITER] [--field_delimiter FIELD_DELIMITER]");

			return;
		}

		JobExecutionResult result = env.getLastJobExecutionResult();

		NumberFormat nf = NumberFormat.getInstance();
		System.out.println("Execution runtime: " + nf.format(result.getNetRuntime()) + " ms");
	}
}
