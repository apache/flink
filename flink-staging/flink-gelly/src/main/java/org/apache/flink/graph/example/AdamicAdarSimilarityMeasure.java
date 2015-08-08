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

package org.apache.flink.graph.example;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.example.utils.AdamicAdarSimilarityMeasureData;
import org.apache.flink.graph.library.AdamicAdarSimilarityMeasureAlgorithm;

/**
 * Given a directed, unweighted graph, return a weighted graph where the edge values are equal
 * to the Adamic Adar similarity coefficient which is given as
 * Summation of weights of common neighbors of the source and destination vertex
 * The Adamic Adar weights are given as 1/log(nK) nK is the degree  or the vertex
 *
 * @see <a href="http://social.cs.uiuc.edu/class/cs591kgk/friendsadamic.pdf">Friends and neighbors on the Web</a>
 * <p/>
 * <p>
 * Input files are plain text files and must be formatted as follows:
 * <br>
 * Edges are represented by pairs of srcVertexId, trgVertexId separated by tabs.
 * Edges themselves are separated by newlines.
 * For example: <code>1	2\n1	3\n</code> defines two edges 1-2 and 1-3.
 * </p>
 * <p/>
 * Usage <code> AdamicAdarSimilarityMeasure &lt;edge path&gt; &lt;result path&gt;</code><br>
 * If no parameters are provided, the program is run with default data from
 * {@link org.apache.flink.graph.example.utils.AdamicAdarSimilarityMeasureData}
 */
@SuppressWarnings("serial")
public class AdamicAdarSimilarityMeasure implements ProgramDescription {

	public static void main(String[] args) throws Exception {

		if(!parseParameters(args)) {
			return;
		}
		
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Edge<Long, Double>> edges = AdamicAdarSimilarityMeasure.getEdgesDataSet(env);
		
		Graph<Long, Long, Double> graph = Graph.fromDataSet(edges,
				new MapFunction<Long, Long>() {
			public Long map(Long label) {
				return label;
			}
		}, env);
		
		DataSet<Edge<Long, Double>> adamicAdarEdges = graph.run(new AdamicAdarSimilarityMeasureAlgorithm()).getEdges();
		
		if(fileOutput) {
			adamicAdarEdges.writeAsCsv(outputPath);
			// since file sinks are lazy, we trigger the execution explicitly
			env.execute("Executing Adamic Adar Similarity Measure Example");
			} else {
			adamicAdarEdges.print();
		}
	}
	
	@Override
	public String getDescription() {
		return "Vertex Adamic Adar Similarity Measure";
	}
	
	// *************************************************************************
	// UTIL METHODS
	// *************************************************************************

	private static boolean fileOutput = false;
	private static String edgeInputPath = null;
	private static String outputPath = null;

	private static boolean parseParameters(String [] args) {
		if(args.length > 0) {
			if(args.length != 2) {
				System.err.println("Usage AdamicAdarSimilarityMeasure <edge path> <output path>");
				return false;
			}

			fileOutput = true;
			edgeInputPath = args[0];
			outputPath = args[1];
		} else {
			System.out.println("Executing AdamicAdarSimilarityMeasure example with default parameters and built-in default data.");
			System.out.println("Provide parameters to read input data from files.");
			System.out.println("Usage AdamicAdarSimilarityMeasure <edge path> <output path>");
		}

		return true;
	}

	private static DataSet<Edge<Long, Double>> getEdgesDataSet(ExecutionEnvironment env) {

		if(fileOutput) {
			return env.readCsvFile(edgeInputPath)
					.ignoreComments("#")
					.fieldDelimiter("\t")
					.lineDelimiter("\n")
					.types(Long.class, Long.class)
					.map(new MapFunction<Tuple2<Long, Long>, Edge<Long, Double>>() {
						@Override
						public Edge<Long, Double> map(Tuple2<Long, Long> tuple2) throws Exception {
							return new Edge<Long, Double>(tuple2.f0, tuple2.f1, new Double(0));
						}
					});
		} else {
			return AdamicAdarSimilarityMeasureData.getDefaultEdgeDataSet(env);
		}
	}
}

