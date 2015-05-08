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
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.example.utils.AffinityPropogationData;
import org.apache.flink.graph.library.AffinityPropogation;
import org.apache.flink.graph.utils.Tuple3ToEdgeMap;

public class AffinityPropogationExample implements ProgramDescription{

	public static String similarGraphInputPath = null;
	public static String outputPath = null;
	public static int maxIterations = 100;
	public static double lambda = 0.5;
	public static boolean fileOutput = false;
	
	@Override
	public String getDescription() {
		return "This is an example of Affinity Propogation";
	}
	
	private static boolean parseParameters(String[] programArguments) {
		if(programArguments.length > 0) {
			// parse input arguments
			fileOutput = true;
			if(programArguments.length == 4) {
				similarGraphInputPath = programArguments[0];
				outputPath = programArguments[1];
				maxIterations = Integer.parseInt(programArguments[2]);
				lambda = Double.parseDouble(programArguments[3]);
			} else {
				System.err.println("Usage: AffinityPropogation <similarity graph path> <result path> <num of iterations> <lambda>");
				return false;
			}
		} else {
			System.out.println("Executing Affinity propogation example with default parameters and built-in default data.");
			System.out.println("Provide parameters to read input data from files.");
			System.out.println("See the documentation for the correct format of input files.");
			System.out.println("Usage:  AffinityPropogation <similarity graph path> <vertex preference path> <result path> <num of iterations> <lambda>");
		}
		return true;
	}


	public static void main(String[] args) throws Exception{
		if(!parseParameters(args)) {
			return;
		}
		//set up environment
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		//Read similarity graph
		DataSet<Edge<Long, Double>> edgeSet = getEdgeDataSet(env);
		Graph<Long, Long, Double> graph = Graph.fromDataSet(edgeSet, new MapFunction<Long, Long>(){
			@Override
			public Long map(Long value) throws Exception {
				return value;
			}
		},env);		
		//Run affinity propagation algorithm
		Graph<Long, Long, Double> result = graph.run(new AffinityPropogation(maxIterations, lambda));
		
		//emit result
		if (fileOutput){
			result.getVertices().writeAsCsv(outputPath,"\n", " ");
		}else{
			result.getVertices().print();
		}
		env.execute("Affinity Propogation Example");
	}
	@SuppressWarnings("serial")
	public static DataSet<Edge<Long, Double>> getEdgeDataSet(ExecutionEnvironment env){
		if (fileOutput){
			return env.readCsvFile(similarGraphInputPath)
					.fieldDelimiter(' ').lineDelimiter("\n")
					.types(Long.class, Long.class, Double.class)
					.map(new Tuple3ToEdgeMap<Long,  Double>());
		}else{
			return AffinityPropogationData.getDefautEdgeDataSet(env);
		}
	}
}
