/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.example.java.graph;

import eu.stratosphere.api.common.ProgramDescription;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.IterativeDataSet;
import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.api.java.functions.JoinFunction;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.example.java.graph.util.ConnectedComponentsData;
import eu.stratosphere.util.Collector;

import java.util.Iterator;


public class TransitiveClosureNaive implements ProgramDescription {


	public static void main (String... args) throws Exception{

		if (!parseParameters(args)) {
			return;
		}

		// set up execution environment
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple2<Long, Long>> edges = getEdgeDataSet(env);

		IterativeDataSet<Tuple2<Long,Long>> paths = edges.iterate(maxIterations);

		DataSet<Tuple2<Long,Long>> nextPaths = paths
				.join(edges)
				.where(1)
				.equalTo(0)
				.with(new JoinFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>>() {
					@Override
					/**
						left: Path (z,x) - x is reachable by z
						right: Edge (x,y) - edge x-->y exists
						out: Path (z,y) - y is reachable by z
					 */
					public Tuple2<Long, Long> join(Tuple2<Long, Long> left, Tuple2<Long, Long> right) throws Exception {
						return new Tuple2<Long, Long>(
								new Long(left.f0),
								new Long(right.f1));
					}
				})
				.union(paths)
				.groupBy(0, 1)
				.reduceGroup(new GroupReduceFunction<Tuple2<Long, Long>, Tuple2<Long, Long>>() {
					@Override
					public void reduce(Iterator<Tuple2<Long, Long>> values, Collector<Tuple2<Long, Long>> out) throws Exception {
						out.collect(values.next());
					}
				});

		DataSet<Tuple2<Long, Long>> transitiveClosure = paths.closeWith(nextPaths);


		// emit result
		if (fileOutput) {
			transitiveClosure.writeAsCsv(outputPath, "\n", " ");
		} else {
			transitiveClosure.print();
		}

		// execute program
		env.execute("Transitive Closure Example");

	}

	@Override
	public String getDescription() {
		return "Parameters: <edges-path> <result-path> <max-number-of-iterations>";
	}

	// *************************************************************************
	//     UTIL METHODS
	// *************************************************************************

	private static boolean fileOutput = false;
	private static String edgesPath = null;
	private static String outputPath = null;
	private static int maxIterations = 10;

	private static boolean parseParameters(String[] programArguments) {

		if (programArguments.length > 0) {
			// parse input arguments
			fileOutput = true;
			if (programArguments.length == 3) {
				edgesPath = programArguments[0];
				outputPath = programArguments[1];
				maxIterations = Integer.parseInt(programArguments[2]);
			} else {
				System.err.println("Usage: TransitiveClosure <edges path> <result path> <max number of iterations>");
				return false;
			}
		} else {
			System.out.println("Executing TransitiveClosure example with default parameters and built-in default data.");
			System.out.println("  Provide parameters to read input data from files.");
			System.out.println("  See the documentation for the correct format of input files.");
			System.out.println("  Usage: TransitiveClosure <edges path> <result path> <max number of iterations>");
		}
		return true;
	}


	private static DataSet<Tuple2<Long, Long>> getEdgeDataSet(ExecutionEnvironment env) {

		if(fileOutput) {
			return env.readCsvFile(edgesPath).fieldDelimiter(' ').types(Long.class, Long.class);
		} else {
			return ConnectedComponentsData.getDefaultEdgeDataSet(env);
		}
	}

}
