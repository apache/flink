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
import eu.stratosphere.api.java.functions.CoGroupFunction;
import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.example.java.graph.util.ConnectedComponentsData;
import eu.stratosphere.util.Collector;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;


public class TransitiveClosureCoGroup implements ProgramDescription {

	public static class NewPathsFunction
		extends CoGroupFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>> {

		Set<Tuple2<Long,Long>> edgesBuf = new HashSet<Tuple2<Long,Long>>();

		@Override
		public void coGroup (Iterator<Tuple2<Long,Long>> paths,
								Iterator<Tuple2<Long,Long>> edges,
								Collector<Tuple2<Long,Long>> out) throws Exception {

			edgesBuf.clear();
			while (edges.hasNext()) {
				Tuple2<Long,Long> edge = edges.next();
				Tuple2<Long,Long> edgeCopy = new Tuple2<Long, Long>(edge.f0, edge.f1);
				edgesBuf.add(edgeCopy);
			}

			while (paths.hasNext()) {
				Tuple2<Long,Long> currPath = paths.next();
				long y1 = currPath.f0.longValue();
				long z = currPath.f1.longValue();
				out.collect (new Tuple2<Long,Long>(new Long(y1),new Long(z)));
				for (Tuple2<Long,Long> currEdge : edgesBuf) {
					long x = currEdge.f0.longValue();
					long y2 = currEdge.f1.longValue();
					assert (y1 == y2);
					Tuple2<Long,Long> outPath = new Tuple2<Long, Long>(new Long(x), new Long(z));
					out.collect(outPath);
				}
			}
			return;
		}
	}


	public static void main (String... args) throws Exception{

		if (!parseParameters(args)) {
			return;
		}

		// set up execution environment
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setDegreeOfParallelism(1);

		DataSet<Tuple2<Long, Long>> edges = getEdgeDataSet(env);

		IterativeDataSet<Tuple2<Long,Long>> paths = edges.iterate(maxIterations);

		DataSet<Tuple2<Long, Long>> nextPaths = paths
				.coGroup(edges)
				.where(0)
				.equalTo(1)
				.with(new NewPathsFunction())
				.groupBy(0, 1)
				.reduceGroup(new GroupReduceFunction<Tuple2<Long, Long>, Tuple2<Long, Long>>() {
					@Override
					public void reduce(Iterator<Tuple2<Long, Long>> values, Collector<Tuple2<Long, Long>> out) throws Exception {
						out.collect(values.next());
					}
				});

		DataSet<Tuple2<Long,Long>> transitiveClosure = paths.
				closeWith(nextPaths,
						nextPaths.coGroup(paths).where(0).equalTo(0).with(
								new CoGroupFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Object>() {
									Set<Tuple2<Long,Long>> pathBuf = new HashSet<Tuple2<Long, Long>>();
									@Override
									public void coGroup(Iterator<Tuple2<Long, Long>> newPaths, Iterator<Tuple2<Long, Long>> paths, Collector<Object> out) throws Exception {
										pathBuf.clear();
										while (paths.hasNext()) {
											Tuple2<Long,Long> path = paths.next();
											Tuple2<Long,Long> pathCopy = new Tuple2<Long,Long> (path.f0, path.f1);
											pathBuf.add (pathCopy);
										}
										while (newPaths.hasNext()) {
											Tuple2<Long,Long> newPath = newPaths.next();
											if (!pathBuf.contains(newPath)) {
												out.collect(newPath);
											}
										}
									}
								}
						)
				);

		//DataSet<Tuple2<Long,Long>> transitiveClosure = paths.closeWith(nextPaths);


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
		return "Parameters: <vertices-path> <edges-path> <result-path> <max-number-of-iterations>";
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
				System.err.println("Usage: TransitiveClosure <edges path> <result path>");
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

		if (fileOutput) {
			return env.readCsvFile(edgesPath).fieldDelimiter(' ').types(Long.class, Long.class);
		} else {
			return ConnectedComponentsData.getDefaultEdgeDataSet(env);
		}
	}
}
