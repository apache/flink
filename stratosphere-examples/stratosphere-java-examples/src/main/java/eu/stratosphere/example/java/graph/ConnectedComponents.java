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
import eu.stratosphere.api.java.DeltaIteration;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.aggregation.Aggregations;
import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.functions.FunctionAnnotation.ConstantFields;
import eu.stratosphere.api.java.functions.FunctionAnnotation.ConstantFieldsFirst;
import eu.stratosphere.api.java.functions.FunctionAnnotation.ConstantFieldsSecond;
import eu.stratosphere.api.java.functions.JoinFunction;
import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.example.java.graph.util.ConnectedComponentsData;
import eu.stratosphere.util.Collector;

/**
 * An implementation of the connected components algorithm, using a delta iteration.
 * Initially, the algorithm assigns each vertex its own ID. After the algorithm has completed, all vertices in the
 * same component will have the same id. In each step, a vertex picks the minimum of its own ID and its
 * neighbors' IDs, as its new ID.
 * <p>
 * A vertex whose component did not change needs not propagate its information in the next step. Because of that,
 * the algorithm is easily expressible via a delta iteration. We here model the solution set as the vertices with
 * their current component ids, and the workset as the changed vertices. Because we see all vertices initially as
 * changed, the initial workset and the initial solution set are identical. Also, the delta to the solution set
 * is consequently also the next workset.
 * 
 * <p>
 * Input files are plain text files and must be formatted as follows:
 * <ul>
 * <li>Vertices represented as IDs and separated by new-line characters.<br> 
 * For example <code>"1\n2\n12\n42\n63\n"</code> gives five vertices (1), (2), (12), (42), and (63). 
 * <li>Edges are represented as pairs for vertex IDs which are separated by space 
 * characters. Edges are separated by new-line characters.<br>
 * For example <code>"1 2\n2 12\n1 12\n42 63\n"</code> gives four (undirected) edges (1)-(2), (2)-(12), (1)-(12), and (42)-(63).
 * </ul>
 * 
 * <p>
 * This example shows how to use:
 * <ul>
 * <li>Delta Iterations
 * <li>Generic-typed Functions 
 * </ul>
 */
@SuppressWarnings("serial")
public class ConnectedComponents implements ProgramDescription {
	
	// *************************************************************************
	//     PROGRAM
	// *************************************************************************
	
	public static void main(String... args) throws Exception {
		
		parseParameters(args);
		
		// set up execution environment
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		// read vertex and edge data
		DataSet<Long> vertices = getVertexDataSet(env);
		DataSet<Tuple2<Long, Long>> edges = getEdgeDataSet(env);
		
		// assign the initial components (equal to the vertex id.
		DataSet<Tuple2<Long, Long>> verticesWithInitialId = vertices.map(new DuplicateValue<Long>());
		
		// open a delta iteration
		DeltaIteration<Tuple2<Long, Long>, Tuple2<Long, Long>> iteration =
				verticesWithInitialId.iterateDelta(verticesWithInitialId, maxIterations, 0);
		
		// apply the step logic: join with the edges, select the minimum neighbor, update if the component of the candidate is smaller
		DataSet<Tuple2<Long, Long>> changes = iteration.getWorkset().join(edges).where(0).equalTo(0).with(new NeighborWithComponentIDJoin())
				.groupBy(0).aggregate(Aggregations.MIN, 1)
				.join(iteration.getSolutionSet()).where(0).equalTo(0)
				.flatMap(new ComponentIdFilter());

		// close the delta iteration (delta and new workset are identical)
		DataSet<Tuple2<Long, Long>> result = iteration.closeWith(changes, changes);
		
		// emit result
		if(fileOutput) {
			result.writeAsCsv(outputPath, "\n", " ");
		} else {
			result.print();
		}
				
		// execute program
		env.execute("Connected Components Example");
				
	}
	
	// *************************************************************************
	//     USER FUNCTIONS
	// *************************************************************************
	
	/**
	 * Function that turns a value into a 2-tuple where both fields are that value.
	 */
	@ConstantFields("0 -> 0,1") 
	public static final class DuplicateValue<T> extends MapFunction<T, Tuple2<T, T>> {
		
		@Override
		public Tuple2<T, T> map(T value) {
			return new Tuple2<T, T>(value, value);
		}
	}
	
	/**
	 * UDF that joins a (Vertex-ID, Component-ID) pair that represents the current component that
	 * a vertex is associated with, with a (Source-Vertex-ID, Target-VertexID) edge. The function
	 * produces a (Target-vertex-ID, Component-ID) pair.
	 */
	@ConstantFieldsFirst("1 -> 0")
	@ConstantFieldsSecond("1 -> 1")
	public static final class NeighborWithComponentIDJoin extends JoinFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>> {

		@Override
		public Tuple2<Long, Long> join(Tuple2<Long, Long> vertexWithComponent, Tuple2<Long, Long> edge) {
			return new Tuple2<Long, Long>(edge.f1, vertexWithComponent.f1);
		}
	}
	
	/**
	 * The input is nested tuples ( (vertex-id, candidate-component) , (vertex-id, current-component) )
	 */
	public static final class ComponentIdFilter extends FlatMapFunction<Tuple2<Tuple2<Long, Long>, Tuple2<Long, Long>>, Tuple2<Long, Long>> {

		@Override
		public void flatMap(Tuple2<Tuple2<Long, Long>, Tuple2<Long, Long>> value, Collector<Tuple2<Long, Long>> out) {
			if (value.f0.f1 < value.f1.f1) {
				out.collect(value.f0);
			}
		}
	}

	@Override
	public String getDescription() {
		return "Parameters: <vertices-path> <edges-path> <result-path> <max-number-of-iterations>";
	}
	
	// *************************************************************************
	//     UTIL METHODS
	// *************************************************************************
	
	private static boolean fileOutput = false;
	private static String verticesPath = null;
	private static String edgesPath = null;
	private static String outputPath = null;
	private static int maxIterations = 10;
	
	private static void parseParameters(String[] programArguments) {
		
		if(programArguments.length > 0) {
			// parse input arguments
			fileOutput = true;
			if(programArguments.length == 4) {
				verticesPath = programArguments[0];
				edgesPath = programArguments[1];
				outputPath = programArguments[2];
				maxIterations = Integer.parseInt(programArguments[3]);
			} else {
				System.err.println("Usage: ConnectedComponents <vertices path> <edges path> <result path> <max number of iterations>");
				System.exit(1);
			}
		} else {
			System.out.println("Executing Connected Components example with default parameters and built-in default data.");
			System.out.println("  Provide parameters to read input data from files.");
			System.out.println("  Usage: ConnectedComponents <vertices path> <edges path> <result path> <max number of iterations>");
		}
	}
	
	private static DataSet<Long> getVertexDataSet(ExecutionEnvironment env) {
		
		if(fileOutput) {
			return env.readCsvFile(verticesPath).types(Long.class)
						.map(
								new MapFunction<Tuple1<Long>, Long>() {
									public Long map(Tuple1<Long> value) { return value.f0; }
								});
		} else {
			return ConnectedComponentsData.getDefaultVertexDataSet(env);
		}
	}
	
	private static DataSet<Tuple2<Long, Long>> getEdgeDataSet(ExecutionEnvironment env) {
		
		if(fileOutput) {
			return env.readCsvFile(edgesPath).fieldDelimiter(' ').types(Long.class, Long.class); 
		} else {
			return ConnectedComponentsData.getDefaultEdgeDataSet(env);
		}
	}
	
	
}
