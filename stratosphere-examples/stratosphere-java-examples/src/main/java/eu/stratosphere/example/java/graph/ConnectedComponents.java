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
import eu.stratosphere.api.java.DeltaIterativeDataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.aggregation.Aggregations;
import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.functions.JoinFunction;
import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.util.Collector;

/**
 * An implementation of the connected components algorithm, using a delta iteration.
 * Initially, the algorithm assigns each vertex its own ID. After the algorithm has completed, all vertices in the
 * same component will have the same id. In each step, a vertex 
 * <p>
 * A vertex whose component did not change needs not propagate its information in the next step. Because of that,
 * the algorithm is easily expressible via a delta iteration. We here model the solution set as the vertices with
 * their current component ids, and the workset as the changed vertices. Because we see all vertices initially as
 * changed, the initial workset and the initial solution set are identical. Also, the delta to the solution set
 * is consequently also the next workset.
 */
@SuppressWarnings("serial")
public class ConnectedComponents implements ProgramDescription {
	
	public static void main(String... args) throws Exception {
		if (args.length < 4) {
			System.err.println("Parameters: <vertices-path> <edges-path> <result-path> <max-number-of-iterations>");
			return;
		}
		
		final int maxIterations = Integer.parseInt(args[3]);
		
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		DataSet<Long> vertices = env.readCsvFile(args[0]).types(Long.class).map(new MapFunction<Tuple1<Long>, Long>() {
			public Long map(Tuple1<Long> value) { return value.f0; } });
		
		DataSet<Tuple2<Long, Long>> edges = env.readCsvFile(args[1]).fieldDelimiter(' ').types(Long.class, Long.class);
		
		DataSet<Tuple2<Long, Long>> result = doConnectedComponents(vertices, edges, maxIterations);
		
		result.writeAsCsv(args[2], "\n", " ");
		env.execute("Connected Components");
	}
	
	
	public static DataSet<Tuple2<Long, Long>> doConnectedComponents(DataSet<Long> vertices, DataSet<Tuple2<Long, Long>> edges, int maxIterations) {
		
		// assign the initial components (equal to the vertex id.
		DataSet<Tuple2<Long, Long>> verticesWithInitialId = vertices.map(new DuplicateValue<Long>());
		
		// open a delta iteration
		DeltaIterativeDataSet<Tuple2<Long, Long>, Tuple2<Long, Long>> iteration = 
				verticesWithInitialId.iterateDelta(verticesWithInitialId, maxIterations, 0);
		
		// apply the step logic: join with the edges, select the minimum neighbor, update the component of the candidate is smaller
		DataSet<Tuple2<Long, Long>> changes = iteration.join(edges).where(0).equalTo(0).with(new NeighborWithComponentIDJoin())
		                                               .groupBy(0).aggregate(Aggregations.MIN, 1)
		                                               .join(iteration.getSolutionSet()).where(0).equalTo(0)
		                                                .flatMap(new ComponentIdFilter());
		
		// close the delta iteration (delta and new workset are identical)
		return iteration.closeWith(changes, changes);
	}
	
	/**
	 * Function that turns a value into a 2-tuple where both fields are that value.
	 */
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
}
