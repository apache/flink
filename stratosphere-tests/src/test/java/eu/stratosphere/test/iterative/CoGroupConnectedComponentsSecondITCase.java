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

package eu.stratosphere.test.iterative;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.DeltaIteration;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.CoGroupFunction;
import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.functions.FunctionAnnotation.ConstantFieldsFirst;
import eu.stratosphere.api.java.functions.FunctionAnnotation.ConstantFieldsSecond;
import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.io.LocalCollectionOutputFormat;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.example.java.graph.ConnectedComponents.DuplicateValue;
import eu.stratosphere.example.java.graph.ConnectedComponents.NeighborWithComponentIDJoin;
import eu.stratosphere.test.testdata.ConnectedComponentsData;
import eu.stratosphere.test.util.JavaProgramTestBase;
import eu.stratosphere.util.Collector;

@SuppressWarnings("serial")
public class CoGroupConnectedComponentsSecondITCase extends JavaProgramTestBase {
	
	private static final long SEED = 0xBADC0FFEEBEEFL;
	
	private static final int NUM_VERTICES = 1000;
	
	private static final int NUM_EDGES = 10000;

	
	@Override
	protected void testProgram() throws Exception {
			
		// set up execution environment
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		// read vertex and edge data
		DataSet<Long> vertices = env.fromElements(ConnectedComponentsData.getEnumeratingVertices(NUM_VERTICES).split("\n"))
				.map(new VertexParser());
		
		DataSet<Tuple2<Long, Long>> edges = env.fromElements(ConnectedComponentsData.getRandomOddEvenEdges(NUM_EDGES, NUM_VERTICES, SEED).split("\n"))
				.flatMap(new EdgeParser());
		
		// assign the initial components (equal to the vertex id)
		DataSet<Tuple2<Long, Long>> verticesWithInitialId = vertices.map(new DuplicateValue<Long>());
				
		// open a delta iteration
		DeltaIteration<Tuple2<Long, Long>, Tuple2<Long, Long>> iteration =
				verticesWithInitialId.iterateDelta(verticesWithInitialId, 100, 0);
		
		// apply the step logic: join with the edges, select the minimum neighbor, update if the component of the candidate is smaller
		DataSet<Tuple2<Long, Long>> changes = iteration
				.getWorkset().join(edges).where(0).equalTo(0).with(new NeighborWithComponentIDJoin())
				.coGroup(iteration.getSolutionSet()).where(0).equalTo(0)
				.with(new MinIdAndUpdate());

		// close the delta iteration (delta and new workset are identical)
		DataSet<Tuple2<Long, Long>> result = iteration.closeWith(changes, changes);
		
		
		// emit result
		List<Tuple2<Long,Long>> resutTuples = new ArrayList<Tuple2<Long,Long>>();
		result.output(new LocalCollectionOutputFormat<Tuple2<Long,Long>>(resutTuples));
		
		env.execute();
	}
	
	// --------------------------------------------------------------------------------------------
	//  The test program
	// --------------------------------------------------------------------------------------------
	
	public static final class VertexParser extends MapFunction<String, Long> {

		@Override
		public Long map(String value) throws Exception {
			return Long.parseLong(value);
		}
	}
	
	public static final class EdgeParser extends FlatMapFunction<String, Tuple2<Long, Long>> {

		@Override
		public void flatMap(String value, Collector<Tuple2<Long, Long>> out) throws Exception {
			String[] parts = value.split(" ");
			long v1 = Long.parseLong(parts[0]);
			long v2 = Long.parseLong(parts[1]);
			
			out.collect(new Tuple2<Long, Long>(v1, v2));
			out.collect(new Tuple2<Long, Long>(v2, v1));
		}
	}

	@ConstantFieldsFirst("0")
	@ConstantFieldsSecond("0")
	public static final class MinIdAndUpdate extends CoGroupFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>> {
		
		@Override
		public void coGroup(Iterator<Tuple2<Long, Long>> candidates, Iterator<Tuple2<Long, Long>> current, Collector<Tuple2<Long, Long>> out) {
			if (!current.hasNext()) {
				throw new RuntimeException("Error: Id not encountered before.");
			}
			
			Tuple2<Long, Long> old = current.next();
			
			long minimumComponentID = Long.MAX_VALUE;

			while (candidates.hasNext()) {
				long candidateComponentID = candidates.next().f1;
				if (candidateComponentID < minimumComponentID) {
					minimumComponentID = candidateComponentID;
				}
			}
			
			if (minimumComponentID < old.f1) {
				old.f1 = minimumComponentID;
				out.collect(old);
			}
		}
	}
}
