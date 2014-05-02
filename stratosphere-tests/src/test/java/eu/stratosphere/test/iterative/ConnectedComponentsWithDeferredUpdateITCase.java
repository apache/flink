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

import java.io.BufferedReader;
import java.io.Serializable;
import java.util.Collection;

import eu.stratosphere.test.util.RecordAPITestBase;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.operators.FileDataSink;
import eu.stratosphere.api.common.operators.FileDataSource;
import eu.stratosphere.api.common.operators.DeltaIteration;
import eu.stratosphere.api.java.record.functions.JoinFunction;
import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.api.java.record.io.CsvInputFormat;
import eu.stratosphere.api.java.record.io.CsvOutputFormat;
import eu.stratosphere.api.java.record.operators.JoinOperator;
import eu.stratosphere.api.java.record.operators.MapOperator;
import eu.stratosphere.api.java.record.operators.ReduceOperator;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.test.testPrograms.WorksetConnectedComponents.DuplicateLongMap;
import eu.stratosphere.test.testPrograms.WorksetConnectedComponents.MinimumComponentIDReduce;
import eu.stratosphere.test.testPrograms.WorksetConnectedComponents.NeighborWithComponentIDJoin;
import eu.stratosphere.test.testdata.ConnectedComponentsData;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;

@RunWith(Parameterized.class)
public class ConnectedComponentsWithDeferredUpdateITCase extends RecordAPITestBase {
	
	private static final long SEED = 0xBADC0FFEEBEEFL;
	
	private static final int NUM_VERTICES = 1000;
	
	private static final int NUM_EDGES = 10000;

	
	protected String verticesPath;
	protected String edgesPath;
	protected String resultPath;
	
	
	public ConnectedComponentsWithDeferredUpdateITCase(Configuration config) {
		super(config);
	}
	
	@Override
	protected void preSubmit() throws Exception {
		verticesPath = createTempFile("vertices.txt", ConnectedComponentsData.getEnumeratingVertices(NUM_VERTICES));
		edgesPath = createTempFile("edges.txt", ConnectedComponentsData.getRandomOddEvenEdges(NUM_EDGES, NUM_VERTICES, SEED));
		resultPath = getTempFilePath("results");
	}
	
	@Override
	protected Plan getTestJob() {
		boolean extraMapper = config.getBoolean("ExtraMapper", false);
		return getPlan(4, verticesPath, edgesPath, resultPath, 100, extraMapper);
	}

	@Override
	protected void postSubmit() throws Exception {
		for (BufferedReader reader : getResultReader(resultPath)) {
			ConnectedComponentsData.checkOddEvenResult(reader);
		}
	}

	@Parameters
	public static Collection<Object[]> getConfigurations() {
		Configuration config1 = new Configuration();
		config1.setBoolean("ExtraMapper", false);
		
		Configuration config2 = new Configuration();
		config2.setBoolean("ExtraMapper", true);
		
		return toParameterList(config1, config2);
	}
	
	@SuppressWarnings("unchecked")
	public static Plan getPlan(int numSubTasks, String verticesInput, String edgeInput, String output, int maxIterations, boolean extraMap) {

		// data source for initial vertices
		FileDataSource initialVertices = new FileDataSource(new CsvInputFormat(' ', LongValue.class), verticesInput, "Vertices");
		
		MapOperator verticesWithId = MapOperator.builder(DuplicateLongMap.class).input(initialVertices).name("Assign Vertex Ids").build();
		
		// the loop takes the vertices as the solution set and changed vertices as the workset
		// initially, all vertices are changed
		DeltaIteration iteration = new DeltaIteration(0, "Connected Components Iteration");
		iteration.setInitialSolutionSet(verticesWithId);
		iteration.setInitialWorkset(verticesWithId);
		iteration.setMaximumNumberOfIterations(maxIterations);
		
		// data source for the edges
		FileDataSource edges = new FileDataSource(new CsvInputFormat(' ', LongValue.class, LongValue.class), edgeInput, "Edges");

		// join workset (changed vertices) with the edges to propagate changes to neighbors
		JoinOperator joinWithNeighbors = JoinOperator.builder(new NeighborWithComponentIDJoin(), LongValue.class, 0, 0)
				.input1(iteration.getWorkset())
				.input2(edges)
				.name("Join Candidate Id With Neighbor")
				.build();

		// find for each neighbor the smallest of all candidates
		ReduceOperator minCandidateId = ReduceOperator.builder(new MinimumComponentIDReduce(), LongValue.class, 0)
				.input(joinWithNeighbors)
				.name("Find Minimum Candidate Id")
				.build();
		
		// join candidates with the solution set and update if the candidate component-id is smaller
		JoinOperator updateComponentId = JoinOperator.builder(new UpdateComponentIdMatchNonPreserving(), LongValue.class, 0, 0)
				.input1(minCandidateId)
				.input2(iteration.getSolutionSet())
				.name("Update Component Id")
				.build();
		
		if (extraMap) {
			MapOperator mapper = MapOperator.builder(IdentityMap.class).input(updateComponentId).name("idmap").build();
			iteration.setSolutionSetDelta(mapper);
		} else {
			iteration.setSolutionSetDelta(updateComponentId);
		}
		
		iteration.setNextWorkset(updateComponentId);

		// sink is the iteration result
		FileDataSink result = new FileDataSink(new CsvOutputFormat("\n", " ", LongValue.class, LongValue.class), output, iteration, "Result");

		// return the PACT plan
		Plan plan = new Plan(result, "Workset Connected Components");
		plan.setDefaultParallelism(numSubTasks);
		return plan;
	}
	
	public static final class UpdateComponentIdMatchNonPreserving extends JoinFunction implements Serializable {
		private static final long serialVersionUID = 1L;

		@Override
		public void join(Record newVertexWithComponent, Record currentVertexWithComponent, Collector<Record> out){
	
			long candidateComponentID = newVertexWithComponent.getField(1, LongValue.class).getValue();
			long currentComponentID = currentVertexWithComponent.getField(1, LongValue.class).getValue();
	
			if (candidateComponentID < currentComponentID) {
				out.collect(newVertexWithComponent);
			}
		}
	}
	
	public static final class IdentityMap extends MapFunction {
		private static final long serialVersionUID = 1L;

		@Override
		public void map(Record record, Collector<Record> out) throws Exception {
			out.collect(record);
		}
	}
}
