/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.test.iterative;

import java.io.BufferedReader;
import java.io.Serializable;
import java.util.Collection;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.api.Job;
import eu.stratosphere.api.operators.FileDataSink;
import eu.stratosphere.api.operators.FileDataSource;
import eu.stratosphere.api.operators.WorksetIteration;
import eu.stratosphere.api.record.functions.MapFunction;
import eu.stratosphere.api.record.functions.JoinFunction;
import eu.stratosphere.api.record.io.CsvOutputFormat;
import eu.stratosphere.api.record.operators.MapOperator;
import eu.stratosphere.api.record.operators.JoinOperator;
import eu.stratosphere.api.record.operators.ReduceOperator;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.example.record.connectedcomponents.DuplicateLongInputFormat;
import eu.stratosphere.example.record.connectedcomponents.LongLongInputFormat;
import eu.stratosphere.example.record.connectedcomponents.WorksetConnectedComponents.MinimumComponentIDReduce;
import eu.stratosphere.example.record.connectedcomponents.WorksetConnectedComponents.NeighborWithComponentIDJoin;
import eu.stratosphere.pact.test.iterative.nephele.ConnectedComponentsNepheleITCase;
import eu.stratosphere.test.util.TestBase2;
import eu.stratosphere.types.PactLong;
import eu.stratosphere.types.PactRecord;
import eu.stratosphere.util.Collector;

@RunWith(Parameterized.class)
public class ConnectedComponentsWithDeferredUpdateITCase extends TestBase2 {
	
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
		verticesPath = createTempFile("vertices.txt", ConnectedComponentsNepheleITCase.getEnumeratingVertices(NUM_VERTICES));
		edgesPath = createTempFile("edges.txt", ConnectedComponentsNepheleITCase.getRandomOddEvenEdges(NUM_EDGES, NUM_VERTICES, SEED));
		resultPath = getTempFilePath("results");
	}
	
	@Override
	protected Job getPactPlan() {
		int dop = config.getInteger("ConnectedComponents#NumSubtasks", 1);
		int maxIterations = config.getInteger("ConnectedComponents#NumIterations", 1);
		boolean extraMapper = config.getBoolean("ConnectedComponents#ExtraMapper", false);
		
		return getPlan(dop, verticesPath, edgesPath, resultPath, maxIterations, extraMapper);
	}

	@Override
	protected void postSubmit() throws Exception {
		for (BufferedReader reader : getResultReader(resultPath)) {
			ConnectedComponentsNepheleITCase.checkOddEvenResult(reader);
		}
	}

	@Parameters
	public static Collection<Object[]> getConfigurations() {
		Configuration config1 = new Configuration();
		config1.setInteger("ConnectedComponents#NumSubtasks", 4);
		config1.setInteger("ConnectedComponents#NumIterations", 100);
		config1.setBoolean("ConnectedComponents#ExtraMapper", false);
		
		Configuration config2 = new Configuration();
		config2.setInteger("ConnectedComponents#NumSubtasks", 4);
		config2.setInteger("ConnectedComponents#NumIterations", 100);
		config2.setBoolean("ConnectedComponents#ExtraMapper", true);
		
		return toParameterList(config1, config2);
	}
	
	public static Job getPlan(int numSubTasks, String verticesInput, String edgeInput, String output, int maxIterations, boolean extraMap) {

		// data source for initial vertices
		FileDataSource initialVertices = new FileDataSource(new DuplicateLongInputFormat(), verticesInput, "Vertices");
		
		// the loop takes the vertices as the solution set and changed vertices as the workset
		// initially, all vertices are changed
		WorksetIteration iteration = new WorksetIteration(0, "Connected Components Iteration");
		iteration.setInitialSolutionSet(initialVertices);
		iteration.setInitialWorkset(initialVertices);
		iteration.setMaximumNumberOfIterations(maxIterations);
		
		// data source for the edges
		FileDataSource edges = new FileDataSource(new LongLongInputFormat(), edgeInput, "Edges");

		// join workset (changed vertices) with the edges to propagate changes to neighbors
		JoinOperator joinWithNeighbors = JoinOperator.builder(new NeighborWithComponentIDJoin(), PactLong.class, 0, 0)
				.input1(iteration.getWorkset())
				.input2(edges)
				.name("Join Candidate Id With Neighbor")
				.build();

		// find for each neighbor the smallest of all candidates
		ReduceOperator minCandidateId = ReduceOperator.builder(new MinimumComponentIDReduce(), PactLong.class, 0)
				.input(joinWithNeighbors)
				.name("Find Minimum Candidate Id")
				.build();
		
		// join candidates with the solution set and update if the candidate component-id is smaller
		JoinOperator updateComponentId = JoinOperator.builder(new UpdateComponentIdMatchNonPreserving(), PactLong.class, 0, 0)
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
		FileDataSink result = new FileDataSink(new CsvOutputFormat(), output, iteration, "Result");
		CsvOutputFormat.configureRecordFormat(result)
			.recordDelimiter('\n')
			.fieldDelimiter(' ')
			.field(PactLong.class, 0)
			.field(PactLong.class, 1);

		// return the PACT plan
		Job plan = new Job(result, "Workset Connected Components");
		plan.setDefaultParallelism(numSubTasks);
		return plan;
	}
	
	public static final class UpdateComponentIdMatchNonPreserving extends JoinFunction implements Serializable {
		private static final long serialVersionUID = 1L;

		@Override
		public void match(PactRecord newVertexWithComponent, PactRecord currentVertexWithComponent, Collector<PactRecord> out){
	
			long candidateComponentID = newVertexWithComponent.getField(1, PactLong.class).getValue();
			long currentComponentID = currentVertexWithComponent.getField(1, PactLong.class).getValue();
	
			if (candidateComponentID < currentComponentID) {
				out.collect(newVertexWithComponent);
			}
		}
	}
	
	public static final class IdentityMap extends MapFunction {

		@Override
		public void map(PactRecord record, Collector<PactRecord> out) throws Exception {
			out.collect(record);
		}
	}
}
