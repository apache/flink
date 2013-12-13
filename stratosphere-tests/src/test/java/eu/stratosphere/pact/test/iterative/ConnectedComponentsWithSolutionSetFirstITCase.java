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

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFieldsSecondExcept;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.example.connectedcomponents.DuplicateLongInputFormat;
import eu.stratosphere.pact.example.connectedcomponents.LongLongInputFormat;
import eu.stratosphere.pact.example.connectedcomponents.WorksetConnectedComponents.MinimumComponentIDReduce;
import eu.stratosphere.pact.example.connectedcomponents.WorksetConnectedComponents.NeighborWithComponentIDJoin;
import eu.stratosphere.pact.generic.contract.WorksetIteration;
import eu.stratosphere.pact.test.iterative.nephele.ConnectedComponentsNepheleITCase;
import eu.stratosphere.pact.test.util.TestBase2;

@RunWith(Parameterized.class)
public class ConnectedComponentsWithSolutionSetFirstITCase extends TestBase2 {
	
	private static final long SEED = 0xBADC0FFEEBEEFL;
	
	private static final int NUM_VERTICES = 1000;
	
	private static final int NUM_EDGES = 10000;

	
	protected String verticesPath;
	protected String edgesPath;
	protected String resultPath;
	
	
	public ConnectedComponentsWithSolutionSetFirstITCase(Configuration config) {
		super(config);
	}
	
	@Override
	protected void preSubmit() throws Exception {
		verticesPath = createTempFile("vertices.txt", ConnectedComponentsNepheleITCase.getEnumeratingVertices(NUM_VERTICES));
		edgesPath = createTempFile("edges.txt", ConnectedComponentsNepheleITCase.getRandomOddEvenEdges(NUM_EDGES, NUM_VERTICES, SEED));
		resultPath = getTempFilePath("results");
	}
	
	@Override
	protected Plan getPactPlan() {
		int dop = config.getInteger("ConnectedComponents#NumSubtasks", 1);
		int maxIterations = config.getInteger("ConnectedComponents#NumIterations", 1);
		
		return getPlanForWorksetConnectedComponentsWithSolutionSetAsFirstInput(dop, verticesPath, edgesPath, resultPath, maxIterations);
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
		return toParameterList(config1);
	}
	
	// --------------------------------------------------------------------------------------------
	//  Classes and methods for the test program
	// --------------------------------------------------------------------------------------------
	
	@ConstantFieldsSecondExcept({})
	public static final class UpdateComponentIdMatchMirrored extends MatchStub implements Serializable {
		
		private static final long serialVersionUID = 1L;

		@Override
		public void match(PactRecord currentVertexWithComponent, PactRecord newVertexWithComponent, Collector<PactRecord> out){
	
			long candidateComponentID = newVertexWithComponent.getField(1, PactLong.class).getValue();
			long currentComponentID = currentVertexWithComponent.getField(1, PactLong.class).getValue();
	
			if (candidateComponentID < currentComponentID) {
				out.collect(newVertexWithComponent);
			}
		}
	}
	
	private static Plan getPlanForWorksetConnectedComponentsWithSolutionSetAsFirstInput(
			int numSubTasks, String verticesInput, String edgeInput, String output, int maxIterations)
	{
		// create DataSourceContract for the vertices
		FileDataSource initialVertices = new FileDataSource(new DuplicateLongInputFormat(), verticesInput, "Vertices");
		
		WorksetIteration iteration = new WorksetIteration(0, "Connected Components Iteration");
		iteration.setInitialSolutionSet(initialVertices);
		iteration.setInitialWorkset(initialVertices);
		iteration.setMaximumNumberOfIterations(maxIterations);
		
		// create DataSourceContract for the edges
		FileDataSource edges = new FileDataSource(new LongLongInputFormat(), edgeInput, "Edges");

		// create CrossContract for distance computation
		MatchContract joinWithNeighbors = MatchContract.builder(new NeighborWithComponentIDJoin(), PactLong.class, 0, 0)
				.input1(iteration.getWorkset())
				.input2(edges)
				.name("Join Candidate Id With Neighbor")
				.build();

		// create ReduceContract for finding the nearest cluster centers
		ReduceContract minCandidateId = ReduceContract.builder(new MinimumComponentIDReduce(), PactLong.class, 0)
				.input(joinWithNeighbors)
				.name("Find Minimum Candidate Id")
				.build();
		
		// create CrossContract for distance computation
		MatchContract updateComponentId = MatchContract.builder(new UpdateComponentIdMatchMirrored(), PactLong.class, 0, 0)
				.input1(iteration.getSolutionSet())
				.input2(minCandidateId)
				.name("Update Component Id")
				.build();
		
		iteration.setNextWorkset(updateComponentId);
		iteration.setSolutionSetDelta(updateComponentId);

		// create DataSinkContract for writing the new cluster positions
		FileDataSink result = new FileDataSink(new RecordOutputFormat(), output, iteration, "Result");
		RecordOutputFormat.configureRecordFormat(result)
			.recordDelimiter('\n')
			.fieldDelimiter(' ')
			.field(PactLong.class, 0)
			.field(PactLong.class, 1);

		// return the PACT plan
		Plan plan = new Plan(result, "Workset Connected Components");
		plan.setDefaultParallelism(numSubTasks);
		return plan;
	}
}
