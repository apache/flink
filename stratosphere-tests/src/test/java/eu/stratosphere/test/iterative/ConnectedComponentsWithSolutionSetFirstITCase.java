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

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.operators.FileDataSink;
import eu.stratosphere.api.common.operators.FileDataSource;
import eu.stratosphere.api.common.operators.DeltaIteration;
import eu.stratosphere.api.java.record.functions.JoinFunction;
import eu.stratosphere.api.java.record.functions.FunctionAnnotation.ConstantFieldsSecondExcept;
import eu.stratosphere.api.java.record.io.CsvInputFormat;
import eu.stratosphere.api.java.record.io.CsvOutputFormat;
import eu.stratosphere.api.java.record.operators.JoinOperator;
import eu.stratosphere.api.java.record.operators.MapOperator;
import eu.stratosphere.api.java.record.operators.ReduceOperator;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.example.java.record.connectedcomponents.WorksetConnectedComponents.DuplicateLongMap;
import eu.stratosphere.example.java.record.connectedcomponents.WorksetConnectedComponents.MinimumComponentIDReduce;
import eu.stratosphere.example.java.record.connectedcomponents.WorksetConnectedComponents.NeighborWithComponentIDJoin;
import eu.stratosphere.test.iterative.nephele.ConnectedComponentsNepheleITCase;
import eu.stratosphere.test.util.TestBase2;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;

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
	protected Plan getTestJob() {
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
	public static final class UpdateComponentIdMatchMirrored extends JoinFunction implements Serializable {
		
		private static final long serialVersionUID = 1L;

		@Override
		public void match(Record currentVertexWithComponent, Record newVertexWithComponent, Collector<Record> out){
	
			long candidateComponentID = newVertexWithComponent.getField(1, LongValue.class).getValue();
			long currentComponentID = currentVertexWithComponent.getField(1, LongValue.class).getValue();
	
			if (candidateComponentID < currentComponentID) {
				out.collect(newVertexWithComponent);
			}
		}
	}
	
	@SuppressWarnings("unchecked")
	private static Plan getPlanForWorksetConnectedComponentsWithSolutionSetAsFirstInput(
			int numSubTasks, String verticesInput, String edgeInput, String output, int maxIterations)
	{
		// data source for initial vertices
		FileDataSource initialVertices = new FileDataSource(new CsvInputFormat(' ', LongValue.class), verticesInput, "Vertices");
		
		MapOperator verticesWithId = MapOperator.builder(DuplicateLongMap.class).input(initialVertices).name("Assign Vertex Ids").build();
		
		DeltaIteration iteration = new DeltaIteration(0, "Connected Components Iteration");
		iteration.setInitialSolutionSet(verticesWithId);
		iteration.setInitialWorkset(verticesWithId);
		iteration.setMaximumNumberOfIterations(maxIterations);
		
		// create DataSourceContract for the edges
		FileDataSource edges = new FileDataSource(new CsvInputFormat(' ', LongValue.class, LongValue.class), edgeInput, "Edges");

		// create CrossOperator for distance computation
		JoinOperator joinWithNeighbors = JoinOperator.builder(new NeighborWithComponentIDJoin(), LongValue.class, 0, 0)
				.input1(iteration.getWorkset())
				.input2(edges)
				.name("Join Candidate Id With Neighbor")
				.build();

		// create ReduceOperator for finding the nearest cluster centers
		ReduceOperator minCandidateId = ReduceOperator.builder(new MinimumComponentIDReduce(), LongValue.class, 0)
				.input(joinWithNeighbors)
				.name("Find Minimum Candidate Id")
				.build();
		
		// create CrossOperator for distance computation
		JoinOperator updateComponentId = JoinOperator.builder(new UpdateComponentIdMatchMirrored(), LongValue.class, 0, 0)
				.input1(iteration.getSolutionSet())
				.input2(minCandidateId)
				.name("Update Component Id")
				.build();
		
		iteration.setNextWorkset(updateComponentId);
		iteration.setSolutionSetDelta(updateComponentId);

		// create DataSinkContract for writing the new cluster positions
		FileDataSink result = new FileDataSink(new CsvOutputFormat(), output, iteration, "Result");
		CsvOutputFormat.configureRecordFormat(result)
			.recordDelimiter('\n')
			.fieldDelimiter(' ')
			.field(LongValue.class, 0)
			.field(LongValue.class, 1);

		// return the PACT plan
		Plan plan = new Plan(result, "Workset Connected Components");
		plan.setDefaultParallelism(numSubTasks);
		return plan;
	}
}
