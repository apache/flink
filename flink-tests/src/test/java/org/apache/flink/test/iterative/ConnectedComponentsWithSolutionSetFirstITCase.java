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


package org.apache.flink.test.iterative;

import java.io.BufferedReader;
import java.io.Serializable;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.java.record.functions.JoinFunction;
import org.apache.flink.api.java.record.functions.FunctionAnnotation.ConstantFieldsSecondExcept;
import org.apache.flink.api.java.record.io.CsvInputFormat;
import org.apache.flink.api.java.record.io.CsvOutputFormat;
import org.apache.flink.api.java.record.operators.DeltaIteration;
import org.apache.flink.api.java.record.operators.FileDataSink;
import org.apache.flink.api.java.record.operators.FileDataSource;
import org.apache.flink.api.java.record.operators.JoinOperator;
import org.apache.flink.api.java.record.operators.MapOperator;
import org.apache.flink.api.java.record.operators.ReduceOperator;
import org.apache.flink.test.recordJobs.graph.WorksetConnectedComponents.DuplicateLongMap;
import org.apache.flink.test.recordJobs.graph.WorksetConnectedComponents.MinimumComponentIDReduce;
import org.apache.flink.test.recordJobs.graph.WorksetConnectedComponents.NeighborWithComponentIDJoin;
import org.apache.flink.test.testdata.ConnectedComponentsData;
import org.apache.flink.test.util.RecordAPITestBase;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.Record;
import org.apache.flink.util.Collector;

/**
 * Tests a bug that prevented that the solution set can be on both sides of the match/cogroup function.
 */
@SuppressWarnings("deprecation")
public class ConnectedComponentsWithSolutionSetFirstITCase extends RecordAPITestBase {
	
	private static final long SEED = 0xBADC0FFEEBEEFL;
	
	private static final int NUM_VERTICES = 1000;
	
	private static final int NUM_EDGES = 10000;

	
	protected String verticesPath;
	protected String edgesPath;
	protected String resultPath;

	public ConnectedComponentsWithSolutionSetFirstITCase(){
		setTaskManagerNumSlots(parallelism);
	}
	
	
	@Override
	protected void preSubmit() throws Exception {
		verticesPath = createTempFile("vertices.txt", ConnectedComponentsData.getEnumeratingVertices(NUM_VERTICES));
		edgesPath = createTempFile("edges.txt", ConnectedComponentsData.getRandomOddEvenEdges(NUM_EDGES, NUM_VERTICES, SEED));
		resultPath = getTempFilePath("results");
	}
	
	@Override
	protected Plan getTestJob() {
		return getPlanForWorksetConnectedComponentsWithSolutionSetAsFirstInput(parallelism, verticesPath, edgesPath,
				resultPath, 100);
	}

	@Override
	protected void postSubmit() throws Exception {
		for (BufferedReader reader : getResultReader(resultPath)) {
			ConnectedComponentsData.checkOddEvenResult(reader);
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//  Classes and methods for the test program
	// --------------------------------------------------------------------------------------------
	
	@ConstantFieldsSecondExcept({})
	public static final class UpdateComponentIdMatchMirrored extends JoinFunction implements Serializable {
		
		private static final long serialVersionUID = 1L;

		@Override
		public void join(Record currentVertexWithComponent, Record newVertexWithComponent, Collector<Record> out){
	
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
