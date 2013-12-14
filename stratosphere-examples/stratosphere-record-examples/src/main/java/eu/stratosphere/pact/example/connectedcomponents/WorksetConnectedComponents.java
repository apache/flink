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

package eu.stratosphere.pact.example.connectedcomponents;

import java.io.Serializable;
import java.util.Iterator;

import eu.stratosphere.api.functions.StubAnnotation.ConstantFields;
import eu.stratosphere.api.functions.StubAnnotation.ConstantFieldsFirst;
import eu.stratosphere.api.operators.FileDataSink;
import eu.stratosphere.api.operators.FileDataSource;
import eu.stratosphere.api.operators.WorksetIteration;
import eu.stratosphere.api.plan.Plan;
import eu.stratosphere.api.plan.PlanAssembler;
import eu.stratosphere.api.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.contract.ReduceContract.Combinable;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.types.PactLong;
import eu.stratosphere.types.PactRecord;
import eu.stratosphere.util.Collector;

/**
 *
 */
public class WorksetConnectedComponents implements PlanAssembler, PlanAssemblerDescription {
	
	/**
	 * UDF that joins a (Vertex-ID, Component-ID) pair that represents the current component that
	 * a vertex is associated with, with a (Source-Vertex-ID, Target-VertexID) edge. The function
	 * produces a (Target-vertex-ID, Component-ID) pair.
	 */
	public static final class NeighborWithComponentIDJoin extends MatchStub implements Serializable {
		
		private static final long serialVersionUID = 1L;

		private final PactRecord result = new PactRecord();

		@Override
		public void match(PactRecord vertexWithComponent, PactRecord edge, Collector<PactRecord> out) {
			this.result.setField(0, edge.getField(1, PactLong.class));
			this.result.setField(1, vertexWithComponent.getField(1, PactLong.class));
			out.collect(this.result);
		}
	}
	
	/**
	 * Minimum aggregation over (Vertex-ID, Component-ID) pairs, selecting the pair with the smallest Comonent-ID.
	 */
	@Combinable
	@ConstantFields(0)
	public static final class MinimumComponentIDReduce extends ReduceStub implements Serializable {
		private static final long serialVersionUID = 1L;

		private final PactRecord result = new PactRecord();
		private final PactLong vertexId = new PactLong();
		private final PactLong minComponentId = new PactLong();
		
		@Override
		public void reduce(Iterator<PactRecord> records, Collector<PactRecord> out) {

			final PactRecord first = records.next();
			final long vertexID = first.getField(0, PactLong.class).getValue();
			
			long minimumComponentID = first.getField(1, PactLong.class).getValue();

			while (records.hasNext()) {
				long candidateComponentID = records.next().getField(1, PactLong.class).getValue();
				if (candidateComponentID < minimumComponentID) {
					minimumComponentID = candidateComponentID;
				}
			}
			
			this.vertexId.setValue(vertexID);
			this.minComponentId.setValue(minimumComponentID);
			this.result.setField(0, this.vertexId);
			this.result.setField(1, this.minComponentId);
			out.collect(this.result);
		}
	}
	
	/**
	 * UDF that joins a candidate (Vertex-ID, Component-ID) pair with another (Vertex-ID, Component-ID) pair.
	 * Returns the candidate pair, if the candidate's Component-ID is smaller.
	 */
	@ConstantFieldsFirst(0)
	public static final class UpdateComponentIdMatch extends MatchStub implements Serializable {
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
	
	@Override
	public Plan getPlan(String... args) {
		// parse job parameters
		final int numSubTasks = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		final String verticesInput = (args.length > 1 ? args[1] : "");
		final String edgeInput = (args.length > 2 ? args[2] : "");
		final String output = (args.length > 3 ? args[3] : "");
		final int maxIterations = (args.length > 4 ? Integer.parseInt(args[4]) : 1);

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
		MatchContract joinWithNeighbors = MatchContract.builder(new NeighborWithComponentIDJoin(), PactLong.class, 0, 0)
				.input1(iteration.getWorkset())
				.input2(edges)
				.name("Join Candidate Id With Neighbor")
				.build();

		// find for each neighbor the smallest of all candidates
		ReduceContract minCandidateId = ReduceContract.builder(new MinimumComponentIDReduce(), PactLong.class, 0)
				.input(joinWithNeighbors)
				.name("Find Minimum Candidate Id")
				.build();
		
		// join candidates with the solution set and update if the candidate component-id is smaller
		MatchContract updateComponentId = MatchContract.builder(new UpdateComponentIdMatch(), PactLong.class, 0, 0)
				.input1(minCandidateId)
				.input2(iteration.getSolutionSet())
				.name("Update Component Id")
				.build();
		
		iteration.setNextWorkset(updateComponentId);
		iteration.setSolutionSetDelta(updateComponentId);

		// sink is the iteration result
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

	@Override
	public String getDescription() {
		return "Parameters: <numberOfSubTasks> <vertices> <edges> <out> <maxIterations>";
	}
}
