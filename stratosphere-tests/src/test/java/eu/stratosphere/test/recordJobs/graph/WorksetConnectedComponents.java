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

package eu.stratosphere.test.recordJobs.graph;

import java.io.Serializable;
import java.util.Iterator;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.Program;
import eu.stratosphere.api.common.ProgramDescription;
import eu.stratosphere.api.java.record.operators.FileDataSink;
import eu.stratosphere.api.java.record.operators.FileDataSource;
import eu.stratosphere.api.java.record.operators.DeltaIteration;
import eu.stratosphere.api.java.record.functions.JoinFunction;
import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.api.java.record.functions.ReduceFunction;
import eu.stratosphere.api.java.record.functions.FunctionAnnotation.ConstantFields;
import eu.stratosphere.api.java.record.functions.FunctionAnnotation.ConstantFieldsFirst;
import eu.stratosphere.api.java.record.io.CsvInputFormat;
import eu.stratosphere.api.java.record.io.CsvOutputFormat;
import eu.stratosphere.api.java.record.operators.JoinOperator;
import eu.stratosphere.api.java.record.operators.MapOperator;
import eu.stratosphere.api.java.record.operators.ReduceOperator;
import eu.stratosphere.api.java.record.operators.ReduceOperator.Combinable;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;

/**
 *
 */
public class WorksetConnectedComponents implements Program, ProgramDescription {
	
	private static final long serialVersionUID = 1L;

	public static final class DuplicateLongMap extends MapFunction implements Serializable {
		private static final long serialVersionUID = 1L;

		@Override
		public void map(Record record, Collector<Record> out) throws Exception {
			record.setField(1, record.getField(0, LongValue.class));
			out.collect(record);
		}
	}
	
	/**
	 * UDF that joins a (Vertex-ID, Component-ID) pair that represents the current component that
	 * a vertex is associated with, with a (Source-Vertex-ID, Target-VertexID) edge. The function
	 * produces a (Target-vertex-ID, Component-ID) pair.
	 */
	public static final class NeighborWithComponentIDJoin extends JoinFunction implements Serializable {
		private static final long serialVersionUID = 1L;

		private final Record result = new Record();

		@Override
		public void join(Record vertexWithComponent, Record edge, Collector<Record> out) {
			this.result.setField(0, edge.getField(1, LongValue.class));
			this.result.setField(1, vertexWithComponent.getField(1, LongValue.class));
			out.collect(this.result);
		}
	}
	
	/**
	 * Minimum aggregation over (Vertex-ID, Component-ID) pairs, selecting the pair with the smallest Comonent-ID.
	 */
	@Combinable
	@ConstantFields(0)
	public static final class MinimumComponentIDReduce extends ReduceFunction implements Serializable {
		private static final long serialVersionUID = 1L;

		private final Record result = new Record();
		private final LongValue vertexId = new LongValue();
		private final LongValue minComponentId = new LongValue();
		
		@Override
		public void reduce(Iterator<Record> records, Collector<Record> out) {

			final Record first = records.next();
			final long vertexID = first.getField(0, LongValue.class).getValue();
			
			long minimumComponentID = first.getField(1, LongValue.class).getValue();

			while (records.hasNext()) {
				long candidateComponentID = records.next().getField(1, LongValue.class).getValue();
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
	public static final class UpdateComponentIdMatch extends JoinFunction implements Serializable {
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
	
	@SuppressWarnings("unchecked")
	@Override
	public Plan getPlan(String... args) {
		// parse job parameters
		final int numSubTasks = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		final String verticesInput = (args.length > 1 ? args[1] : "");
		final String edgeInput = (args.length > 2 ? args[2] : "");
		final String output = (args.length > 3 ? args[3] : "");
		final int maxIterations = (args.length > 4 ? Integer.parseInt(args[4]) : 1);

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
		JoinOperator updateComponentId = JoinOperator.builder(new UpdateComponentIdMatch(), LongValue.class, 0, 0)
				.input1(minCandidateId)
				.input2(iteration.getSolutionSet())
				.name("Update Component Id")
				.build();
		
		iteration.setNextWorkset(updateComponentId);
		iteration.setSolutionSetDelta(updateComponentId);

		// sink is the iteration result
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

	@Override
	public String getDescription() {
		return "Parameters: <numberOfSubTasks> <vertices> <edges> <out> <maxIterations>";
	}
}
