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


package org.apache.flink.test.recordJobs.graph;

import java.io.Serializable;
import java.util.Iterator;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.Program;
import org.apache.flink.api.java.record.functions.CoGroupFunction;
import org.apache.flink.api.java.record.functions.FunctionAnnotation.ConstantFieldsFirst;
import org.apache.flink.api.java.record.functions.FunctionAnnotation.ConstantFieldsSecond;
import org.apache.flink.api.java.record.io.CsvInputFormat;
import org.apache.flink.api.java.record.io.CsvOutputFormat;
import org.apache.flink.api.java.record.operators.CoGroupOperator;
import org.apache.flink.api.java.record.operators.DeltaIteration;
import org.apache.flink.api.java.record.operators.FileDataSink;
import org.apache.flink.api.java.record.operators.FileDataSource;
import org.apache.flink.api.java.record.operators.JoinOperator;
import org.apache.flink.api.java.record.operators.MapOperator;
import org.apache.flink.test.recordJobs.graph.WorksetConnectedComponents.DuplicateLongMap;
import org.apache.flink.test.recordJobs.graph.WorksetConnectedComponents.NeighborWithComponentIDJoin;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.Record;
import org.apache.flink.util.Collector;

@SuppressWarnings("deprecation")
public class ConnectedComponentsWithCoGroup implements Program {
	
	private static final long serialVersionUID = 1L;

	@ConstantFieldsFirst(0)
	@ConstantFieldsSecond(0)
	public static final class MinIdAndUpdate extends CoGroupFunction implements Serializable {
		private static final long serialVersionUID = 1L;

		private final LongValue newComponentId = new LongValue();
		
		@Override
		public void coGroup(Iterator<Record> candidates, Iterator<Record> current, Collector<Record> out) throws Exception {
			if (!current.hasNext()) {
				throw new Exception("Error: Id not encountered before.");
			}
			Record old = current.next();
			long oldId = old.getField(1, LongValue.class).getValue();
			
			long minimumComponentID = Long.MAX_VALUE;

			while (candidates.hasNext()) {
				long candidateComponentID = candidates.next().getField(1, LongValue.class).getValue();
				if (candidateComponentID < minimumComponentID) {
					minimumComponentID = candidateComponentID;
				}
			}
			
			if (minimumComponentID < oldId) {
				newComponentId.setValue(minimumComponentID);
				old.setField(1, newComponentId);
				out.collect(old);
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

		CoGroupOperator minAndUpdate = CoGroupOperator.builder(new MinIdAndUpdate(), LongValue.class, 0, 0)
				.input1(joinWithNeighbors)
				.input2(iteration.getSolutionSet())
				.name("Min Id and Update")
				.build();
		
		iteration.setNextWorkset(minAndUpdate);
		iteration.setSolutionSetDelta(minAndUpdate);

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
