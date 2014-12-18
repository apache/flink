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
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.record.functions.JoinFunction;
import org.apache.flink.api.java.record.functions.ReduceFunction;
import org.apache.flink.api.java.record.functions.FunctionAnnotation.ConstantFields;
import org.apache.flink.api.java.record.operators.BulkIteration;
import org.apache.flink.api.java.record.operators.FileDataSink;
import org.apache.flink.api.java.record.operators.FileDataSource;
import org.apache.flink.api.java.record.operators.JoinOperator;
import org.apache.flink.api.java.record.operators.ReduceOperator;
import org.apache.flink.api.java.record.operators.ReduceOperator.Combinable;
import org.apache.flink.test.recordJobs.graph.pageRankUtil.DanglingPageRankInputFormat;
import org.apache.flink.test.recordJobs.graph.pageRankUtil.ImprovedAdjacencyListInputFormat;
import org.apache.flink.test.recordJobs.graph.pageRankUtil.LongArrayView;
import org.apache.flink.test.recordJobs.graph.pageRankUtil.PageWithRankOutFormat;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.Record;
import org.apache.flink.util.Collector;

@SuppressWarnings("deprecation")
public class SimplePageRank implements Program, ProgramDescription {
	
	private static final long serialVersionUID = 1L;
	
	private static final String NUM_VERTICES_CONFIG_PARAM = "pageRank.numVertices";
	
	// --------------------------------------------------------------------------------------------

	public static final class JoinVerexWithEdgesMatch extends JoinFunction implements Serializable {
		private static final long serialVersionUID = 1L;

		private Record record = new Record();
		private LongValue vertexID = new LongValue();
		private DoubleValue partialRank = new DoubleValue();
		private DoubleValue rank = new DoubleValue();

		private LongArrayView adjacentNeighbors = new LongArrayView();
		
		@Override
		public void join(Record pageWithRank, Record edges, Collector<Record> out) throws Exception {
			rank = pageWithRank.getField(1, rank);
			adjacentNeighbors = edges.getField(1, adjacentNeighbors);
			int numNeighbors = adjacentNeighbors.size();

			double rankToDistribute = rank.getValue() / (double) numNeighbors;

			partialRank.setValue(rankToDistribute);
			record.setField(1, partialRank);
			
			for (int n = 0; n < numNeighbors; n++) {
				vertexID.setValue(adjacentNeighbors.getQuick(n));
				record.setField(0, vertexID);
				out.collect(record);
			}
		}
	}
	
	@Combinable
	@ConstantFields(0)
	public static final class AggregatingReduce extends ReduceFunction implements Serializable {
		private static final long serialVersionUID = 1L;
		
		private final DoubleValue sum = new DoubleValue();

		@Override
		public void reduce(Iterator<Record> pageWithPartialRank, Collector<Record> out) throws Exception {
			Record rec = null;
			double rankSum = 0.0;
			
			while (pageWithPartialRank.hasNext()) {
				rec = pageWithPartialRank.next();
				rankSum += rec.getField(1, DoubleValue.class).getValue();
			}
			sum.setValue(rankSum);
			
			rec.setField(1, sum);
			out.collect(rec);
		}
	}
	
	public static final class JoinOldAndNew extends JoinFunction implements Serializable {
		private static final long serialVersionUID = 1L;

		private Record record = new Record();
		private LongValue vertexID = new LongValue();
		private DoubleValue newRank = new DoubleValue();
		private DoubleValue rank = new DoubleValue();
		
		@Override
		public void join(Record pageWithRank, Record newPageWithRank, Collector<Record> out) throws Exception {
			rank = pageWithRank.getField(1, rank);
			newRank = newPageWithRank.getField(1, newRank);
			vertexID = pageWithRank.getField(0, vertexID);
			
			double epsilon = 0.05;
			double criterion = rank.getValue() - newRank.getValue();
			
			if(Math.abs(criterion) > epsilon)
			{
				record.setField(0, new IntValue(1));
				out.collect(record);
			}
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	public Plan getPlan(String ... args) {
		int dop = 1;
		String pageWithRankInputPath = "";
		String adjacencyListInputPath = "";
		String outputPath = "";
		int numIterations = 25;
		long numVertices = 5;

		if (args.length >= 6) {
			dop = Integer.parseInt(args[0]);
			pageWithRankInputPath = args[1];
			adjacencyListInputPath = args[2];
			outputPath = args[3];
			numIterations = Integer.parseInt(args[4]);
			numVertices = Long.parseLong(args[5]);
		}
		
		FileDataSource pageWithRankInput = new FileDataSource(new DanglingPageRankInputFormat(),
			pageWithRankInputPath, "PageWithRank Input");
		pageWithRankInput.getParameters().setLong(NUM_VERTICES_CONFIG_PARAM, numVertices);
		
		BulkIteration iteration = new BulkIteration("Page Rank Loop");
		iteration.setInput(pageWithRankInput);
		
		FileDataSource adjacencyListInput = new FileDataSource(new ImprovedAdjacencyListInputFormat(),
			adjacencyListInputPath, "AdjancencyListInput");
		
		JoinOperator join = JoinOperator.builder(new JoinVerexWithEdgesMatch(), LongValue.class, 0, 0)
				.input1(iteration.getPartialSolution())
				.input2(adjacencyListInput)
				.name("Join with Edges")
				.build();
		
		ReduceOperator rankAggregation = ReduceOperator.builder(new AggregatingReduce(), LongValue.class, 0)
				.input(join)
				.name("Rank Aggregation")
				.build();
		
		iteration.setNextPartialSolution(rankAggregation);
		iteration.setMaximumNumberOfIterations(numIterations);
		
		JoinOperator termination = JoinOperator.builder(new JoinOldAndNew(), LongValue.class, 0, 0)
				.input1(iteration.getPartialSolution())
				.input2(rankAggregation)
				.name("Join Old and New")
				.build();
		
		iteration.setTerminationCriterion(termination);
		
		FileDataSink out = new FileDataSink(new PageWithRankOutFormat(), outputPath, iteration, "Final Ranks");

		Plan p = new Plan(out, "Simple PageRank");
		p.setDefaultParallelism(dop);
		return p;
	}

	@Override
	public String getDescription() {
		return "Parameters: <degree-of-parallelism> <pages-input-path> <edges-input-path> <output-path> <max-iterations> <num-vertices> <num-dangling-vertices>";
	}
}
