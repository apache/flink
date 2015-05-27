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

package org.apache.flink.test.failingPrograms;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.java.record.functions.MapFunction;
import org.apache.flink.api.java.record.operators.FileDataSink;
import org.apache.flink.api.java.record.operators.FileDataSource;
import org.apache.flink.api.java.record.operators.MapOperator;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plantranslate.JobGraphGenerator;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.test.operators.io.ContractITCaseIOFormats.ContractITCaseInputFormat;
import org.apache.flink.test.operators.io.ContractITCaseIOFormats.ContractITCaseOutputFormat;
import org.apache.flink.test.util.FailingTestBase;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.Record;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.Collector;

/**
 * Tests whether the system recovers from a runtime exception from the user code.
 */
public class TaskFailureITCase extends FailingTestBase {

	private static final int parallelism = 4;

	// input for map tasks
	private static final String MAP_IN = "1 1\n2 2\n2 8\n4 4\n4 4\n6 6\n7 7\n8 8\n" +
											"1 1\n2 2\n2 2\n4 4\n4 4\n6 3\n5 9\n8 8\n" +
											"1 1\n2 2\n2 2\n3 0\n4 4\n5 9\n7 7\n8 8\n" +
											"1 1\n9 1\n5 9\n4 4\n4 4\n6 6\n7 7\n8 8\n";

	// expected result of working map job
	private static final String MAP_RESULT = "1 11\n2 12\n4 14\n4 14\n1 11\n2 12\n2 12\n4 14\n4 14\n" +
											"3 16\n1 11\n2 12\n2 12\n0 13\n4 14\n1 11\n4 14\n4 14\n";

	private String inputPath;
	private String resultPath;

	public TaskFailureITCase(){
		setTaskManagerNumSlots(parallelism);
	}
	
	@Override
	protected void preSubmit() throws Exception {
		inputPath = createTempFile("input", MAP_IN);
		resultPath = getTempDirPath("result");
	}


	@Override
	protected JobGraph getFailingJobGraph() throws Exception {
		
		// init data source 
		FileDataSource input = new FileDataSource(new ContractITCaseInputFormat(), inputPath);

		// init failing map task
		MapOperator testMapper = MapOperator.builder(FailingMapper.class).build();

		// init data sink
		FileDataSink output = new FileDataSink(new ContractITCaseOutputFormat(), resultPath);

		// compose failing program
		output.setInput(testMapper);
		testMapper.setInput(input);

		// generate plan
		Plan plan = new Plan(output);
		plan.setExecutionConfig(new ExecutionConfig());
		plan.setDefaultParallelism(parallelism);

		// optimize and compile plan 
		Optimizer pc = new Optimizer(new DataStatistics(), this.config);
		OptimizedPlan op = pc.compile(plan);
		
		// return job graph of failing job
		JobGraphGenerator jgg = new JobGraphGenerator();
		return jgg.compileJobGraph(op);
	}


	@Override
	protected JobGraph getJobGraph() throws Exception {
		
		// init data source 
		FileDataSource input = new FileDataSource(new ContractITCaseInputFormat(), inputPath);

		// init (working) map task
		MapOperator testMapper = MapOperator.builder(TestMapper.class).build();

		// init data sink
		FileDataSink output = new FileDataSink(new ContractITCaseOutputFormat(), resultPath);

		// compose working program
		output.setInput(testMapper);
		testMapper.setInput(input);

		// generate plan
		Plan plan = new Plan(output);
		plan.setExecutionConfig(new ExecutionConfig());
		plan.setDefaultParallelism(4);

		// optimize and compile plan
		Optimizer pc = new Optimizer(new DataStatistics(), this.config);
		OptimizedPlan op = pc.compile(plan);

		// return job graph of working job
		JobGraphGenerator jgg = new JobGraphGenerator();
		return jgg.compileJobGraph(op);
	}


	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(MAP_RESULT, resultPath);
	}
	

	@Override
	protected int getTimeout() {
		// time out for this job is 30 secs
		return 30;
	}


	/**
	 * working program
	 */
	public static class TestMapper extends MapFunction {

		private static final long serialVersionUID = 1L;
		
		private final StringValue string = new StringValue();
		private final IntValue integer = new IntValue();

		@Override
		public void map(Record record, Collector<Record> out) throws Exception {
			
			final StringValue keyString = record.getField(0, this.string);
			final int key = Integer.parseInt(keyString.toString());
			
			final StringValue valueString = record.getField(1, this.string);
			final int value = Integer.parseInt(valueString.toString());
			
			if (key + value < 10) {
				record.setField(0, valueString);
				this.integer.setValue(key + 10);
				record.setField(1, this.integer);
				out.collect(record);
			}
		}
	}
	
	/**
	 * Map Failing program
	 */
	public static class FailingMapper extends MapFunction {

		private static final long serialVersionUID = 1L;

		@Override
		public void map(Record record, Collector<Record> out) throws Exception {
			throw new RuntimeException("This is an expected Test Exception");
		}
	}
}
