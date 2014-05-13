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

package eu.stratosphere.test.failingPrograms;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.java.record.operators.FileDataSink;
import eu.stratosphere.api.java.record.operators.FileDataSource;
import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.api.java.record.operators.MapOperator;
import eu.stratosphere.compiler.DataStatistics;
import eu.stratosphere.compiler.PactCompiler;
import eu.stratosphere.compiler.plan.OptimizedPlan;
import eu.stratosphere.compiler.plantranslate.NepheleJobGraphGenerator;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.test.operators.io.ContractITCaseIOFormats.ContractITCaseInputFormat;
import eu.stratosphere.test.operators.io.ContractITCaseIOFormats.ContractITCaseOutputFormat;
import eu.stratosphere.test.util.FailingTestBase;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;

/**
 * Tests whether the system recovers from a runtime exception from the user code.
 */
public class TaskFailureITCase extends FailingTestBase {

	// input for map tasks
	private static final String MAP_IN = "1 1\n2 2\n2 8\n4 4\n4 4\n6 6\n7 7\n8 8\n" +
											"1 1\n2 2\n2 2\n4 4\n4 4\n6 3\n5 9\n8 8\n" +
											"1 1\n2 2\n2 2\n3 0\n4 4\n5 9\n7 7\n8 8\n" +
											"1 1\n9 1\n5 9\n4 4\n4 4\n6 6\n7 7\n8 8\n";

	// expected result of working map job
	private static final String MAP_RESULT = "1 11\n2 12\n4 14\n4 14\n1 11\n2 12\n2 12\n4 14\n4 14\n3 16\n1 11\n2 12\n2 12\n0 13\n4 14\n1 11\n4 14\n4 14\n";

	private String inputPath;
	private String resultPath;
	
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
		plan.setDefaultParallelism(4);

		// optimize and compile plan 
		PactCompiler pc = new PactCompiler(new DataStatistics());
		OptimizedPlan op = pc.compile(plan);
		
		// return job graph of failing job
		NepheleJobGraphGenerator jgg = new NepheleJobGraphGenerator();
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
		plan.setDefaultParallelism(4);

		// optimize and compile plan
		PactCompiler pc = new PactCompiler(new DataStatistics());
		OptimizedPlan op = pc.compile(plan);

		// return job graph of working job
		NepheleJobGraphGenerator jgg = new NepheleJobGraphGenerator();
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
