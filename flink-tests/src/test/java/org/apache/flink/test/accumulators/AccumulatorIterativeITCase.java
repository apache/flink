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


package org.apache.flink.test.accumulators;

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.java.record.functions.ReduceFunction;
import org.apache.flink.api.java.record.io.CsvOutputFormat;
import org.apache.flink.api.java.record.io.TextInputFormat;
import org.apache.flink.api.java.record.operators.BulkIteration;
import org.apache.flink.api.java.record.operators.FileDataSink;
import org.apache.flink.api.java.record.operators.FileDataSource;
import org.apache.flink.api.java.record.operators.ReduceOperator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.util.RecordAPITestBase;
import org.apache.flink.types.Record;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.Collector;
import org.junit.Assert;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * To be finished !!! Didn't test with iterations yet;-(
 */
@SuppressWarnings("deprecation")
@RunWith(Parameterized.class)
public class AccumulatorIterativeITCase extends RecordAPITestBase {

	private static final String INPUT = "1\n" + "2\n" + "3\n";
	private static final String EXPECTED = "6\n";
	
	private static final int NUM_ITERATIONS = 3;
	private static final int NUM_SUBTASKS = 1;

	protected String dataPath;
	protected String resultPath;
	
	public AccumulatorIterativeITCase(Configuration config) {
		super(config);
	}

	@Override
	protected void preSubmit() throws Exception {
		dataPath = createTempFile("datapoints.txt", INPUT);
		resultPath = getTempFilePath("result");
	}
	
	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(EXPECTED, resultPath);
		
		Integer res = getJobExecutionResult().getAccumulatorResult("test");
		Assert.assertEquals(Integer.valueOf(NUM_ITERATIONS * 6), res);
	}

	@Override
	protected Plan getTestJob() {
		Plan plan = getTestPlanPlan(config.getInteger("IterationAllReducer#NoSubtasks", 1), dataPath, resultPath);
		return plan;
	}

	@Parameters
	public static Collection<Object[]> getConfigurations() {
		Configuration config1 = new Configuration();
		config1.setInteger("IterationAllReducer#NoSubtasks", NUM_SUBTASKS);
		return toParameterList(config1);
	}
	
	static Plan getTestPlanPlan(int numSubTasks, String input, String output) {

		FileDataSource initialInput = new FileDataSource(TextInputFormat.class, input, "input");
		
		BulkIteration iteration = new BulkIteration("Loop");
		iteration.setInput(initialInput);
		iteration.setMaximumNumberOfIterations(NUM_ITERATIONS);

		ReduceOperator sumReduce = ReduceOperator.builder(new SumReducer())
				.input(iteration.getPartialSolution())
				.name("Compute sum (Reduce)")
				.build();
		
		iteration.setNextPartialSolution(sumReduce);

		@SuppressWarnings("unchecked")
		FileDataSink finalResult = new FileDataSink(new CsvOutputFormat("\n",  " ", StringValue.class), output, iteration, "Output");

		Plan plan = new Plan(finalResult, "Iteration with AllReducer (keyless Reducer)");
		plan.setDefaultParallelism(numSubTasks);
		return plan;
	}
	
	static final class SumReducer extends ReduceFunction implements Serializable {
		
		private static final long serialVersionUID = 1L;
		
		private IntCounter testCounter = new IntCounter();

		@Override
		public void reduce(Iterator<Record> records, Collector<Record> out) {
			// Compute the sum
			int sum = 0;

			while (records.hasNext()) {
				Record r = records.next();
				Integer value = Integer.parseInt(r.getField(0, StringValue.class).getValue());
				sum += value;
				testCounter.add(value);
			}
			out.collect(new Record(new StringValue(Integer.toString(sum))));
		}

		@Override
		public void close() throws Exception {
			super.close();
			getRuntimeContext().addAccumulator("test", this.testCounter);
		}
	}

}
