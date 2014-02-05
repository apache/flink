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

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;

import org.junit.Assert;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.operators.BulkIteration;
import eu.stratosphere.api.common.operators.FileDataSink;
import eu.stratosphere.api.common.operators.FileDataSource;
import eu.stratosphere.api.java.record.functions.ReduceFunction;
import eu.stratosphere.api.java.record.io.CsvOutputFormat;
import eu.stratosphere.api.java.record.io.TextInputFormat;
import eu.stratosphere.api.java.record.operators.ReduceOperator;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.test.util.TestBase2;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;
import eu.stratosphere.util.LogUtils;

@RunWith(Parameterized.class)
public class IterationWithAllReducerITCase extends TestBase2 {

	private static final String INPUT = "1\n" + "2\n" + "3\n" + "4\n" + "5\n" + "6\n" + "7\n" + "8\n";
	private static final String EXPECTED = "1\n";

	protected String dataPath;
	protected String resultPath;

	
	public IterationWithAllReducerITCase(Configuration config) {
		super(config);
		
		LogUtils.initializeDefaultConsoleLogger();
	}

	@Override
	protected void preSubmit() throws Exception {
		dataPath = createTempFile("datapoints.txt", INPUT);
		resultPath = getTempFilePath("result");
	}
	
	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(EXPECTED, resultPath);
	}

	@Override
	protected Plan getTestJob() {
		Plan plan = getTestPlanPlan(config.getInteger("DOP", 1), dataPath, resultPath);
		return plan;
	}

	@Parameters
	public static Collection<Object[]> getConfigurations() {
		Configuration config = new Configuration();
		config.setInteger("DOP", 4);
		return toParameterList(config);
	}
	
	private static Plan getTestPlanPlan(int numSubTasks, String input, String output) {

		FileDataSource initialInput = new FileDataSource(TextInputFormat.class, input, "input");
		
		BulkIteration iteration = new BulkIteration("Loop");
		iteration.setInput(initialInput);
		iteration.setMaximumNumberOfIterations(5);
		
		Assert.assertTrue(iteration.getMaximumNumberOfIterations() > 1);

		ReduceOperator sumReduce = ReduceOperator.builder(new PickOneReducer())
				.input(iteration.getPartialSolution())
				.name("Compute sum (Reduce)")
				.build();
		
		iteration.setNextPartialSolution(sumReduce);

		FileDataSink finalResult = new FileDataSink(CsvOutputFormat.class, output, iteration, "Output");
		CsvOutputFormat.configureRecordFormat(finalResult)
			.recordDelimiter('\n')
			.fieldDelimiter(' ')
			.field(StringValue.class, 0);

		Plan plan = new Plan(finalResult, "Iteration with AllReducer (keyless Reducer)");
		
		plan.setDefaultParallelism(numSubTasks);
		Assert.assertTrue(plan.getDefaultParallelism() > 1);
		
		return plan;
	}
	
	public static final class PickOneReducer extends ReduceFunction implements Serializable {
		private static final long serialVersionUID = 1L;
		
		@Override
		public void reduce(Iterator<Record> it, Collector<Record> out) {
			out.collect(it.next());
		}
	}
}
