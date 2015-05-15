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

package org.apache.flink.test.operators;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.java.record.functions.ReduceFunction;
import org.apache.flink.api.java.record.io.DelimitedInputFormat;
import org.apache.flink.api.java.record.operators.FileDataSink;
import org.apache.flink.api.java.record.operators.FileDataSource;
import org.apache.flink.api.java.record.operators.ReduceOperator;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plantranslate.JobGraphGenerator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.test.operators.io.ContractITCaseIOFormats.ContractITCaseInputFormat;
import org.apache.flink.test.operators.io.ContractITCaseIOFormats.ContractITCaseOutputFormat;
import org.apache.flink.test.util.RecordAPITestBase;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.Record;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.Collector;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;

@SuppressWarnings("deprecation")
@RunWith(Parameterized.class)
public class ReduceITCase extends RecordAPITestBase {

	String inPath = null;
	String resultPath = null;

	public ReduceITCase(Configuration testConfig) {
		super(testConfig);
	}

	private static final String IN = "1 1\n2 2\n2 8\n4 4\n4 4\n6 6\n7 7\n8 8\n1 1\n" +
			"2 2\n2 2\n4 4\n4 4\n6 3\n5 9\n8 8\n1 1\n2 2\n2 2\n3 0\n4 4\n5 9\n7 7\n8 8\n" +
			"1 1\n9 1\n5 9\n4 4\n4 4\n6 6\n7 7\n8 8\n";

	private static final String RESULT = "1 4\n2 18\n3 0\n4 28\n5 27\n6 15\n7 21\n8 32\n9 1\n";

	@Override
	protected void preSubmit() throws Exception {
		inPath = createTempFile("in.txt", IN);
		resultPath = getTempDirPath("result");
	}

	@ReduceOperator.Combinable
	public static class TestReducer extends ReduceFunction implements Serializable {
		private static final long serialVersionUID = 1L;

		private StringValue reduceValue = new StringValue();
		private StringValue combineValue = new StringValue();

		@Override
		public void combine(Iterator<Record> records, Collector<Record> out) {
			Record record = null;
			int sum = 0;
			
			while (records.hasNext()) {
				record = records.next();
				combineValue = record.getField(1, combineValue);
				sum += Integer.parseInt(combineValue.toString());
			}
			combineValue.setValue(sum + "");
			record.setField(1, combineValue);
			out.collect(record);
		}

		@Override
		public void reduce(Iterator<Record> records, Collector<Record> out) {
			Record record = null;
			int sum = 0;
			
			while (records.hasNext()) {
				record = records.next();
				reduceValue = record.getField(1, reduceValue);
				sum += Integer.parseInt(reduceValue.toString());
			}
			record.setField(1, new IntValue(sum));
			out.collect(record);
		}
	}

	@Override
	protected JobGraph getJobGraph() throws Exception {
		FileDataSource input = new FileDataSource(
				new ContractITCaseInputFormat(), inPath);
		DelimitedInputFormat.configureDelimitedFormat(input)
			.recordDelimiter('\n');
		input.setParallelism(config.getInteger("ReduceTest#NoSubtasks", 1));

		ReduceOperator testReducer = ReduceOperator.builder(new TestReducer(), StringValue.class, 0)
			.build();
		testReducer.setParallelism(config.getInteger("ReduceTest#NoSubtasks", 1));
		testReducer.getParameters().setString(Optimizer.HINT_LOCAL_STRATEGY,
				config.getString("ReduceTest#LocalStrategy", ""));
		testReducer.getParameters().setString(Optimizer.HINT_SHIP_STRATEGY,
				config.getString("ReduceTest#ShipStrategy", ""));

		FileDataSink output = new FileDataSink(
				new ContractITCaseOutputFormat(), resultPath);
		output.setParallelism(1);

		output.setInput(testReducer);
		testReducer.setInput(input);

		Plan plan = new Plan(output);
		plan.setExecutionConfig(new ExecutionConfig());
		Optimizer pc = new Optimizer(new DataStatistics(), this.config);
		OptimizedPlan op = pc.compile(plan);

		JobGraphGenerator jgg = new JobGraphGenerator();
		return jgg.compileJobGraph(op);

	}

	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(RESULT, resultPath);
	}

	@Parameters
	public static Collection<Object[]> getConfigurations() throws FileNotFoundException, IOException {

		LinkedList<Configuration> tConfigs = new LinkedList<Configuration>();

		String[] localStrategies = { Optimizer.HINT_LOCAL_STRATEGY_SORT };
		String[] shipStrategies = { Optimizer.HINT_SHIP_STRATEGY_REPARTITION_HASH };

		for (String localStrategy : localStrategies) {
			for (String shipStrategy : shipStrategies) {

				Configuration config = new Configuration();
				config.setString("ReduceTest#LocalStrategy", localStrategy);
				config.setString("ReduceTest#ShipStrategy", shipStrategy);
				config.setInteger("ReduceTest#NoSubtasks", 4);
				tConfigs.add(config);
			}
		}

		return toParameterList(tConfigs);
	}
}
