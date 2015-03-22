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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.java.record.functions.JoinFunction;
import org.apache.flink.api.java.record.io.DelimitedInputFormat;
import org.apache.flink.api.java.record.operators.FileDataSink;
import org.apache.flink.api.java.record.operators.FileDataSource;
import org.apache.flink.api.java.record.operators.JoinOperator;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.configuration.Configuration;
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
import java.util.LinkedList;

@SuppressWarnings("deprecation")
@RunWith(Parameterized.class)
public class JoinITCase extends RecordAPITestBase {

	private static final Logger LOG = LoggerFactory.getLogger(JoinITCase.class);

	String leftInPath = null;
	String rightInPath = null;
	String resultPath = null;

	public JoinITCase(Configuration testConfig) {
		super(testConfig);
	}

	private static final String LEFT_IN = "1 1\n2 2\n3 3\n4 4\n1 2\n2 3\n3 4\n4 5\n" +
			"1 3\n2 4\n3 5\n4 6\n1 4\n2 5\n3 6\n4 7\n";

	private static final String RIGHT_IN = "1 1\n2 2\n3 3\n5 1\n1 1\n2 2\n3 3\n6 1\n" +
			"1 1\n2 2\n2 2\n7 1\n1 1\n2 2\n2 2\n8 1\n";

	private static final String RESULT = "2 1\n2 1\n2 1\n2 1\n2 2\n2 2\n2 2\n2 2\n2 3\n2 3\n2 3\n2 3\n2 4\n2 4\n2 4\n2 4\n"
		+ "4 1\n4 1\n4 2\n4 2\n4 3\n4 3\n4 4\n4 4\n"
		+ "3 1\n3 2\n3 3\n3 4\n3 1\n3 2\n3 3\n3 4\n3 1\n3 2\n3 3\n3 4\n3 1\n3 2\n3 3\n3 4\n3 1\n3 2\n3 3\n3 4\n3 1\n3 2\n3 3\n3 4\n";

	@Override
	protected void preSubmit() throws Exception {
		leftInPath = createTempFile("left_in.txt", LEFT_IN);
		rightInPath = createTempFile("right_in.txt", RIGHT_IN);
		resultPath = getTempDirPath("result");
	}

	public static class TestMatcher extends JoinFunction implements Serializable {
		private static final long serialVersionUID = 1L;

		private StringValue keyString = new StringValue();
		private StringValue valueString = new StringValue();
		
		@Override
		public void join(Record value1, Record value2, Collector<Record> out)
				throws Exception {
			keyString = value1.getField(0, keyString);
			keyString.setValue(""+ (Integer.parseInt(keyString.getValue())+1));
			value1.setField(0, keyString);
			valueString = value1.getField(1, valueString);
			int val1 = Integer.parseInt(valueString.getValue())+2;
			valueString = value2.getField(1, valueString);
			int val2 = Integer.parseInt(valueString.getValue())+1;
			
			value1.setField(1, new IntValue(val1 - val2));
			
			out.collect(value1);
			
			if (LOG.isDebugEnabled())
				LOG.debug("Processed: [" + keyString.toString() + "," + val1 + "] + " +
						"[" + keyString.toString() + "," + val2 + "]");
		}

	}

	@Override
	protected Plan getTestJob() {
		FileDataSource input_left = new FileDataSource(
				new ContractITCaseInputFormat(), leftInPath);
		DelimitedInputFormat.configureDelimitedFormat(input_left)
			.recordDelimiter('\n');
		input_left.setParallelism(config.getInteger("MatchTest#NoSubtasks", 1));

		FileDataSource input_right = new FileDataSource(
				new ContractITCaseInputFormat(), rightInPath);
		DelimitedInputFormat.configureDelimitedFormat(input_right)
			.recordDelimiter('\n');
		input_right.setParallelism(config.getInteger("MatchTest#NoSubtasks", 1));

		JoinOperator testMatcher = JoinOperator.builder(new TestMatcher(), StringValue.class, 0, 0)
			.build();
		testMatcher.setParallelism(config.getInteger("MatchTest#NoSubtasks", 1));
		testMatcher.getParameters().setString(Optimizer.HINT_LOCAL_STRATEGY,
				config.getString("MatchTest#LocalStrategy", ""));
		if (config.getString("MatchTest#ShipStrategy", "").equals("BROADCAST_FIRST")) {
			testMatcher.getParameters().setString(Optimizer.HINT_SHIP_STRATEGY_FIRST_INPUT,
					Optimizer.HINT_SHIP_STRATEGY_BROADCAST);
			testMatcher.getParameters().setString(Optimizer.HINT_SHIP_STRATEGY_SECOND_INPUT,
					Optimizer.HINT_SHIP_STRATEGY_FORWARD);
		} else if (config.getString("MatchTest#ShipStrategy", "").equals("BROADCAST_SECOND")) {
			testMatcher.getParameters().setString(Optimizer.HINT_SHIP_STRATEGY_FIRST_INPUT,
					Optimizer.HINT_SHIP_STRATEGY_FORWARD);
			testMatcher.getParameters().setString(Optimizer.HINT_SHIP_STRATEGY_SECOND_INPUT,
					Optimizer.HINT_SHIP_STRATEGY_BROADCAST);
		} else {
			testMatcher.getParameters().setString(Optimizer.HINT_SHIP_STRATEGY,
					config.getString("MatchTest#ShipStrategy", ""));
		}

		FileDataSink output = new FileDataSink(
				new ContractITCaseOutputFormat(), resultPath);
		output.setParallelism(1);

		output.setInput(testMatcher);
		testMatcher.setFirstInput(input_left);
		testMatcher.setSecondInput(input_right);

		return new Plan(output);
	}

	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(RESULT, resultPath);
	}

	@Parameters
	public static Collection<Object[]> getConfigurations() throws FileNotFoundException, IOException {

		LinkedList<Configuration> tConfigs = new LinkedList<Configuration>();

		String[] localStrategies = { Optimizer.HINT_LOCAL_STRATEGY_SORT_BOTH_MERGE,
				Optimizer.HINT_LOCAL_STRATEGY_HASH_BUILD_FIRST, Optimizer.HINT_LOCAL_STRATEGY_HASH_BUILD_SECOND };

		String[] shipStrategies = { Optimizer.HINT_SHIP_STRATEGY_REPARTITION_HASH, "BROADCAST_FIRST", "BROADCAST_SECOND"};

		for (String localStrategy : localStrategies) {
			for (String shipStrategy : shipStrategies) {

				Configuration config = new Configuration();
				config.setString("MatchTest#LocalStrategy", localStrategy);
				config.setString("MatchTest#ShipStrategy", shipStrategy);
				config.setInteger("MatchTest#NoSubtasks", 4);

				tConfigs.add(config);
			}
		}

		return toParameterList(tConfigs);
	}
}
