/**
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

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.java.record.functions.CrossFunction;
import org.apache.flink.api.java.record.io.DelimitedInputFormat;
import org.apache.flink.api.java.record.operators.CrossOperator;
import org.apache.flink.api.java.record.operators.FileDataSink;
import org.apache.flink.api.java.record.operators.FileDataSource;
import org.apache.flink.compiler.PactCompiler;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.operators.io.ContractITCaseIOFormats.ContractITCaseInputFormat;
import org.apache.flink.test.operators.io.ContractITCaseIOFormats.ContractITCaseOutputFormat;
import org.apache.flink.test.util.RecordAPITestBase;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.Record;
import org.apache.flink.types.StringValue;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.LinkedList;

@RunWith(Parameterized.class)
public class CrossITCase extends RecordAPITestBase {

	private String leftInPath = null;
	private String rightInPath = null;
	private String resultPath = null;

	public CrossITCase(Configuration testConfig) {
		super(testConfig);
	}

	//private static final String LEFT_IN = "1 1\n2 2\n1 1\n2 2\n3 3\n4 4\n3 3\n4 4\n";

	//private static final String RIGHT_IN = "1 1\n1 2\n2 2\n2 4\n3 3\n3 6\n4 4\n4 8\n";

	//private static final String RESULT = "4 1\n4 1\n4 2\n4 2\n5 2\n5 2\n5 4\n5 4\n6 3\n6 3\n7 4\n7 4\n"
	//	+ "5 0\n5 0\n5 1\n5 1\n6 1\n6 1\n6 3\n6 3\n7 2\n7 2\n8 3\n8 3\n"
	//	+ "6 -1\n6 -1\n6 0\n6 0\n7 0\n7 0\n8 1\n8 1\n" + "7 -2\n7 -2\n7 -1\n7 -1\n8 -1\n8 -1\n";

	//private static final String RESULT = "10 1\n10 1\n10 5\n10 5\n4 1\n4 1\n4 2\n4 2\n5 0\n5 0\n5 1\n," +
	//		"5 1\n5 2\n5 2\n5 4\n5 4\n6 -1\n6 -1\n6 0\n6 0\n6 1\n6 1\n6 3\n6 3\n6 3\n6 3\n6 6\n6 6\n7 -1\n" +
	//		"7 -1\n7 -2\n7 -2\n7 0\n7 0\n7 2\n7 2\n7 2\n7 2\n7 4\n7 4\n7 5\n7 5\n7 8\n7 8\n8 -1\n8 -1\n8 1\n" +
	//		"8 1\n8 1\n8 1\n8 3\n8 3\n8 4\n8 4\n8 7\n8 7\n9 0\n9 0\n9 2\n9 2\n9 3\n9 3\n9 6\n9 6\n";

	//private static final String RESULT = "2 2\n4 4\n1 1\n3 3\n2 2\n4 4\n1 1\n3 3\n5 0\n5 1\n6 1\n 6 3\n" +
	//		"7 2\n7 5\n8 3\n8 7\n7 -2\n7 -1\n8 -1\n8 1\n9 0\n9 3\n10 1\n10 5\n4 1\n4 2\n5 2\n5 4\n6 3\n" +
	//		"6 6\n7 4\n7 8\n6 -1\n6 0\n7 0\n7 2\n8 1\n8 4\n9 2\n9 6\n5 0\n5 1\n6 1\n6 3\n7 2\n7 5\n 8 3\n" +
	//		"8 7\n7 -2\n7 -1\n8 -1\n8 1\n9 0\n9 3\n10 1\n10 5\n4 1\n4 2\n5 2\n5 4\n6 3\n6 6\n7 4\n7 8\n" +
	//		"6 -1\n6 0\n7 0\n7 2\n8 1\n8 4\n9 2\n9 6";


	private static final String LEFT_IN = "1 1\n2 2\n3 3\n";
	private static final String RIGHT_IN = "3 6\n4 4\n4 8\n";

	private static final String RESULT = "6 6\n7 5\n7 8\n7 4\n8 3\n8 7\n8 4\n9 2\n9 6\n";

	@Override
	protected void preSubmit() throws Exception {
		leftInPath = createTempFile("left_in.txt", LEFT_IN);
		rightInPath = createTempFile("right_in.txt", RIGHT_IN);
		resultPath = getTempDirPath("result");
	}


	public static class TestCross extends CrossFunction implements Serializable {
		private static final long serialVersionUID = 1L;

		private StringValue string = new StringValue();
		private IntValue integer = new IntValue();
		
		@Override
		public Record cross(Record record1, Record record2) throws Exception {
			string = record1.getField(1, string);
			int val1 = Integer.parseInt(string.toString());
			string = record2.getField(1, string);
			int val2 = Integer.parseInt(string.toString());
			string = record1.getField(0, string);
			int key1 = Integer.parseInt(string.toString());
			string = record2.getField(0, string);
			int key2 = Integer.parseInt(string.toString());

			string.setValue((key1 + key2 + 2) + "");
			integer.setValue(val2 - val1 + 1);

			record1.setField(0, string);
			record1.setField(1, integer);

			return record1;
		}

	}

	@Override
	protected Plan getTestJob() {

		FileDataSource input_left = new FileDataSource(
				new ContractITCaseInputFormat(), leftInPath);
		DelimitedInputFormat.configureDelimitedFormat(input_left)
			.recordDelimiter('\n');
		input_left.setDegreeOfParallelism(config.getInteger("CrossTest#NoSubtasks", 1));

		FileDataSource input_right = new FileDataSource(
				new ContractITCaseInputFormat(), rightInPath);
		DelimitedInputFormat.configureDelimitedFormat(input_right)
			.recordDelimiter('\n');
		input_right.setDegreeOfParallelism(config.getInteger("CrossTest#NoSubtasks", 1));

		CrossOperator testCross = CrossOperator.builder(new TestCross()).build();
		testCross.setDegreeOfParallelism(config.getInteger("CrossTest#NoSubtasks", 1));
		testCross.getParameters().setString(PactCompiler.HINT_LOCAL_STRATEGY,
				config.getString("CrossTest#LocalStrategy", ""));
		if (config.getString("CrossTest#ShipStrategy", "").equals("BROADCAST_FIRST")) {
			testCross.getParameters().setString(PactCompiler.HINT_SHIP_STRATEGY_FIRST_INPUT,
					PactCompiler.HINT_SHIP_STRATEGY_BROADCAST);
			testCross.getParameters().setString(PactCompiler.HINT_SHIP_STRATEGY_SECOND_INPUT,
					PactCompiler.HINT_SHIP_STRATEGY_FORWARD);
		} else if (config.getString("CrossTest#ShipStrategy", "").equals("BROADCAST_SECOND")) {
			testCross.getParameters().setString(PactCompiler.HINT_SHIP_STRATEGY_FIRST_INPUT,
					PactCompiler.HINT_SHIP_STRATEGY_BROADCAST);
			testCross.getParameters().setString(PactCompiler.HINT_SHIP_STRATEGY_SECOND_INPUT,
					PactCompiler.HINT_SHIP_STRATEGY_FORWARD);
		} else {
			testCross.getParameters().setString(PactCompiler.HINT_SHIP_STRATEGY,
					config.getString("CrossTest#ShipStrategy", ""));
		}

		FileDataSink output = new FileDataSink(
				new ContractITCaseOutputFormat(), resultPath);
		output.setDegreeOfParallelism(1);

		output.setInput(testCross);
		testCross.setFirstInput(input_left);
		testCross.setSecondInput(input_right);

		return new Plan(output);
	}

	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(RESULT, resultPath);
	}

	@Parameters
	public static Collection<Object[]> getConfigurations() throws FileNotFoundException, IOException {

		LinkedList<Configuration> tConfigs = new LinkedList<Configuration>();

		String[] localStrategies = { PactCompiler.HINT_LOCAL_STRATEGY_NESTEDLOOP_BLOCKED_OUTER_FIRST,
				PactCompiler.HINT_LOCAL_STRATEGY_NESTEDLOOP_BLOCKED_OUTER_SECOND,
				PactCompiler.HINT_LOCAL_STRATEGY_NESTEDLOOP_STREAMED_OUTER_FIRST,
				PactCompiler.HINT_LOCAL_STRATEGY_NESTEDLOOP_STREAMED_OUTER_SECOND };

		String[] shipStrategies = { "BROADCAST_FIRST", "BROADCAST_SECOND"
		// PactCompiler.HINT_SHIP_STRATEGY_BROADCAST
		// PactCompiler.HINT_SHIP_STRATEGY_SFR
		};

		for (String localStrategy : localStrategies) {
			for (String shipStrategy : shipStrategies) {

				Configuration config = new Configuration();
				config.setString("CrossTest#LocalStrategy", localStrategy);
				config.setString("CrossTest#ShipStrategy", shipStrategy);
				config.setInteger("CrossTest#NoSubtasks", 4);

				tConfigs.add(config);
			}
		}

		return toParameterList(tConfigs);
	}
}
