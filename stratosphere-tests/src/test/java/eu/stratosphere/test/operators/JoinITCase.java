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

package eu.stratosphere.test.operators;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.java.record.operators.FileDataSink;
import eu.stratosphere.api.java.record.operators.FileDataSource;
import eu.stratosphere.api.java.record.functions.JoinFunction;
import eu.stratosphere.api.java.record.io.DelimitedInputFormat;
import eu.stratosphere.api.java.record.operators.JoinOperator;
import eu.stratosphere.compiler.PactCompiler;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.test.operators.io.ContractITCaseIOFormats.ContractITCaseInputFormat;
import eu.stratosphere.test.operators.io.ContractITCaseIOFormats.ContractITCaseOutputFormat;
import eu.stratosphere.test.util.RecordAPITestBase;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.LinkedList;

@RunWith(Parameterized.class)
public class JoinITCase extends RecordAPITestBase {

	private static final Log LOG = LogFactory.getLog(JoinITCase.class);

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
		input_left.setDegreeOfParallelism(config.getInteger("MatchTest#NoSubtasks", 1));

		FileDataSource input_right = new FileDataSource(
				new ContractITCaseInputFormat(), rightInPath);
		DelimitedInputFormat.configureDelimitedFormat(input_right)
			.recordDelimiter('\n');
		input_right.setDegreeOfParallelism(config.getInteger("MatchTest#NoSubtasks", 1));

		JoinOperator testMatcher = JoinOperator.builder(new TestMatcher(), StringValue.class, 0, 0)
			.build();
		testMatcher.setDegreeOfParallelism(config.getInteger("MatchTest#NoSubtasks", 1));
		testMatcher.getParameters().setString(PactCompiler.HINT_LOCAL_STRATEGY,
				config.getString("MatchTest#LocalStrategy", ""));
		if (config.getString("MatchTest#ShipStrategy", "").equals("BROADCAST_FIRST")) {
			testMatcher.getParameters().setString(PactCompiler.HINT_SHIP_STRATEGY_FIRST_INPUT,
					PactCompiler.HINT_SHIP_STRATEGY_BROADCAST);
			testMatcher.getParameters().setString(PactCompiler.HINT_SHIP_STRATEGY_SECOND_INPUT,
					PactCompiler.HINT_SHIP_STRATEGY_FORWARD);
		} else if (config.getString("MatchTest#ShipStrategy", "").equals("BROADCAST_SECOND")) {
			testMatcher.getParameters().setString(PactCompiler.HINT_SHIP_STRATEGY_FIRST_INPUT,
					PactCompiler.HINT_SHIP_STRATEGY_FORWARD);
			testMatcher.getParameters().setString(PactCompiler.HINT_SHIP_STRATEGY_SECOND_INPUT,
					PactCompiler.HINT_SHIP_STRATEGY_BROADCAST);
		} else {
			testMatcher.getParameters().setString(PactCompiler.HINT_SHIP_STRATEGY,
					config.getString("MatchTest#ShipStrategy", ""));
		}

		FileDataSink output = new FileDataSink(
				new ContractITCaseOutputFormat(), resultPath);
		output.setDegreeOfParallelism(1);

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

		String[] localStrategies = { PactCompiler.HINT_LOCAL_STRATEGY_SORT_BOTH_MERGE,
				PactCompiler.HINT_LOCAL_STRATEGY_HASH_BUILD_FIRST, PactCompiler.HINT_LOCAL_STRATEGY_HASH_BUILD_SECOND };

		String[] shipStrategies = { PactCompiler.HINT_SHIP_STRATEGY_REPARTITION_HASH, "BROADCAST_FIRST", "BROADCAST_SECOND"};

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
