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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.LinkedList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.operators.FileDataSink;
import eu.stratosphere.api.common.operators.FileDataSource;
import eu.stratosphere.api.java.record.functions.CrossFunction;
import eu.stratosphere.api.java.record.io.DelimitedInputFormat;
import eu.stratosphere.api.java.record.operators.CrossOperator;
import eu.stratosphere.compiler.DataStatistics;
import eu.stratosphere.compiler.PactCompiler;
import eu.stratosphere.compiler.plan.OptimizedPlan;
import eu.stratosphere.compiler.plantranslate.NepheleJobGraphGenerator;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.test.operators.io.ContractITCaseIOFormats.ContractITCaseInputFormat;
import eu.stratosphere.test.operators.io.ContractITCaseIOFormats.ContractITCaseOutputFormat;
import eu.stratosphere.test.util.TestBase;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;

/**
 */
@RunWith(Parameterized.class)
public class CrossITCase extends TestBase

{
	private static final Log LOG = LogFactory.getLog(CrossITCase.class);

	public CrossITCase(String clusterConfig, Configuration testConfig) {
		super(testConfig, clusterConfig);
	}

	private static final String CROSS_LEFT_IN_1 = "1 1\n2 2\n";

	private static final String CROSS_LEFT_IN_2 = "1 1\n2 2\n";

	private static final String CROSS_LEFT_IN_3 = "3 3\n4 4\n";

	private static final String CROSS_LEFT_IN_4 = "3 3\n4 4\n";

	private static final String CROSS_RIGHT_IN_1 = "1 1\n1 2\n";

	private static final String CROSS_RIGHT_IN_2 = "2 2\n2 4\n";

	private static final String CROSS_RIGHT_IN_3 = "3 3\n3 6\n";

	private static final String CROSS_RIGHT_IN_4 = "4 4\n4 8\n";

	private static final String CROSS_RESULT = "4 1\n4 1\n4 2\n4 2\n5 2\n5 2\n5 4\n5 4\n6 3\n6 3\n7 4\n7 4\n"
		+ "5 0\n5 0\n5 1\n5 1\n6 1\n6 1\n6 3\n6 3\n7 2\n7 2\n8 3\n8 3\n"
		+ "6 -1\n6 -1\n6 0\n6 0\n7 0\n7 0\n8 1\n8 1\n" + "7 -2\n7 -2\n7 -1\n7 -1\n8 -1\n8 -1\n";

	@Override
	protected void preSubmit() throws Exception {
		String tempDir = getFilesystemProvider().getTempDirPath();

		getFilesystemProvider().createDir(tempDir + "/cross_left");

		getFilesystemProvider().createFile(tempDir + "/cross_left/crossTest_1.txt", CROSS_LEFT_IN_1);
		getFilesystemProvider().createFile(tempDir + "/cross_left/crossTest_2.txt", CROSS_LEFT_IN_2);
		getFilesystemProvider().createFile(tempDir + "/cross_left/crossTest_3.txt", CROSS_LEFT_IN_3);
		getFilesystemProvider().createFile(tempDir + "/cross_left/crossTest_4.txt", CROSS_LEFT_IN_4);

		getFilesystemProvider().createDir(tempDir + "/cross_right");

		getFilesystemProvider().createFile(tempDir + "/cross_right/crossTest_1.txt", CROSS_RIGHT_IN_1);
		getFilesystemProvider().createFile(tempDir + "/cross_right/crossTest_2.txt", CROSS_RIGHT_IN_2);
		getFilesystemProvider().createFile(tempDir + "/cross_right/crossTest_3.txt", CROSS_RIGHT_IN_3);
		getFilesystemProvider().createFile(tempDir + "/cross_right/crossTest_4.txt", CROSS_RIGHT_IN_4);
	}


	public static class TestCross extends CrossFunction implements Serializable {
		private static final long serialVersionUID = 1L;

		private StringValue string = new StringValue();
		private IntValue integer = new IntValue();
		
		@Override
		public void cross(Record record1, Record record2, Collector<Record> out) {
			string = record1.getField(1, string);
			int val1 = Integer.parseInt(string.toString());
			string = record2.getField(1, string);
			int val2 = Integer.parseInt(string.toString());
			string = record1.getField(0, string);
			int key1 = Integer.parseInt(string.toString());
			string = record2.getField(0, string);
			int key2 = Integer.parseInt(string.toString());
			
			LOG.debug("Processing { [" + key1 + "," + val1 + "] , [" + key2 + "," + val2 + "] }");
			
			if (val1 + val2 <= 6) {
				string.setValue((key1 + key2 + 2) + "");
				integer.setValue(val2 - val1 + 1);
				
				record1.setField(0, string);
				record1.setField(1, integer);
				
				out.collect(record1);
			}
		}

	}

	@Override
	protected JobGraph getJobGraph() throws Exception {

		String pathPrefix = getFilesystemProvider().getURIPrefix() + getFilesystemProvider().getTempDirPath();

		FileDataSource input_left = new FileDataSource(
				new ContractITCaseInputFormat(), pathPrefix + "/cross_left");
		DelimitedInputFormat.configureDelimitedFormat(input_left)
			.recordDelimiter('\n');
		input_left.setDegreeOfParallelism(config.getInteger("CrossTest#NoSubtasks", 1));

		FileDataSource input_right = new FileDataSource(
				new ContractITCaseInputFormat(), pathPrefix + "/cross_right");
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
				new ContractITCaseOutputFormat(), pathPrefix + "/result.txt");
		output.setDegreeOfParallelism(1);

		output.addInput(testCross);
		testCross.addFirstInput(input_left);
		testCross.addSecondInput(input_right);

		Plan plan = new Plan(output);

		PactCompiler pc = new PactCompiler(new DataStatistics());
		OptimizedPlan op = pc.compile(plan);

		NepheleJobGraphGenerator jgg = new NepheleJobGraphGenerator();
		return jgg.compileJobGraph(op);

	}

	@Override
	protected void postSubmit() throws Exception {

		String tempDir = getFilesystemProvider().getTempDirPath();
		
		compareResultsByLinesInMemory(CROSS_RESULT, tempDir + "/result.txt");
		
		getFilesystemProvider().delete(tempDir + "/result.txt", true);
		getFilesystemProvider().delete(tempDir + "/cross_left", true);
		getFilesystemProvider().delete(tempDir + "/cross_right", true);
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

		return toParameterList(CrossITCase.class, tConfigs);
	}
}
