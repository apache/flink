/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.test.contracts;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.StringTokenizer;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.pact.common.contract.CrossContract;
import eu.stratosphere.pact.common.contract.DataSinkContract;
import eu.stratosphere.pact.common.contract.DataSourceContract;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.io.TextOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.CrossStub;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.compiler.PactCompiler;
import eu.stratosphere.pact.compiler.jobgen.JobGraphGenerator;
import eu.stratosphere.pact.compiler.plan.OptimizedPlan;
import eu.stratosphere.pact.test.util.TestBase;

/**
 * @author Fabian Hueske
 */
@RunWith(Parameterized.class)
public class CrossTest extends TestBase

{
	private static final Log LOG = LogFactory.getLog(CrossTest.class);

	public CrossTest(String clusterConfig, Configuration testConfig) {
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

	private static final String CROSS_RESULT = "2 0\n2 0\n2 1\n2 1\n3 1\n3 1\n3 3\n3 3\n4 2\n4 2\n5 3\n5 3\n"
			+ "3 -1\n3 -1\n3 0\n3 0\n4 0\n4 0\n4 2\n4 2\n5 1\n5 1\n6 2\n6 2\n"
			+ "4 -2\n4 -2\n4 -1\n4 -1\n5 -1\n5 -1\n6 0\n6 0\n" + "5 -3\n5 -3\n5 -2\n5 -2\n6 -2\n6 -2\n";

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

	public static class CrossTestInFormat extends TextInputFormat<PactString, PactString> {

		@Override
		public boolean readLine(KeyValuePair<PactString, PactString> pair, byte[] line) {

			pair.setKey(new PactString(new String((char) line[0] + "")));
			pair.setValue(new PactString(new String((char) line[2] + "")));

			LOG.debug("Read in: [" + pair.getKey() + "," + pair.getValue() + "]");
			return true;
		}
	}

	public static class CrossTestOutFormat extends TextOutputFormat<PactString, PactInteger> {

		@Override
		public byte[] writeLine(KeyValuePair<PactString, PactInteger> pair) {
			LOG.debug("Writing out: [" + pair.getKey() + "," + pair.getValue() + "]");
			
			return (pair.getKey().toString() + " " + pair.getValue().toString() + "\n").getBytes();
		}
	}

	public static class TestCross extends
			CrossStub<PactString, PactString, PactString, PactString, PactString, PactInteger> {

		public void cross(PactString key1, PactString value1, PactString key2, PactString value2,
				Collector<PactString, PactInteger> out) {
			LOG.debug("Processing { [" + key1 + "," + value1 + "] , [" + key2 + "," + value2 + "] }");
			if (Integer.parseInt(value1.toString()) + Integer.parseInt(value2.toString()) <= 6) {
				out.collect(new PactString(Integer.parseInt(key1.toString()) + Integer.parseInt(key2.toString()) + ""),
						new PactInteger(Integer.parseInt(value2.toString()) - Integer.parseInt(value1.toString())));
			}
		}

	}

	@Override
	protected JobGraph getJobGraph() throws Exception {

		String pathPrefix = getFilesystemProvider().getURIPrefix() + getFilesystemProvider().getTempDirPath();

		DataSourceContract<PactString, PactString> input_left = new DataSourceContract<PactString, PactString>(
				CrossTestInFormat.class, pathPrefix + "/cross_left");
		input_left.setFormatParameter("delimiter", "\n");
		input_left.setDegreeOfParallelism(config.getInteger("CrossTest#NoSubtasks", 1));

		DataSourceContract<PactString, PactString> input_right = new DataSourceContract<PactString, PactString>(
				CrossTestInFormat.class, pathPrefix + "/cross_right");
		input_right.setFormatParameter("delimiter", "\n");
		input_right.setDegreeOfParallelism(config.getInteger("CrossTest#NoSubtasks", 1));

		CrossContract<PactString, PactString, PactString, PactString, PactString, PactInteger> testCross = new CrossContract<PactString, PactString, PactString, PactString, PactString, PactInteger>(
				TestCross.class);
		testCross.setDegreeOfParallelism(config.getInteger("CrossTest#NoSubtasks", 1));
		testCross.getStubParameters().setString(PactCompiler.HINT_LOCAL_STRATEGY,
				config.getString("CrossTest#LocalStrategy", ""));
		if (config.getString("CrossTest#ShipStrategy", "").equals("BROADCAST_FIRST")) {
			testCross.getStubParameters().setString(PactCompiler.HINT_SHIP_STRATEGY_FIRST_INPUT,
					PactCompiler.HINT_SHIP_STRATEGY_BROADCAST);
			testCross.getStubParameters().setString(PactCompiler.HINT_SHIP_STRATEGY_SECOND_INPUT,
					PactCompiler.HINT_SHIP_STRATEGY_FORWARD);
		} else if (config.getString("CrossTest#ShipStrategy", "").equals("BROADCAST_SECOND")) {
			testCross.getStubParameters().setString(PactCompiler.HINT_SHIP_STRATEGY_FIRST_INPUT,
					PactCompiler.HINT_SHIP_STRATEGY_BROADCAST);
			testCross.getStubParameters().setString(PactCompiler.HINT_SHIP_STRATEGY_SECOND_INPUT,
					PactCompiler.HINT_SHIP_STRATEGY_FORWARD);
		} else {
			testCross.getStubParameters().setString(PactCompiler.HINT_SHIP_STRATEGY,
					config.getString("CrossTest#ShipStrategy", ""));
		}

		DataSinkContract<PactString, PactInteger> output = new DataSinkContract<PactString, PactInteger>(
				CrossTestOutFormat.class, pathPrefix + "/result.txt");
		output.setDegreeOfParallelism(1);

		output.setInput(testCross);
		testCross.setFirstInput(input_left);
		testCross.setSecondInput(input_right);

		Plan plan = new Plan(output);

		PactCompiler pc = new PactCompiler();
		OptimizedPlan op = pc.compile(plan);

		JobGraphGenerator jgg = new JobGraphGenerator();
		return jgg.compileJobGraph(op);

	}

	@Override
	protected void postSubmit() throws Exception {

		String tempDir = getFilesystemProvider().getTempDirPath();
		
		// read result
		InputStream is = getFilesystemProvider().getInputStream(
				tempDir + "/result.txt");
		BufferedReader reader = new BufferedReader(new InputStreamReader(is));
		String line = reader.readLine();
		Assert.assertNotNull("No output computed", line);

		// collect out lines
		PriorityQueue<String> computedResult = new PriorityQueue<String>();
		while (line != null) {
			computedResult.add(line);
			line = reader.readLine();
		}
		reader.close();

		PriorityQueue<String> expectedResult = new PriorityQueue<String>();
		StringTokenizer st = new StringTokenizer(CROSS_RESULT, "\n");
		while (st.hasMoreElements()) {
			expectedResult.add(st.nextToken());
		}

		// print expected and computed results
		LOG.debug("Expected: " + expectedResult);
		LOG.debug("Computed: " + computedResult);

		Assert.assertEquals("Computed and expected results have different size", expectedResult.size(), computedResult
				.size());

		while (!expectedResult.isEmpty()) {
			String expectedLine = expectedResult.poll();
			String computedLine = computedResult.poll();
			LOG.debug("expLine: <" + expectedLine + ">\t\t: compLine: <" + computedLine + ">");
			Assert.assertEquals("Computed and expected lines differ", expectedLine, computedLine);
		}

		getFilesystemProvider().delete(tempDir + "/result.txt", false);
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

		return toParameterList(CrossTest.class, tConfigs);
	}
}
