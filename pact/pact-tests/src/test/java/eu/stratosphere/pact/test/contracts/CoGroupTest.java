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
import java.util.Iterator;
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
import eu.stratosphere.pact.common.contract.CoGroupContract;
import eu.stratosphere.pact.common.contract.DataSinkContract;
import eu.stratosphere.pact.common.contract.DataSourceContract;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.io.TextOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.stub.CoGroupStub;
import eu.stratosphere.pact.common.stub.Collector;
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
public class CoGroupTest extends TestBase
/*
 * TODO: - Allow multiple data sinks - Add strategy selection for CoGroup -
 * Implement CoGroupStub!!!
 */

{
	public CoGroupTest(String clusterConfig, Configuration testConfig) {
		super(testConfig, clusterConfig);
	}

	private static final String COGROUP_LEFT_IN_1 = "1 1\n2 2\n3 3\n4 4\n";

	private static final String COGROUP_LEFT_IN_2 = "1 2\n2 3\n3 4\n4 5\n";

	private static final String COGROUP_LEFT_IN_3 = "1 3\n2 4\n3 5\n4 6\n";

	private static final String COGROUP_LEFT_IN_4 = "1 4\n2 5\n3 6\n4 7\n";

	private static final String COGROUP_RIGHT_IN_1 = "1 1\n2 2\n3 3\n5 1\n";

	private static final String COGROUP_RIGHT_IN_2 = "1 1\n2 2\n3 3\n6 1\n";

	private static final String COGROUP_RIGHT_IN_3 = "1 1\n2 2\n2 2\n7 1\n";

	private static final String COGROUP_RIGHT_IN_4 = "1 1\n2 2\n2 2\n8 1\n";

	private static final String COGROUP_RESULT = "1 6\n2 2\n3 12\n4 22\n5 -1\n6 -1\n7 -1\n8 -1\n";

	@Override
	protected void preSubmit() throws Exception {
		getHDFSProvider().createDir(getHDFSProvider().getHdfsHome() + "/cogroup_left");

		getHDFSProvider().writeFileToHDFS(getHDFSProvider().getHdfsHome() + "/cogroup_left/cogroupTest_1.txt",
			COGROUP_LEFT_IN_1);
		getHDFSProvider().writeFileToHDFS(getHDFSProvider().getHdfsHome() + "/cogroup_left/cogroupTest_2.txt",
			COGROUP_LEFT_IN_2);
		getHDFSProvider().writeFileToHDFS(getHDFSProvider().getHdfsHome() + "/cogroup_left/cogroupTest_3.txt",
			COGROUP_LEFT_IN_3);
		getHDFSProvider().writeFileToHDFS(getHDFSProvider().getHdfsHome() + "/cogroup_left/cogroupTest_4.txt",
			COGROUP_LEFT_IN_4);

		getHDFSProvider().createDir(getHDFSProvider().getHdfsHome() + "/cogroup_right");

		getHDFSProvider().writeFileToHDFS(getHDFSProvider().getHdfsHome() + "/cogroup_right/cogroupTest_1.txt",
			COGROUP_RIGHT_IN_1);
		getHDFSProvider().writeFileToHDFS(getHDFSProvider().getHdfsHome() + "/cogroup_right/cogroupTest_2.txt",
			COGROUP_RIGHT_IN_2);
		getHDFSProvider().writeFileToHDFS(getHDFSProvider().getHdfsHome() + "/cogroup_right/cogroupTest_3.txt",
			COGROUP_RIGHT_IN_3);
		getHDFSProvider().writeFileToHDFS(getHDFSProvider().getHdfsHome() + "/cogroup_right/cogroupTest_4.txt",
			COGROUP_RIGHT_IN_4);
	}

	public static class CoGroupTestInFormat extends TextInputFormat<PactString, PactString> {

		private static final Log LOG = LogFactory.getLog(CoGroupTestInFormat.class);

		@Override
		public boolean readLine(KeyValuePair<PactString, PactString> pair, byte[] line) {

			pair.setKey(new PactString(new String((char) line[0] + "")));
			pair.setValue(new PactString(new String((char) line[2] + "")));

			LOG.info("Read in: [" + pair.getKey() + "," + pair.getValue() + "]");

			return true;
		}

		// @Override
		// public byte[] writeLine(KeyValuePair<N_String, N_String> pair)
		// {
		// return (pair.getKey().toString() + " " + pair.getValue().toString() + "\n").getBytes();
		// }
	}

	public static class CoGroupOutFormat extends TextOutputFormat<PactString, PactInteger> {

		private static final Log LOG = LogFactory.getLog(CoGroupOutFormat.class);

		// @Override
		// public void readLine(KeyValuePair<N_String, N_Integer> pair, byte[] line)
		// {
		//
		// String[] tokens = line.toString().split(" ");
		//
		// pair.setKey(new N_String(tokens[0]));
		// pair.setValue(new N_Integer(Integer.parseInt(tokens[1])));
		//
		// }

		@Override
		public byte[] writeLine(KeyValuePair<PactString, PactInteger> pair) {
			LOG.info("Writing out: [" + pair.getKey() + "," + pair.getValue() + "]");

			return (pair.getKey().toString() + " " + pair.getValue().toString() + "\n").getBytes();
		}
	}

	public static class TestCoGrouper extends CoGroupStub<PactString, PactString, PactString, PactString, PactInteger> {

		private static final Log LOG = LogFactory.getLog(TestCoGrouper.class);

		@Override
		public void coGroup(PactString key, Iterator<PactString> values1, Iterator<PactString> values2,
				Collector<PactString, PactInteger> out) {
			int sum = 0;
			LOG.info("Start iterating over input1");
			while (values1.hasNext()) {
				PactString value = values1.next();
				sum += Integer.parseInt(value.toString());

				LOG.info("Processed: [" + key + "," + value + "]");
			}
			LOG.info("Start iterating over input2");
			while (values2.hasNext()) {
				PactString value = values2.next();
				sum -= Integer.parseInt(value.toString());

				LOG.info("Processed: [" + key + "," + value + "]");
			}
			out.collect(key, new PactInteger(sum));
			LOG.info("Finished");
		}

	}

	@Override
	protected JobGraph getJobGraph() throws Exception {
		DataSourceContract<PactString, PactString> input_left = new DataSourceContract<PactString, PactString>(
			CoGroupTestInFormat.class, getHDFSProvider().getHdfsHome() + "/cogroup_left");
		input_left.setFormatParameter("delimiter", "\n");
		input_left
			.setDegreeOfParallelism(config.getInteger("CoGroupTest#NoSubtasks", 1));

		DataSourceContract<PactString, PactString> input_right = new DataSourceContract<PactString, PactString>(
			CoGroupTestInFormat.class, getHDFSProvider().getHdfsHome() + "/cogroup_right");
		input_right.setFormatParameter("delimiter", "\n");
		input_right.setDegreeOfParallelism(config
			.getInteger("CoGroupTest#NoSubtasks", 1));

		CoGroupContract<PactString, PactString, PactString, PactString, PactInteger> testCoGrouper = new CoGroupContract<PactString, PactString, PactString, PactString, PactInteger>(
			TestCoGrouper.class);
		testCoGrouper.setDegreeOfParallelism(config.getInteger("CoGroupTest#NoSubtasks",
			1));
		testCoGrouper.getStubParameters().setString(PactCompiler.HINT_LOCAL_STRATEGY,
			config.getString("CoGroupTest#LocalStrategy", ""));
		testCoGrouper.getStubParameters().setString(PactCompiler.HINT_SHIP_STRATEGY,
			config.getString("CoGroupTest#ShipStrategy", ""));

		DataSinkContract<PactString, PactInteger> output = new DataSinkContract<PactString, PactInteger>(
			CoGroupOutFormat.class, getHDFSProvider().getHdfsHome() + "/result.txt");
		output.setDegreeOfParallelism(1);

		output.setInput(testCoGrouper);
		testCoGrouper.setFirstInput(input_left);
		testCoGrouper.setSecondInput(input_right);

		Plan plan = new Plan(output);

		PactCompiler pc = new PactCompiler();
		OptimizedPlan op = pc.compile(plan);

		JobGraphGenerator jgg = new JobGraphGenerator();
		return jgg.compileJobGraph(op);
	}

	@Override
	protected void postSubmit() throws Exception {

		// read result
		InputStream is = getHDFSProvider().getHdfsInputStream(getHDFSProvider().getHdfsHome() + "/result.txt");
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
		StringTokenizer st = new StringTokenizer(COGROUP_RESULT, "\n");
		while (st.hasMoreElements()) {
			expectedResult.add(st.nextToken());
		}

		// print expected and computed results
		System.out.println("Expected: " + expectedResult);
		System.out.println("Computed: " + computedResult);

		Assert.assertEquals("Computed and expected results have different size", expectedResult.size(), computedResult
			.size());

		while (!expectedResult.isEmpty()) {
			String expectedLine = expectedResult.poll();
			String computedLine = computedResult.poll();
			System.out.println("expLine: <" + expectedLine + ">\t\t: compLine: <" + computedLine + ">");
			Assert.assertEquals("Computed and expected lines differ", expectedLine, computedLine);
		}
	}

	@Parameters
	public static Collection<Object[]> getConfigurations() throws FileNotFoundException, IOException {

		LinkedList<Configuration> tConfigs = new LinkedList<Configuration>();

		String[] localStrategies = { PactCompiler.HINT_LOCAL_STRATEGY_SORT, };

		String[] shipStrategies = { PactCompiler.HINT_SHIP_STRATEGY_REPARTITION, };

		for (String localStrategy : localStrategies) {
			for (String shipStrategy : shipStrategies) {

				Configuration config = new Configuration();
				config.setString("CoGroupTest#LocalStrategy", localStrategy);
				config.setString("CoGroupTest#ShipStrategy", shipStrategy);
				config.setInteger("CoGroupTest#NoSubtasks", 4);

				tConfigs.add(config);
			}
		}

		return toParameterList(CoGroupTest.class, tConfigs);
	}
}
