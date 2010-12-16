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
import eu.stratosphere.pact.common.contract.DataSinkContract;
import eu.stratosphere.pact.common.contract.DataSourceContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.contract.ReduceContract.Combinable;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.io.TextOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.ReduceStub;
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
public class ReduceTest extends TestBase
/*
 * TODO: - Allow multiple data sinks
 */

{

	public ReduceTest(String clusterConfig, Configuration testConfig) {
		super(testConfig, clusterConfig);
	}

	private static final String REDUCE_IN_1 = "1 1\n2 2\n2 8\n4 4\n4 4\n6 6\n7 7\n8 8\n";

	private static final String REDUCE_IN_2 = "1 1\n2 2\n2 2\n4 4\n4 4\n6 3\n5 9\n8 8\n";

	private static final String REDUCE_IN_3 = "1 1\n2 2\n2 2\n3 0\n4 4\n5 9\n7 7\n8 8\n";

	private static final String REDUCE_IN_4 = "1 1\n9 1\n5 9\n4 4\n4 4\n6 6\n7 7\n8 8\n";

	private static final String REDUCE_RESULT = "1 4\n2 18\n3 0\n4 28\n5 27\n6 15\n7 21\n8 32\n9 1\n";

	@Override
	protected void preSubmit() throws Exception {

		this.getHDFSProvider().createDir(getHDFSProvider().getHdfsHome() + "/reduceInput");

		this.getHDFSProvider().writeFileToHDFS(getHDFSProvider().getHdfsHome() + "/reduceInput/reduceTest_1.txt",
			REDUCE_IN_1);
		this.getHDFSProvider().writeFileToHDFS(getHDFSProvider().getHdfsHome() + "/reduceInput/reduceTest_2.txt",
			REDUCE_IN_2);
		this.getHDFSProvider().writeFileToHDFS(getHDFSProvider().getHdfsHome() + "/reduceInput/reduceTest_3.txt",
			REDUCE_IN_3);
		this.getHDFSProvider().writeFileToHDFS(getHDFSProvider().getHdfsHome() + "/reduceInput/reduceTest_4.txt",
			REDUCE_IN_4);
	}

	public static class ReduceTestInFormat extends TextInputFormat<PactString, PactString> {

		private static final Log LOG = LogFactory.getLog(ReduceTestInFormat.class);

		@Override
		public boolean readLine(KeyValuePair<PactString, PactString> pair, byte[] line) {

			pair.setKey(new PactString(new String((char) line[0] + "")));
			pair.setValue(new PactString(new String((char) line[2] + "")));

			LOG.info("Read in: [" + pair.getKey() + "," + pair.getValue() + "]");
			return true;
		}
	}

	public static class ReduceTestOutFormat extends TextOutputFormat<PactString, PactInteger> {
		private static final Log LOG = LogFactory.getLog(ReduceTestOutFormat.class);

		@Override
		public byte[] writeLine(KeyValuePair<PactString, PactInteger> pair) {
			LOG.info("Writing out: [" + pair.getKey() + "," + pair.getValue() + "]");

			return (pair.getKey().toString() + " " + pair.getValue().toString() + "\n").getBytes();
		}
	}

	@Combinable
	public static class TestReducer extends ReduceStub<PactString, PactString, PactString, PactInteger> {
		private static final Log LOG = LogFactory.getLog(TestReducer.class);

		@Override
		public void reduce(PactString key, Iterator<PactString> values, Collector<PactString, PactInteger> out) {

			int sum = 0;
			while (values.hasNext()) {
				PactString v = values.next();
				sum += Integer.parseInt(v.toString());

				LOG.info("Processed: [" + key + "," + v + "]");
			}
			out.collect(key, new PactInteger(sum));
		}

		@Override
		public void combine(PactString key, Iterator<PactString> values, Collector<PactString, PactString> out) {
			int sum = 0;

			while (values.hasNext()) {
				PactString v = values.next();
				sum += Integer.parseInt(v.toString());

				LOG.info("Combined: [" + key + "," + v + "]");
			}

			out.collect(key, new PactString(sum + ""));
		}
	}

	@Override
	protected JobGraph getJobGraph() throws Exception {
		DataSourceContract<PactString, PactString> input = new DataSourceContract<PactString, PactString>(
			ReduceTestInFormat.class, getHDFSProvider().getHdfsHome() + "/reduceInput");
		input.setFormatParameter("delimiter", "\n");
		input.setDegreeOfParallelism(config.getInteger("ReduceTest#NoSubtasks", 1));

		ReduceContract<PactString, PactString, PactString, PactInteger> testReducer = new ReduceContract<PactString, PactString, PactString, PactInteger>(
			TestReducer.class);
		testReducer
			.setDegreeOfParallelism(config.getInteger("ReduceTest#NoSubtasks", 1));
		testReducer.getStubParameters().setString(PactCompiler.HINT_LOCAL_STRATEGY,
			config.getString("ReduceTest#LocalStrategy", ""));
		testReducer.getStubParameters().setString(PactCompiler.HINT_SHIP_STRATEGY,
			config.getString("ReduceTest#ShipStrategy", ""));

		DataSinkContract<PactString, PactInteger> output = new DataSinkContract<PactString, PactInteger>(
			ReduceTestOutFormat.class, getHDFSProvider().getHdfsHome() + "/result.txt");
		output.setDegreeOfParallelism(1);

		output.setInput(testReducer);
		testReducer.setInput(input);

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
		StringTokenizer st = new StringTokenizer(REDUCE_RESULT, "\n");
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

		getHDFSProvider().delete(getHDFSProvider().getHdfsHome() + "/result.txt", false);
		getHDFSProvider().delete(getHDFSProvider().getHdfsHome() + "/reduceInput", true);
	}

	@Parameters
	public static Collection<Object[]> getConfigurations() throws FileNotFoundException, IOException {

		LinkedList<Configuration> tConfigs = new LinkedList<Configuration>();

		String[] localStrategies = { PactCompiler.HINT_LOCAL_STRATEGY_SORT };
		String[] shipStrategies = { PactCompiler.HINT_SHIP_STRATEGY_REPARTITION };

		for (String localStrategy : localStrategies) {
			for (String shipStrategy : shipStrategies) {

				Configuration config = new Configuration();
				config.setString("ReduceTest#LocalStrategy", localStrategy);
				config.setString("ReduceTest#ShipStrategy", shipStrategy);
				config.setInteger("ReduceTest#NoSubtasks", 4);
				tConfigs.add(config);
			}
		}

		return toParameterList(ReduceTest.class, tConfigs);
	}
}
