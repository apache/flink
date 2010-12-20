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
import eu.stratosphere.pact.common.contract.DataSinkContract;
import eu.stratosphere.pact.common.contract.DataSourceContract;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.io.TextOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
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
public class MapTest extends TestBase

{
	public MapTest(String clusterConfig, Configuration testConfig) {
		super(testConfig, clusterConfig);
	}

	private static final String MAP_IN_1 = "1 1\n2 2\n2 8\n4 4\n4 4\n6 6\n7 7\n8 8\n";

	private static final String MAP_IN_2 = "1 1\n2 2\n2 2\n4 4\n4 4\n6 3\n5 9\n8 8\n";

	private static final String MAP_IN_3 = "1 1\n2 2\n2 2\n3 0\n4 4\n5 9\n7 7\n8 8\n";

	private static final String MAP_IN_4 = "1 1\n9 1\n5 9\n4 4\n4 4\n6 6\n7 7\n8 8\n";

	private static final String MAP_RESULT = "1 11\n2 12\n4 14\n4 14\n1 11\n2 12\n2 12\n4 14\n4 14\n3 16\n1 11\n2 12\n2 12\n0 13\n4 14\n1 11\n4 14\n4 14\n";

	@Override
	protected void preSubmit() throws Exception {
		getHDFSProvider().writeFileToHDFS("mapTest_1.txt", MAP_IN_1);
		getHDFSProvider().writeFileToHDFS("mapTest_2.txt", MAP_IN_2);
		getHDFSProvider().writeFileToHDFS("mapTest_3.txt", MAP_IN_3);
		getHDFSProvider().writeFileToHDFS("mapTest_4.txt", MAP_IN_4);

	}

	public static class MapTestInFormat extends TextInputFormat<PactString, PactString> {

		private static final Log LOG = LogFactory.getLog(MapTestInFormat.class);

		@Override
		public boolean readLine(KeyValuePair<PactString, PactString> pair, byte[] line) {

			pair.setKey(new PactString(new String((char) line[0] + "")));
			pair.setValue(new PactString(new String((char) line[2] + "")));

			LOG.info("Read in: [" + pair.getKey() + "," + pair.getValue() + "]");

			return true;
		}

	}

	public static class MapTestOutFormat extends TextOutputFormat<PactString, PactInteger> {

		private static final Log LOG = LogFactory.getLog(MapTestOutFormat.class);

		@Override
		public byte[] writeLine(KeyValuePair<PactString, PactInteger> pair) {
			LOG.info("Writing out: [" + pair.getKey() + "," + pair.getValue() + "]");

			return (pair.getKey().toString() + " " + pair.getValue().toString() + "\n").getBytes();
		}
	}

	public static class TestMapper extends MapStub<PactString, PactString, PactString, PactInteger> {

		private static final Log LOG = LogFactory.getLog(TestMapper.class);

		public void map(PactString key, PactString value, Collector<PactString, PactInteger> out) {
			if (Integer.parseInt(key.toString()) + Integer.parseInt(value.toString()) < 10) {
				out.collect(value, new PactInteger(Integer.parseInt(key.toString()) + 10));

				LOG.info("Processed: [" + key + "," + value + "]");
			}
		}
	}

	@Override
	protected JobGraph getJobGraph() throws Exception {
		DataSourceContract<PactString, PactString> input = new DataSourceContract<PactString, PactString>(
			MapTestInFormat.class, getHDFSProvider().getHdfsHome());
		input.setFormatParameter("delimiter", "\n");
		input.setDegreeOfParallelism(config.getInteger("MapTest#NoSubtasks", 1));

		MapContract<PactString, PactString, PactString, PactInteger> testMapper = new MapContract<PactString, PactString, PactString, PactInteger>(
			TestMapper.class);
		testMapper.setDegreeOfParallelism(config.getInteger("MapTest#NoSubtasks", 1));

		DataSinkContract<PactString, PactInteger> output = new DataSinkContract<PactString, PactInteger>(
			MapTestOutFormat.class, getHDFSProvider().getHdfsHome() + "/result.txt");
		output.setDegreeOfParallelism(1);

		output.setInput(testMapper);
		testMapper.setInput(input);

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
		StringTokenizer st = new StringTokenizer(MAP_RESULT, "\n");
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
		getHDFSProvider().delete(getHDFSProvider().getHdfsHome() + "/mapTest_1.txt", false);
		getHDFSProvider().delete(getHDFSProvider().getHdfsHome() + "/mapTest_2.txt", false);
		getHDFSProvider().delete(getHDFSProvider().getHdfsHome() + "/mapTest_3.txt", false);
		getHDFSProvider().delete(getHDFSProvider().getHdfsHome() + "/mapTest_4.txt", false);
	}

	@Parameters
	public static Collection<Object[]> getConfigurations() throws FileNotFoundException, IOException {
		LinkedList<Configuration> testConfigs = new LinkedList<Configuration>();

		Configuration config = new Configuration();
		config.setInteger("MapTest#NoSubtasks", 4);
		testConfigs.add(config);

		return toParameterList(MapTest.class, testConfigs);
	}
}
