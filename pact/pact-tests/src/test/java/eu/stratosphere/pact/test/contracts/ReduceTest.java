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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;

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

{
	private static final Log LOG = LogFactory.getLog(ReduceTest.class);

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

		String tempDir = getFilesystemProvider().getTempDirPath();

		this.getFilesystemProvider().createDir(tempDir + "/reduceInput");

		this.getFilesystemProvider().createFile(tempDir + "/reduceInput/reduceTest_1.txt", REDUCE_IN_1);
		this.getFilesystemProvider().createFile(tempDir + "/reduceInput/reduceTest_2.txt", REDUCE_IN_2);
		this.getFilesystemProvider().createFile(tempDir + "/reduceInput/reduceTest_3.txt", REDUCE_IN_3);
		this.getFilesystemProvider().createFile(tempDir + "/reduceInput/reduceTest_4.txt", REDUCE_IN_4);
	}

	public static class ReduceTestInFormat extends TextInputFormat<PactString, PactString> {

		@Override
		public boolean readLine(KeyValuePair<PactString, PactString> pair, byte[] line) {

			pair.setKey(new PactString(new String((char) line[0] + "")));
			pair.setValue(new PactString(new String((char) line[2] + "")));

			LOG.debug("Read in: [" + pair.getKey() + "," + pair.getValue() + "]");
			return true;
		}
	}

	public static class ReduceTestOutFormat extends TextOutputFormat<PactString, PactInteger> {

		@Override
		public byte[] writeLine(KeyValuePair<PactString, PactInteger> pair) {
			LOG.debug("Writing out: [" + pair.getKey() + "," + pair.getValue() + "]");

			return (pair.getKey().toString() + " " + pair.getValue().toString() + "\n").getBytes();
		}
	}

	@Combinable
	public static class TestReducer extends ReduceStub<PactString, PactString, PactString, PactInteger> {

		@Override
		public void reduce(PactString key, Iterator<PactString> values, Collector<PactString, PactInteger> out) {

			int sum = 0;
			while (values.hasNext()) {
				PactString v = values.next();
				sum += Integer.parseInt(v.toString());

				LOG.debug("Processed: [" + key + "," + v + "]");
			}
			out.collect(key, new PactInteger(sum));
		}

		@Override
		public void combine(PactString key, Iterator<PactString> values, Collector<PactString, PactString> out) {
			int sum = 0;

			while (values.hasNext()) {
				PactString v = values.next();
				sum += Integer.parseInt(v.toString());

				LOG.debug("Combined: [" + key + "," + v + "]");
			}

			out.collect(key, new PactString(sum + ""));
		}
	}

	@Override
	protected JobGraph getJobGraph() throws Exception {
		String pathPrefix = getFilesystemProvider().getURIPrefix() + getFilesystemProvider().getTempDirPath();

		DataSourceContract<PactString, PactString> input = new DataSourceContract<PactString, PactString>(
				ReduceTestInFormat.class, pathPrefix + "/reduceInput");
		input.setFormatParameter("delimiter", "\n");
		input.setDegreeOfParallelism(config.getInteger("ReduceTest#NoSubtasks", 1));

		ReduceContract<PactString, PactString, PactString, PactInteger> testReducer = new ReduceContract<PactString, PactString, PactString, PactInteger>(
				TestReducer.class);
		testReducer.setDegreeOfParallelism(config.getInteger("ReduceTest#NoSubtasks", 1));
		testReducer.getStubParameters().setString(PactCompiler.HINT_LOCAL_STRATEGY,
				config.getString("ReduceTest#LocalStrategy", ""));
		testReducer.getStubParameters().setString(PactCompiler.HINT_SHIP_STRATEGY,
				config.getString("ReduceTest#ShipStrategy", ""));

		DataSinkContract<PactString, PactInteger> output = new DataSinkContract<PactString, PactInteger>(
				ReduceTestOutFormat.class, pathPrefix + "/result.txt");
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

		String tempDir = getFilesystemProvider().getTempDirPath();

		compareResultsByLinesInMemory(REDUCE_RESULT, tempDir + "/result.txt");
		
		getFilesystemProvider().delete(tempDir + "/result.txt", true);
		getFilesystemProvider().delete(tempDir + "/reduceInput", true);
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
