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
import eu.stratosphere.pact.common.contract.CoGroupContract;
import eu.stratosphere.pact.common.contract.FileDataSinkContract;
import eu.stratosphere.pact.common.contract.FileDataSourceContract;
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
public class CoGroupITCase extends TestBase

{
	private static final Log LOG = LogFactory.getLog(CoGroupITCase.class);

	public CoGroupITCase(String clusterConfig, Configuration testConfig) {
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
		String tempPath = getFilesystemProvider().getTempDirPath();

		getFilesystemProvider().createDir(tempPath + "/cogroup_left");

		getFilesystemProvider().createFile(tempPath + "/cogroup_left/cogroupTest_1.txt", COGROUP_LEFT_IN_1);
		getFilesystemProvider().createFile(tempPath + "/cogroup_left/cogroupTest_2.txt", COGROUP_LEFT_IN_2);
		getFilesystemProvider().createFile(tempPath + "/cogroup_left/cogroupTest_3.txt", COGROUP_LEFT_IN_3);
		getFilesystemProvider().createFile(tempPath + "/cogroup_left/cogroupTest_4.txt", COGROUP_LEFT_IN_4);

		getFilesystemProvider().createDir(tempPath + "/cogroup_right");

		getFilesystemProvider().createFile(tempPath + "/cogroup_right/cogroupTest_1.txt", COGROUP_RIGHT_IN_1);
		getFilesystemProvider().createFile(tempPath + "/cogroup_right/cogroupTest_2.txt", COGROUP_RIGHT_IN_2);
		getFilesystemProvider().createFile(tempPath + "/cogroup_right/cogroupTest_3.txt", COGROUP_RIGHT_IN_3);
		getFilesystemProvider().createFile(tempPath + "/cogroup_right/cogroupTest_4.txt", COGROUP_RIGHT_IN_4);
	}

	public static class CoGroupTestInFormat extends TextInputFormat<PactString, PactString> {

		@Override
		public boolean readLine(KeyValuePair<PactString, PactString> pair, byte[] line) {

			pair.setKey(new PactString(new String((char) line[0] + "")));
			pair.setValue(new PactString(new String((char) line[2] + "")));

			LOG.debug("Read in: [" + pair.getKey() + "," + pair.getValue() + "]");

			return true;
		}

	}

	public static class CoGroupOutFormat extends TextOutputFormat<PactString, PactInteger> {

		@Override
		public byte[] writeLine(KeyValuePair<PactString, PactInteger> pair) {

			LOG.debug("Writing out: [" + pair.getKey() + "," + pair.getValue() + "]");

			return (pair.getKey().toString() + " " + pair.getValue().toString() + "\n").getBytes();
		}
	}

	public static class TestCoGrouper extends CoGroupStub<PactString, PactString, PactString, PactString, PactInteger> {

		@Override
		public void coGroup(PactString key, Iterator<PactString> values1, Iterator<PactString> values2,
				Collector<PactString, PactInteger> out) {
			int sum = 0;
			LOG.debug("Start iterating over input1");
			while (values1.hasNext()) {
				PactString value = values1.next();
				sum += Integer.parseInt(value.toString());

				LOG.debug("Processed: [" + key + "," + value + "]");
			}
			LOG.debug("Start iterating over input2");
			while (values2.hasNext()) {
				PactString value = values2.next();
				sum -= Integer.parseInt(value.toString());

				LOG.debug("Processed: [" + key + "," + value + "]");
			}
			out.collect(key, new PactInteger(sum));
			LOG.debug("Finished");
		}

	}

	@Override
	protected JobGraph getJobGraph() throws Exception {

		String pathPrefix = getFilesystemProvider().getURIPrefix() + getFilesystemProvider().getTempDirPath();

		FileDataSourceContract<PactString, PactString> input_left = new FileDataSourceContract<PactString, PactString>(
				CoGroupTestInFormat.class, pathPrefix + "/cogroup_left");
		input_left.setParameter(TextInputFormat.RECORD_DELIMITER, "\n");
		input_left.setDegreeOfParallelism(config.getInteger("CoGroupTest#NoSubtasks", 1));

		FileDataSourceContract<PactString, PactString> input_right = new FileDataSourceContract<PactString, PactString>(
				CoGroupTestInFormat.class, pathPrefix + "/cogroup_right");
		input_right.setParameter(TextInputFormat.RECORD_DELIMITER, "\n");
		input_right.setDegreeOfParallelism(config.getInteger("CoGroupTest#NoSubtasks", 1));

		CoGroupContract<PactString, PactString, PactString, PactString, PactInteger> testCoGrouper = new CoGroupContract<PactString, PactString, PactString, PactString, PactInteger>(
				TestCoGrouper.class);
		testCoGrouper.setDegreeOfParallelism(config.getInteger("CoGroupTest#NoSubtasks", 1));
		testCoGrouper.getParameters().setString(PactCompiler.HINT_LOCAL_STRATEGY,
				config.getString("CoGroupTest#LocalStrategy", ""));
		testCoGrouper.getParameters().setString(PactCompiler.HINT_SHIP_STRATEGY,
				config.getString("CoGroupTest#ShipStrategy", ""));

		FileDataSinkContract<PactString, PactInteger> output = new FileDataSinkContract<PactString, PactInteger>(
				CoGroupOutFormat.class, pathPrefix + "/result.txt");
		output.setDegreeOfParallelism(1);

		output.addInput(testCoGrouper);
		testCoGrouper.addFirstInput(input_left);
		testCoGrouper.addSecondInput(input_right);

		Plan plan = new Plan(output);

		PactCompiler pc = new PactCompiler();
		OptimizedPlan op = pc.compile(plan);

		JobGraphGenerator jgg = new JobGraphGenerator();
		return jgg.compileJobGraph(op);
	}

	@Override
	protected void postSubmit() throws Exception {

		String tempPath = getFilesystemProvider().getTempDirPath();

		compareResultsByLinesInMemory(COGROUP_RESULT, tempPath + "/result.txt");
				
		getFilesystemProvider().delete(tempPath + "/result.txt", true);
		getFilesystemProvider().delete(tempPath + "/cogroup_left", true);
		getFilesystemProvider().delete(tempPath + "/cogroup_right", true);
	}

	@Parameters
	public static Collection<Object[]> getConfigurations() throws FileNotFoundException, IOException {

		LinkedList<Configuration> tConfigs = new LinkedList<Configuration>();

		String[] localStrategies = { PactCompiler.HINT_LOCAL_STRATEGY_SORT_BOTH_MERGE};

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

		return toParameterList(CoGroupITCase.class, tConfigs);
	}
}
