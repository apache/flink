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
import java.util.LinkedList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.pact.common.contract.FileDataSinkContract;
import eu.stratosphere.pact.common.contract.FileDataSourceContract;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.io.input.TextInputFormat;
import eu.stratosphere.pact.common.io.output.TextOutputFormat;
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
public class MapITCase extends TestBase

{
	private static final Log LOG = LogFactory.getLog(MapITCase.class);
	
	public MapITCase(String clusterConfig, Configuration testConfig) {
		super(testConfig, clusterConfig);
	}

	private static final String MAP_IN_1 = "1 1\n2 2\n2 8\n4 4\n4 4\n6 6\n7 7\n8 8\n";

	private static final String MAP_IN_2 = "1 1\n2 2\n2 2\n4 4\n4 4\n6 3\n5 9\n8 8\n";

	private static final String MAP_IN_3 = "1 1\n2 2\n2 2\n3 0\n4 4\n5 9\n7 7\n8 8\n";

	private static final String MAP_IN_4 = "1 1\n9 1\n5 9\n4 4\n4 4\n6 6\n7 7\n8 8\n";

	private static final String MAP_RESULT = "1 11\n2 12\n4 14\n4 14\n1 11\n2 12\n2 12\n4 14\n4 14\n3 16\n1 11\n2 12\n2 12\n0 13\n4 14\n1 11\n4 14\n4 14\n";

	@Override
	protected void preSubmit() throws Exception {
		String tempDir = getFilesystemProvider().getTempDirPath();
		
		getFilesystemProvider().createDir(tempDir + "/mapInput");
		
		getFilesystemProvider().createFile(tempDir+"/mapInput/mapTest_1.txt", MAP_IN_1);
		getFilesystemProvider().createFile(tempDir+"/mapInput/mapTest_2.txt", MAP_IN_2);
		getFilesystemProvider().createFile(tempDir+"/mapInput/mapTest_3.txt", MAP_IN_3);
		getFilesystemProvider().createFile(tempDir+"/mapInput/mapTest_4.txt", MAP_IN_4);
	}

	public static class MapTestInFormat extends TextInputFormat<PactString, PactString> {

		@Override
		public boolean readLine(KeyValuePair<PactString, PactString> pair, byte[] line) {

			pair.setKey(new PactString(new String((char) line[0] + "")));
			pair.setValue(new PactString(new String((char) line[2] + "")));

			LOG.debug("Read in: [" + pair.getKey() + "," + pair.getValue() + "]");

			return true;
		}

	}

	public static class MapTestOutFormat extends TextOutputFormat<PactString, PactInteger> {

		@Override
		public byte[] writeLine(KeyValuePair<PactString, PactInteger> pair) {
			LOG.debug("Writing out: [" + pair.getKey() + "," + pair.getValue() + "]");

			return (pair.getKey().toString() + " " + pair.getValue().toString() + "\n").getBytes();
		}
	}

	public static class TestMapper extends MapStub<PactString, PactString, PactString, PactInteger> {

		@Override
		public void map(PactString key, PactString value, Collector<PactString, PactInteger> out) {
			
			LOG.debug("Processed: [" + key + "," + value + "]");
			
			if (Integer.parseInt(key.toString()) + Integer.parseInt(value.toString()) < 10) {

				out.collect(value, new PactInteger(Integer.parseInt(key.toString()) + 10));
				
			}
		}
	}

	@Override
	protected JobGraph getJobGraph() throws Exception {
		String pathPrefix = getFilesystemProvider().getURIPrefix()+getFilesystemProvider().getTempDirPath();
		
		FileDataSourceContract<PactString, PactString> input = new FileDataSourceContract<PactString, PactString>(
			MapTestInFormat.class, pathPrefix+"/mapInput");
		input.setParameter(TextInputFormat.RECORD_DELIMITER, "\n");
		input.setDegreeOfParallelism(config.getInteger("MapTest#NoSubtasks", 1));

		MapContract<PactString, PactString, PactString, PactInteger> testMapper = new MapContract<PactString, PactString, PactString, PactInteger>(
			TestMapper.class);
		testMapper.setDegreeOfParallelism(config.getInteger("MapTest#NoSubtasks", 1));

		FileDataSinkContract<PactString, PactInteger> output = new FileDataSinkContract<PactString, PactInteger>(
			MapTestOutFormat.class, pathPrefix + "/result.txt");
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
		String tempDir = getFilesystemProvider().getTempDirPath();
		
		compareResultsByLinesInMemory(MAP_RESULT, tempDir+ "/result.txt");
		
		getFilesystemProvider().delete(tempDir+ "/result.txt", true);
		getFilesystemProvider().delete(tempDir+ "/mapInput", true);
		
	}

	@Parameters
	public static Collection<Object[]> getConfigurations() throws FileNotFoundException, IOException {
		LinkedList<Configuration> testConfigs = new LinkedList<Configuration>();

		Configuration config = new Configuration();
		config.setInteger("MapTest#NoSubtasks", 4);
		testConfigs.add(config);

		return toParameterList(MapITCase.class, testConfigs);
	}
}
