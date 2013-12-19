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

package eu.stratosphere.test.failingPrograms;

import java.io.FileNotFoundException;
import java.io.IOException;
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
import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.api.java.record.io.DelimitedInputFormat;
import eu.stratosphere.api.java.record.operators.MapOperator;
import eu.stratosphere.compiler.DataStatistics;
import eu.stratosphere.compiler.PactCompiler;
import eu.stratosphere.compiler.plan.OptimizedPlan;
import eu.stratosphere.compiler.plantranslate.NepheleJobGraphGenerator;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.test.operators.io.ContractITCaseIOFormats.ContractITCaseInputFormat;
import eu.stratosphere.test.operators.io.ContractITCaseIOFormats.ContractITCaseOutputFormat;
import eu.stratosphere.test.util.FailingTestBase;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;

/**
 * Tests whether the system recovers from a runtime exception from the PACT user code.
 * 
 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
 *
 */
@RunWith(Parameterized.class)
public class TaskFailureITCase extends FailingTestBase {

	/**
	 * {@inheritDoc}
	 * 
	 * @param clusterConfig
	 * @param testConfig
	 */
	public TaskFailureITCase(String clusterConfig, Configuration testConfig) {
		super(testConfig,clusterConfig);
	}

	// log
	private static final Log LOG = LogFactory.getLog(TaskFailureITCase.class);

	// input for map tasks
	private static final String MAP_IN_1 = "1 1\n2 2\n2 8\n4 4\n4 4\n6 6\n7 7\n8 8\n";
	private static final String MAP_IN_2 = "1 1\n2 2\n2 2\n4 4\n4 4\n6 3\n5 9\n8 8\n";
	private static final String MAP_IN_3 = "1 1\n2 2\n2 2\n3 0\n4 4\n5 9\n7 7\n8 8\n";
	private static final String MAP_IN_4 = "1 1\n9 1\n5 9\n4 4\n4 4\n6 6\n7 7\n8 8\n";

	// expected result of working map job
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


	@Override
	protected JobGraph getFailingJobGraph() throws Exception {

		// path prefix for temp data 
		String pathPrefix = getFilesystemProvider().getURIPrefix()+getFilesystemProvider().getTempDirPath();
		
		// init data source 
		FileDataSource input = new FileDataSource(
			new ContractITCaseInputFormat(), pathPrefix+"/mapInput");
		DelimitedInputFormat.configureDelimitedFormat(input)
			.recordDelimiter('\n');
		input.setDegreeOfParallelism(config.getInteger("MapTest#NoSubtasks", 1));

		// init failing map task
		MapOperator testMapper = MapOperator.builder(FailingMapper.class).build();
		testMapper.setDegreeOfParallelism(config.getInteger("MapTest#NoSubtasks", 1));

		// init data sink
		FileDataSink output = new FileDataSink(
			new ContractITCaseOutputFormat(), pathPrefix + "/result.txt");
		output.setDegreeOfParallelism(1);

		// compose failing program
		output.setInput(testMapper);
		testMapper.setInput(input);

		// generate plan
		Plan plan = new Plan(output);

		// optimize and compile plan 
		PactCompiler pc = new PactCompiler(new DataStatistics());
		OptimizedPlan op = pc.compile(plan);
		
		// return job graph of failing job
		NepheleJobGraphGenerator jgg = new NepheleJobGraphGenerator();
		return jgg.compileJobGraph(op);
	}


	@Override
	protected JobGraph getJobGraph() throws Exception {
		
		// path prefix for temp data 
		String pathPrefix = getFilesystemProvider().getURIPrefix()+getFilesystemProvider().getTempDirPath();
		
		// init data source 
		FileDataSource input = new FileDataSource(
			new ContractITCaseInputFormat(), pathPrefix+"/mapInput");
		DelimitedInputFormat.configureDelimitedFormat(input)
			.recordDelimiter('\n');
		input.setDegreeOfParallelism(config.getInteger("MapTest#NoSubtasks", 1));

		// init (working) map task
		MapOperator testMapper = MapOperator.builder(TestMapper.class).build();
		testMapper.setDegreeOfParallelism(config.getInteger("MapTest#NoSubtasks", 1));

		// init data sink
		FileDataSink output = new FileDataSink(
			new ContractITCaseOutputFormat(), pathPrefix + "/result.txt");
		output.setDegreeOfParallelism(1);

		// compose working program
		output.setInput(testMapper);
		testMapper.setInput(input);

		// generate plan
		Plan plan = new Plan(output);

		// optimize and compile plan
		PactCompiler pc = new PactCompiler(new DataStatistics());
		OptimizedPlan op = pc.compile(plan);

		// return job graph of working job
		NepheleJobGraphGenerator jgg = new NepheleJobGraphGenerator();
		return jgg.compileJobGraph(op);
	}


	@Override
	protected void postSubmit() throws Exception {
		String tempDir = getFilesystemProvider().getTempDirPath();
		
		// check result
		compareResultsByLinesInMemory(MAP_RESULT, tempDir+ "/result.txt");

		// delete temp data
		getFilesystemProvider().delete(tempDir+ "/result.txt", true);
		getFilesystemProvider().delete(tempDir+ "/mapInput", true);
		
	}
	

	@Override
	protected int getTimeout() {
		// time out for this job is 30 secs
		return 30;
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @return
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	@Parameters
	public static Collection<Object[]> getConfigurations() throws FileNotFoundException, IOException {
		LinkedList<Configuration> testConfigs = new LinkedList<Configuration>();

		Configuration config = new Configuration();
		config.setInteger("MapTest#NoSubtasks", 4);
		testConfigs.add(config);

		return toParameterList(TaskFailureITCase.class, testConfigs);
	}

	/**
	 * Map stub implementation for working program
	 * 
	 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
	 *
	 */
	public static class TestMapper extends MapFunction {

		private final StringValue string = new StringValue();
		private final IntValue integer = new IntValue();

		@Override
		public void map(Record record, Collector<Record> out) throws Exception {
			
			final StringValue keyString = record.getField(0, this.string);
			final int key = Integer.parseInt(keyString.toString());
			
			final StringValue valueString = record.getField(1, this.string);
			final int value = Integer.parseInt(valueString.toString());
			
			if (LOG.isDebugEnabled())
					LOG.debug("Processed: [" + key + "," + value + "]");
			
			if (key + value < 10) {
				record.setField(0, valueString);
				this.integer.setValue(key + 10);
				record.setField(1, this.integer);
				out.collect(record);
			}
			
		}
	}
	
	/**
	 * Map stub implementation that fails with a {@link RuntimeException}.
	 * 
	 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
	 *
	 */
	public static class FailingMapper extends MapFunction {

		@Override
		public void map(Record record, Collector<Record> out) throws Exception {
			throw new RuntimeException("This is an expected Test Exception");
		}
		
	}
	
}
