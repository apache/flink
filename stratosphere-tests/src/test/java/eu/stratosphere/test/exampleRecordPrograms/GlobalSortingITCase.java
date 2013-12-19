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

package eu.stratosphere.test.exampleRecordPrograms;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.Program;
import eu.stratosphere.api.common.distributions.UniformIntegerDistribution;
import eu.stratosphere.api.common.operators.FileDataSink;
import eu.stratosphere.api.common.operators.FileDataSource;
import eu.stratosphere.api.common.operators.Order;
import eu.stratosphere.api.common.operators.Ordering;
import eu.stratosphere.api.record.io.CsvInputFormat;
import eu.stratosphere.api.record.io.CsvOutputFormat;
import eu.stratosphere.compiler.DataStatistics;
import eu.stratosphere.compiler.PactCompiler;
import eu.stratosphere.compiler.plan.OptimizedPlan;
import eu.stratosphere.compiler.plantranslate.NepheleJobGraphGenerator;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.test.util.TestBase;
import eu.stratosphere.types.IntValue;

@RunWith(Parameterized.class)
public class GlobalSortingITCase extends TestBase {

	private static final Log LOG = LogFactory.getLog(GlobalSortingITCase.class);
	
	private String recordsPath = null;
	private String resultPath = null;

	private ArrayList<Integer> records;

	public GlobalSortingITCase(Configuration config) {
		super(config);
	}

	@Override
	protected void preSubmit() throws Exception {
		
		recordsPath = getFilesystemProvider().getTempDirPath() + "/records";
		resultPath = getFilesystemProvider().getTempDirPath() + "/result";
		
		records = new ArrayList<Integer>();
		
		//Generate records
		Random rnd = new Random(1988);
		int numRecordsPerSplit = 1000;
		
		getFilesystemProvider().createDir(recordsPath);
		int numSplits = 4;
		for (int i = 0; i < numSplits; i++) {
			StringBuilder sb = new StringBuilder(numSplits*2);
			for (int j = 0; j < numRecordsPerSplit; j++) {
				int number = rnd.nextInt();
				records.add(number);
				sb.append(number);
				sb.append('\n');
			}
			getFilesystemProvider().createFile(recordsPath + "/part_" + i + ".txt", sb.toString());
			
			if (LOG.isDebugEnabled())
				LOG.debug("Records Part " + (i + 1) + ":\n>" + sb.toString() + "<");
		}

	}

	@Override
	protected JobGraph getJobGraph() throws Exception {

		GlobalSort globalSort = new GlobalSort();
		Plan plan = globalSort.getPlan(
				config.getString("GlobalSortingTest#NoSubtasks", "1"), 
				getFilesystemProvider().getURIPrefix()+recordsPath,
				getFilesystemProvider().getURIPrefix()+resultPath);

		PactCompiler pc = new PactCompiler(new DataStatistics());
		OptimizedPlan op = pc.compile(plan);

		NepheleJobGraphGenerator jgg = new NepheleJobGraphGenerator();
		return jgg.compileJobGraph(op);
	}

	@Override
	protected void postSubmit() throws Exception {
		//Construct expected result
		Collections.sort(this.records);
		
		// Test results
		compareResultsByLinesInMemoryStrictOrder(this.records, this.resultPath);

	}
	
	@Override
	public void stopCluster() throws Exception {
		getFilesystemProvider().delete(recordsPath, true);
		getFilesystemProvider().delete(resultPath, true);
		super.stopCluster();
	}
	

	@Parameters
	public static Collection<Object[]> getConfigurations() {

		LinkedList<Configuration> tConfigs = new LinkedList<Configuration>();

		Configuration config = new Configuration();
		config.setInteger("GlobalSortingTest#NoSubtasks", 4);
		tConfigs.add(config);

		return toParameterList(tConfigs);
	}
	
	private static class GlobalSort implements Program {
		
		@Override
		public Plan getPlan(String... args) throws IllegalArgumentException {
			// parse program parameters
			int numSubtasks       = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
			String recordsPath    = (args.length > 1 ? args[1] : "");
			String output        = (args.length > 2 ? args[2] : "");
			
			FileDataSource source = new FileDataSource(CsvInputFormat.class, recordsPath);
			source.setDegreeOfParallelism(numSubtasks);
			CsvInputFormat.configureRecordFormat(source)
				.recordDelimiter('\n')
				.fieldDelimiter('|')
				.field(IntValue.class, 0);
			
			FileDataSink sink =
				new FileDataSink(CsvOutputFormat.class, output);
			sink.setDegreeOfParallelism(numSubtasks);
			CsvOutputFormat.configureRecordFormat(sink)
				.recordDelimiter('\n')
				.fieldDelimiter('|')
				.lenient(true)
				.field(IntValue.class, 0);
			
			sink.setGlobalOrder(new Ordering(0, IntValue.class, Order.ASCENDING), new UniformIntegerDistribution(Integer.MIN_VALUE, Integer.MAX_VALUE));
			sink.setInput(source);
			
			return new Plan(sink);
		}
		
	}
}
