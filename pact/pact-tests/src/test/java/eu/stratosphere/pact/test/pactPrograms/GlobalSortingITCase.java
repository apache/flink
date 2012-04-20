///***********************************************************************************************************************
// *
// * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
// *
// * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
// * the License. You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// * specific language governing permissions and limitations under the License.
// *
// **********************************************************************************************************************/
//
//package eu.stratosphere.pact.test.pactPrograms;
//
//import java.util.ArrayList;
//import java.util.Collection;
//import java.util.Collections;
//import java.util.LinkedList;
//import java.util.Random;
//
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
//import org.junit.runner.RunWith;
//import org.junit.runners.Parameterized;
//import org.junit.runners.Parameterized.Parameters;
//
//import eu.stratosphere.nephele.configuration.Configuration;
//import eu.stratosphere.nephele.jobgraph.JobGraph;
//import eu.stratosphere.pact.common.contract.FileDataSink;
//import eu.stratosphere.pact.common.contract.FileDataSource;
//import eu.stratosphere.pact.common.contract.Order;
//import eu.stratosphere.pact.common.io.DelimitedInputFormat;
//import eu.stratosphere.pact.common.io.DelimitedOutputFormat;
//import eu.stratosphere.pact.common.plan.Plan;
//import eu.stratosphere.pact.common.plan.PlanAssembler;
//import eu.stratosphere.pact.common.type.PactRecord;
//import eu.stratosphere.pact.common.type.base.PactInteger;
//import eu.stratosphere.pact.compiler.PactCompiler;
//import eu.stratosphere.pact.compiler.jobgen.JobGraphGenerator;
//import eu.stratosphere.pact.compiler.plan.OptimizedPlan;
//import eu.stratosphere.pact.test.util.TestBase;
//
//@RunWith(Parameterized.class)
//public class GlobalSortingITCase extends TestBase {
//
//	private static final Log LOG = LogFactory.getLog(GlobalSortingITCase.class);
//	
//	private String recordsPath = null;
//	private String resultPath = null;
//
//	private ArrayList<Integer> records;
//
//	public GlobalSortingITCase(Configuration config) {
//		super(config);
//	}
//
//	@Override
//	protected void preSubmit() throws Exception {
//		
//		recordsPath = getFilesystemProvider().getTempDirPath() + "/records";
//		resultPath = getFilesystemProvider().getTempDirPath() + "/result";
//		
//		records = new ArrayList<Integer>();
//		
//		//Generate records
//		Random rnd = new Random(1988);
//		int numRecordsPerSplit = 1000;
//		
//		getFilesystemProvider().createDir(recordsPath);
//		int numSplits = 4;
//		for (int i = 0; i < numSplits; i++) {
//			StringBuilder sb = new StringBuilder(numSplits*2);
//			for (int j = 0; j < numRecordsPerSplit; j++) {
//				int number = rnd.nextInt();
//				records.add(number);
//				sb.append(number);
//				sb.append('\n');
//			}
//			getFilesystemProvider().createFile(recordsPath + "/part_" + i + ".txt", sb.toString());
//			LOG.debug("Records Part " + (i + 1) + ":\n>" + sb.toString() + "<");
//		}
//
//	}
//
//	@Override
//	protected JobGraph getJobGraph() throws Exception {
//
//		GlobalSort globalSort = new GlobalSort();
//		Plan plan = globalSort.getPlan(
//				config.getString("GlobalSortingTest#NoSubtasks", "1"), 
//				getFilesystemProvider().getURIPrefix()+recordsPath,
//				getFilesystemProvider().getURIPrefix()+resultPath);
//
//		PactCompiler pc = new PactCompiler();
//		OptimizedPlan op = pc.compile(plan);
//
//		JobGraphGenerator jgg = new JobGraphGenerator();
//		return jgg.compileJobGraph(op);
//	}
//
//	@Override
//	protected void postSubmit() throws Exception {
//		//Construct expected result
//		Collections.sort(records);
//		StringBuilder expectedResult = new StringBuilder();
//		for (Integer number: records) {
//			expectedResult.append(number);
//			expectedResult.append('\n');
//		}
//		
//		// Test results
//		compareResultsByLinesInMemory(expectedResult.toString(), recordsPath);
//
//	}
//	
//	@Override
//	public void stopCluster() throws Exception {
//		getFilesystemProvider().delete(recordsPath, true);
//		super.stopCluster();
//	}
//	
//
//	@Parameters
//	public static Collection<Object[]> getConfigurations() {
//
//		LinkedList<Configuration> tConfigs = new LinkedList<Configuration>();
//
//		Configuration config = new Configuration();
//		config.setInteger("GlobalSortingTest#NoSubtasks", 4);
//		tConfigs.add(config);
//
//		return toParameterList(tConfigs);
//	}
//	
//	private static class GlobalSort implements PlanAssembler {
//		public static class IntegerInputFormat extends DelimitedInputFormat {
//			@Override
//			public boolean readRecord(PactRecord target, byte[] bytes, int numBytes) {
//				int number = Integer.parseInt(new String(bytes, 0, numBytes));
//				target.setField(0, new PactInteger(number));
//				return true;
//			}
//		}
//		
//		public static class IntegerOutputFormat extends DelimitedOutputFormat {
//			@Override
//			public int serializeRecord(PactRecord rec, byte[] target) throws Exception {
//				PactInteger number = rec.getField(0, PactInteger.class);
//				byte[] bytes = number.toString().getBytes();
//				if(bytes.length <= target.length) {
//					System.arraycopy(bytes, 0, target, 0, bytes.length);
//					return bytes.length;
//				} else {
//					return -1 * bytes.length;
//				}
//			}
//		}
//
//		@Override
//		public Plan getPlan(String... args) throws IllegalArgumentException {
//			// parse program parameters
//			int noSubtasks       = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
//			String recordsPath    = (args.length > 1 ? args[1] : "");
//			String output        = (args.length > 2 ? args[2] : "");
//			
//			FileDataSource source =
//				new FileDataSource(IntegerInputFormat.class, recordsPath);
//			source.setDegreeOfParallelism(noSubtasks);
//			
//			FileDataSink sink =
//				new FileDataSink(IntegerOutputFormat.class, output);
//			sink.setDegreeOfParallelism(noSubtasks);
//			sink.setGlobalOrder(Order.ASCENDING);
//			sink.setInput(source);
//			
//			return new Plan(sink);
//		}
//		
//	}
//}
