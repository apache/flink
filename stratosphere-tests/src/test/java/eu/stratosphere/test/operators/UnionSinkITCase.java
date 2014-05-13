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

package eu.stratosphere.test.operators;

import java.io.Serializable;
import java.util.Collection;

import eu.stratosphere.test.util.RecordAPITestBase;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.java.record.operators.FileDataSink;
import eu.stratosphere.api.java.record.operators.FileDataSource;
import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.api.java.record.operators.MapOperator;
import eu.stratosphere.compiler.DataStatistics;
import eu.stratosphere.compiler.PactCompiler;
import eu.stratosphere.compiler.plan.OptimizedPlan;
import eu.stratosphere.compiler.plantranslate.NepheleJobGraphGenerator;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.test.operators.io.ContractITCaseIOFormats.ContractITCaseInputFormat;
import eu.stratosphere.test.operators.io.ContractITCaseIOFormats.ContractITCaseOutputFormat;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;

@RunWith(Parameterized.class)
public class UnionSinkITCase extends RecordAPITestBase {
	
	public UnionSinkITCase(Configuration testConfig) {
		super(testConfig);
	}

	private static final String MAP_IN = "1 1\n2 2\n2 8\n4 4\n4 4\n6 6\n7 7\n8 8\n" +
	                                     "1 1\n2 2\n2 2\n4 4\n4 4\n6 3\n5 9\n8 8\n" +
	                                     "1 1\n2 2\n2 2\n3 0\n4 4\n5 9\n7 7\n8 8\n" +
	                                     "1 1\n9 1\n5 9\n4 4\n4 4\n6 6\n7 7\n8 8\n";

	private static final String MAP_RESULT = "1 11\n2 12\n4 14\n4 14\n1 11\n2 12\n2 12\n4 14\n4 14\n3 16\n1 11\n2 12\n2 12\n0 13\n4 14\n1 11\n4 14\n4 14\n";

	private static final String EMPTY_MAP_RESULT = "";
	
	private static final String MAP_RESULT_TWICE = "1 11\n2 12\n4 14\n4 14\n1 11\n2 12\n2 12\n4 14\n4 14\n3 16\n1 11\n2 12\n2 12\n0 13\n4 14\n1 11\n4 14\n4 14\n" +
												"1 11\n2 12\n4 14\n4 14\n1 11\n2 12\n2 12\n4 14\n4 14\n3 16\n1 11\n2 12\n2 12\n0 13\n4 14\n1 11\n4 14\n4 14\n";
	
	private String textInput;
	
	private String emptyInput;
	
	private String resultDir;
	
	@Override
	protected void preSubmit() throws Exception {
		textInput = createTempFile("textdata.txt", MAP_IN);
		emptyInput = createTempFile("emptyfile.txt", "");
		resultDir = getTempDirPath("result");
	}

	public static class TestMapper extends MapFunction implements Serializable {
		private static final long serialVersionUID = 1L;
		
		private StringValue keyString = new StringValue();
		private StringValue valueString = new StringValue();
		
		@Override
		public void map(Record record, Collector<Record> out) throws Exception {
			keyString = record.getField(0, keyString);
			valueString = record.getField(1, valueString);
			
			
			if (Integer.parseInt(keyString.toString()) + Integer.parseInt(valueString.toString()) < 10) {

				record.setField(0, valueString);
				record.setField(1, new IntValue(Integer.parseInt(keyString.toString()) + 10));
				
				out.collect(record);
			}
		}
	}

	@SuppressWarnings({ "deprecation", "unchecked" })
	@Override
	protected JobGraph getJobGraph() throws Exception {
		
		String path1 = config.getBoolean("input1PathHasData", false) ? textInput : emptyInput;
		String path2 = config.getBoolean("input2PathHasData", false) ? textInput : emptyInput;
		
		FileDataSource input1 = new FileDataSource(new ContractITCaseInputFormat(), path1);
		FileDataSource input2 = new FileDataSource(new ContractITCaseInputFormat(), path2);
		
		MapOperator testMapper1 = MapOperator.builder(new TestMapper()).build();
		MapOperator testMapper2 = MapOperator.builder(new TestMapper()).build();

		FileDataSink output = new FileDataSink(new ContractITCaseOutputFormat(), resultDir);

		testMapper1.setInput(input1);
		testMapper2.setInput(input2);

		output.addInput(testMapper1);
		output.addInput(testMapper2);
		
		Plan plan = new Plan(output);
		plan.setDefaultParallelism(4);

		PactCompiler pc = new PactCompiler(new DataStatistics());
		OptimizedPlan op = pc.compile(plan);

		NepheleJobGraphGenerator jgg = new NepheleJobGraphGenerator();
		return jgg.compileJobGraph(op);
	}

	@Override
	protected void postSubmit() throws Exception {
		String expectedResult = config.getString("UnionTest#ExpectedResult", null);
		if (expectedResult == null) {
			throw new Exception("Test corrupt, no expected return data set.");
		}
		compareResultsByLinesInMemory(expectedResult, resultDir);
	}

	@Parameters
	public static Collection<Object[]> getConfigurations() {

		//second input empty
		Configuration config1 = new Configuration();
		config1.setString("UnionTest#ExpectedResult", MAP_RESULT);
		config1.setBoolean("input1PathHasData", true);
		config1.setBoolean("input2PathHasData", false);
		
		
		//first input empty
		Configuration config2 = new Configuration();
		config2.setString("UnionTest#ExpectedResult", MAP_RESULT);
		config2.setBoolean("input1PathHasData", false);
		config2.setBoolean("input2PathHasData", true);
		
		//both inputs full
		Configuration config3 = new Configuration();
		config3.setString("UnionTest#ExpectedResult", MAP_RESULT_TWICE);
		config3.setBoolean("input1PathHasData", true);
		config3.setBoolean("input2PathHasData", true);
		
		//both inputs empty
		Configuration config4 = new Configuration();
		config4.setString("UnionTest#ExpectedResult", EMPTY_MAP_RESULT);
		config4.setBoolean("input1PathHasData", false);
		config4.setBoolean("input2PathHasData", false);

		return toParameterList(config1, config2, config3, config4);
	}
}
