/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.flink.test.operators;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.LinkedList;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.java.record.functions.MapFunction;
import org.apache.flink.api.java.record.io.DelimitedInputFormat;
import org.apache.flink.api.java.record.operators.FileDataSink;
import org.apache.flink.api.java.record.operators.FileDataSource;
import org.apache.flink.api.java.record.operators.MapOperator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.operators.io.ContractITCaseIOFormats.ContractITCaseInputFormat;
import org.apache.flink.test.operators.io.ContractITCaseIOFormats.ContractITCaseOutputFormat;
import org.apache.flink.test.util.RecordAPITestBase;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.Record;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.Collector;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("deprecation")
@RunWith(Parameterized.class)
public class UnionITCase extends RecordAPITestBase {
	private static final Logger LOG = LoggerFactory.getLogger(UnionITCase.class);

	String inPath = null;
	String emptyInPath = null;
	String resultPath = null;
	
	public UnionITCase(Configuration testConfig) {
		super(testConfig);
	}

	private static final String IN = "1 1\n2 2\n2 8\n4 4\n4 4\n6 6\n7 7\n8 8\n1 1\n" +
			"2 2\n2 2\n4 4\n4 4\n6 3\n5 9\n8 8\n1 1\n2 2\n2 2\n3 0\n4 4\n5 9\n7 7\n8 8\n" +
			"1 1\n9 1\n5 9\n4 4\n4 4\n6 6\n7 7\n8 8\n";

	private static final String RESULT = "1 11\n2 12\n4 14\n4 14\n1 11\n2 12\n2 12\n" +
			"4 14\n4 14\n3 16\n1 11\n2 12\n2 12\n0 13\n4 14\n1 11\n4 14\n4 14\n";

	private static final String EMPTY_RESULT = "";
	
	private static final String MAP_RESULT_TWICE = "1 11\n2 12\n4 14\n4 14\n1 11\n2 12\n2 12\n4 14\n4 14\n3 16\n1 11\n2 12\n2 12\n0 13\n4 14\n1 11\n4 14\n4 14\n" +
												"1 11\n2 12\n4 14\n4 14\n1 11\n2 12\n2 12\n4 14\n4 14\n3 16\n1 11\n2 12\n2 12\n0 13\n4 14\n1 11\n4 14\n4 14\n";
	
	@Override
	protected void preSubmit() throws Exception {
		inPath = createTempFile("in.txt", IN);
		emptyInPath = createTempFile("empty_in.txt", "");
		resultPath = getTempDirPath("result");
	}

	public static class TestMapper extends MapFunction implements Serializable {
		private static final long serialVersionUID = 1L;
		
		private StringValue keyString = new StringValue();
		private StringValue valueString = new StringValue();
		
		@Override
		public void map(Record record, Collector<Record> out) throws Exception {
			keyString = record.getField(0, keyString);
			valueString = record.getField(1, valueString);
			
			if (LOG.isDebugEnabled())
				LOG.debug("Processed: [" + keyString.toString() + "," + valueString.getValue() + "]");
			
			if (Integer.parseInt(keyString.toString()) + Integer.parseInt(valueString.toString()) < 10) {

				record.setField(0, valueString);
				record.setField(1, new IntValue(Integer.parseInt(keyString.toString()) + 10));
				
				out.collect(record);
			}
			
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	protected Plan getTestJob() {
		String input1Path = config.getString("UnionTest#Input1Path", "").equals("empty") ? emptyInPath : inPath;
		String input2Path = config.getString("UnionTest#Input2Path", "").equals("empty") ? emptyInPath : inPath;

		FileDataSource input1 = new FileDataSource(
			new ContractITCaseInputFormat(), input1Path);
		DelimitedInputFormat.configureDelimitedFormat(input1)
			.recordDelimiter('\n');
		input1.setDegreeOfParallelism(config.getInteger("UnionTest#NoSubtasks", 1));
		
		FileDataSource input2 = new FileDataSource(
				new ContractITCaseInputFormat(), input2Path);
		DelimitedInputFormat.configureDelimitedFormat(input2)
			.recordDelimiter('\n');
		input2.setDegreeOfParallelism(config.getInteger("UnionTest#NoSubtasks", 1));
		
		MapOperator testMapper = MapOperator.builder(new TestMapper()).build();
		testMapper.setDegreeOfParallelism(config.getInteger("UnionTest#NoSubtasks", 1));

		FileDataSink output = new FileDataSink(
				new ContractITCaseOutputFormat(), resultPath);
		output.setDegreeOfParallelism(1);

		output.setInput(testMapper);

		testMapper.addInput(input1);
		testMapper.addInput(input2);

		return new Plan(output);
	}

	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(config.getString("UnionTest#ExpectedResult", ""), resultPath);

	}

	@Parameters
	public static Collection<Object[]> getConfigurations() throws IOException {
		LinkedList<Configuration> testConfigs = new LinkedList<Configuration>();

		//second input empty
		Configuration config = new Configuration();
		config.setInteger("UnionTest#NoSubtasks", 4);
		config.setString("UnionTest#ExpectedResult", RESULT);
		config.setString("UnionTest#Input1Path", "non-empty");
		config.setString("UnionTest#Input2Path", "empty");
		testConfigs.add(config);
		
		
		//first input empty
		config = new Configuration();
		config.setInteger("UnionTest#NoSubtasks", 4);
		config.setString("UnionTest#ExpectedResult", RESULT);
		config.setString("UnionTest#Input1Path", "empty");
		config.setString("UnionTest#Input2Path", "non-empty");
		testConfigs.add(config);
		
		//both inputs full
		config = new Configuration();
		config.setInteger("UnionTest#NoSubtasks", 4);
		config.setString("UnionTest#ExpectedResult", MAP_RESULT_TWICE);
		config.setString("UnionTest#Input1Path", "non-empty");
		config.setString("UnionTest#Input2Path", "non-empty");
		testConfigs.add(config);
		
		//both inputs empty
		config = new Configuration();
		config.setInteger("UnionTest#NoSubtasks", 4);
		config.setString("UnionTest#ExpectedResult", EMPTY_RESULT);
		config.setString("UnionTest#Input1Path", "empty");
		config.setString("UnionTest#Input2Path", "empty");
		testConfigs.add(config);

		return toParameterList(testConfigs);
	}
}
