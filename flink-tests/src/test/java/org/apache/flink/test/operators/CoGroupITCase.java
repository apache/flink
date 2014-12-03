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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.java.record.functions.CoGroupFunction;
import org.apache.flink.api.java.record.io.DelimitedInputFormat;
import org.apache.flink.api.java.record.io.FileOutputFormat;
import org.apache.flink.api.java.record.operators.CoGroupOperator;
import org.apache.flink.api.java.record.operators.FileDataSink;
import org.apache.flink.api.java.record.operators.FileDataSource;
import org.apache.flink.compiler.PactCompiler;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.util.RecordAPITestBase;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.Record;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.Collector;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@SuppressWarnings("deprecation")
@RunWith(Parameterized.class)
public class CoGroupITCase extends RecordAPITestBase {

	String leftInPath = null;
	String rightInPath = null;
	String resultPath = null;

	public CoGroupITCase(Configuration testConfig) {
		super(testConfig);
	}

	private static final String LEFT_IN = "1 1\n2 2\n3 3\n4 4\n1 2\n2 3\n3 4\n4 5\n" +
			"1 3\n2 4\n3 5\n4 6\n1 4\n2 5\n3 6\n4 7\n";

	private static final String RIGHT_IN = "1 1\n2 2\n3 3\n5 1\n1 1\n2 2\n3 3\n6 1\n" +
			"1 1\n2 2\n2 2\n7 1\n1 1\n2 2\n2 2\n8 1\n";

	private static final String RESULT = "1 6\n2 2\n3 12\n4 22\n5 -1\n6 -1\n7 -1\n8 -1\n";

	@Override
	protected void preSubmit() throws Exception {
		leftInPath = createTempFile("left_in.txt", LEFT_IN);
		rightInPath = createTempFile("right_in.txt", RIGHT_IN);
		resultPath = getTempDirPath("result");
	}

	public static class CoGroupTestInFormat extends DelimitedInputFormat {
		private static final long serialVersionUID = 1L;
		
		private final StringValue keyString = new StringValue();
		private final StringValue valueString = new StringValue();
		
		@Override
		public Record readRecord(Record target, byte[] bytes, int offset, int numBytes) {
			this.keyString.setValueAscii(bytes, offset, 1);
			this.valueString.setValueAscii(bytes, offset + 2, 1);
			target.setField(0, keyString);
			target.setField(1, valueString);

			return target;
		}

	}

	public static class CoGroupOutFormat extends FileOutputFormat {
		private static final long serialVersionUID = 1L;
		
		private final StringBuilder buffer = new StringBuilder();
		private final StringValue keyString = new StringValue();
		private final IntValue valueInteger = new IntValue();
		
		@Override
		public void writeRecord(Record record) throws IOException {
			this.buffer.setLength(0);
			this.buffer.append(record.getField(0, keyString).toString());
			this.buffer.append(' ');
			this.buffer.append(record.getField(1, valueInteger).getValue());
			this.buffer.append('\n');
			
			byte[] bytes = this.buffer.toString().getBytes();
			this.stream.write(bytes);
		}
	}

	public static class TestCoGrouper extends CoGroupFunction implements Serializable {
		private static final long serialVersionUID = 1L;

		private StringValue keyString = new StringValue();
		private StringValue valueString = new StringValue();
		
		@Override
		public void coGroup(Iterator<Record> records1, Iterator<Record> records2, Collector<Record> out) {
			
			Record record = null;
			int sum = 0;
			
			while (records1.hasNext()) {
				record = records1.next();
				keyString = record.getField(0, keyString);
				valueString = record.getField(1, valueString);
				sum += Integer.parseInt(valueString.getValue());
			}
			
			
			while (records2.hasNext()) {
				record = records2.next();
				keyString = record.getField(0, keyString);
				valueString = record.getField(1, valueString);
				sum -= Integer.parseInt(valueString.getValue());
			}
			record.setField(1, new IntValue(sum));
			
			out.collect(record);
		}

	}

	@Override
	protected Plan getTestJob() {
		FileDataSource input_left =  new FileDataSource(new CoGroupTestInFormat(), leftInPath);
		DelimitedInputFormat.configureDelimitedFormat(input_left)
			.recordDelimiter('\n');
		input_left.setDegreeOfParallelism(config.getInteger("CoGroupTest#NoSubtasks", 1));

		FileDataSource input_right =  new FileDataSource(new CoGroupTestInFormat(), rightInPath);
		DelimitedInputFormat.configureDelimitedFormat(input_right)
			.recordDelimiter('\n');
		input_right.setDegreeOfParallelism(config.getInteger("CoGroupTest#NoSubtasks", 1));

		CoGroupOperator testCoGrouper = CoGroupOperator.builder(new TestCoGrouper(), StringValue.class, 0, 0)
			.build();
		testCoGrouper.setDegreeOfParallelism(config.getInteger("CoGroupTest#NoSubtasks", 1));
		testCoGrouper.getParameters().setString(PactCompiler.HINT_LOCAL_STRATEGY,
				config.getString("CoGroupTest#LocalStrategy", ""));
		testCoGrouper.getParameters().setString(PactCompiler.HINT_SHIP_STRATEGY,
				config.getString("CoGroupTest#ShipStrategy", ""));

		FileDataSink output = new FileDataSink(new CoGroupOutFormat(), resultPath);
		output.setDegreeOfParallelism(1);

		output.setInput(testCoGrouper);
		testCoGrouper.setFirstInput(input_left);
		testCoGrouper.setSecondInput(input_right);

		return new Plan(output);
	}

	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(RESULT, resultPath);
	}

	@Parameters
	public static Collection<Object[]> getConfigurations() throws FileNotFoundException, IOException {

		LinkedList<Configuration> tConfigs = new LinkedList<Configuration>();

		String[] localStrategies = { PactCompiler.HINT_LOCAL_STRATEGY_SORT_BOTH_MERGE };

		String[] shipStrategies = { PactCompiler.HINT_SHIP_STRATEGY_REPARTITION_HASH };

		for (String localStrategy : localStrategies) {
			for (String shipStrategy : shipStrategies) {

				Configuration config = new Configuration();
				config.setString("CoGroupTest#LocalStrategy", localStrategy);
				config.setString("CoGroupTest#ShipStrategy", shipStrategy);
				config.setInteger("CoGroupTest#NoSubtasks", 4);

				tConfigs.add(config);
			}
		}

		return toParameterList(tConfigs);
	}
}
