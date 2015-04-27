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

package org.apache.flink.test.recordJobTests;

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.Ordering;
import org.apache.flink.api.java.record.functions.ReduceFunction;
import org.apache.flink.api.java.record.io.CsvInputFormat;
import org.apache.flink.api.java.record.io.CsvOutputFormat;
import org.apache.flink.api.java.record.operators.FileDataSink;
import org.apache.flink.api.java.record.operators.FileDataSource;
import org.apache.flink.api.java.record.operators.ReduceOperator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.util.RecordAPITestBase;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.Record;
import org.apache.flink.util.Collector;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
@SuppressWarnings("deprecation")
public class GroupOrderReduceITCase extends RecordAPITestBase {

	private static final String INPUT = "1,3\n" + "2,1\n" + "5,1\n" + "3,1\n" + "1,8\n" + "1,9\n" + 
										"1,2\n" + "2,3\n" + "7,1\n" + "4,2\n" + "2,7\n" + "2,8\n" +
										"1,1\n" + "2,7\n" + "5,4\n" + "4,3\n" + "3,6\n" + "3,7\n" +
										"1,3\n" + "2,4\n" + "7,1\n" + "5,3\n" + "4,5\n" + "4,6\n" +
										"1,4\n" + "3,9\n" + "8,5\n" + "5,3\n" + "5,4\n" + "5,5\n" +
										"1,7\n" + "3,9\n" + "9,3\n" + "6,2\n" + "6,3\n" + "6,4\n" +
										"1,8\n" + "3,8\n" + "8,7\n" + "6,2\n" + "7,2\n" + "7,3\n" +
										"1,1\n" + "3,7\n" + "9,2\n" + "7,1\n" + "8,1\n" + "8,2\n" +
										"1,2\n" + "2,6\n" + "8,7\n" + "7,1\n" + "9,1\n" + "9,1\n" +
										"1,1\n" + "2,5\n" + "9,5\n" + "8,2\n" + "10,2\n" + "10,1\n" +
										"1,1\n" + "2,6\n" + "2,7\n" + "8,3\n" + "11,3\n" + "11,2\n" +
										"1,2\n" + "2,7\n" + "4,2\n" + "9,4\n" + "12,8\n" + "12,3\n" +
										"1,2\n" + "4,8\n" + "1,7\n" + "9,5\n" + "13,9\n" + "13,4\n" +
										"1,3\n" + "4,2\n" + "3,2\n" + "9,6\n" + "14,7\n" + "14,5\n";

	protected String textPath;
	protected String resultPath;

	
	public GroupOrderReduceITCase(Configuration config) {
		super(config);
		setTaskManagerNumSlots(parallelism);
	}

	
	@Override
	protected void preSubmit() throws Exception {
		textPath = createTempFile("pairs.csv", INPUT);
		resultPath = getTempDirPath("result");
	}

	@Override
	protected Plan getTestJob() {
		
		int parallelism = this.config.getInteger("GroupOrderTest#NumSubtasks", 1);
		
		@SuppressWarnings("unchecked")
		CsvInputFormat format = new CsvInputFormat(',', IntValue.class, IntValue.class);
		FileDataSource source = new FileDataSource(format, this.textPath, "Source");
		
		ReduceOperator reducer = ReduceOperator.builder(CheckingReducer.class)
			.keyField(IntValue.class, 0)
			.input(source)
			.name("Ordered Reducer")
			.build();
		reducer.setGroupOrder(new Ordering(1, IntValue.class, Order.ASCENDING));
		
		FileDataSink sink = new FileDataSink(CsvOutputFormat.class, this.resultPath, reducer, "Sink");
		CsvOutputFormat.configureRecordFormat(sink)
			.recordDelimiter('\n')
			.fieldDelimiter(',')
			.field(IntValue.class, 0)
			.field(IntValue.class, 1);
		
		Plan p = new Plan(sink);
		p.setDefaultParallelism(parallelism);
		return p;
	}

	@Override
	protected void postSubmit() throws Exception {
	}

	@Parameters
	public static Collection<Object[]> getConfigurations() {
		Configuration config = new Configuration();
		config.setInteger("GroupOrderTest#NumSubtasks", parallelism);
		return toParameterList(config);
	}
	
	public static final class CheckingReducer extends ReduceFunction implements Serializable {
		
		private static final long serialVersionUID = 1L;

		@Override
		public void reduce(Iterator<Record> records, Collector<Record> out) throws Exception {
			int lastValue = records.next().getField(1, IntValue.class).getValue();
			
			while (records.hasNext()) {
				int nextValue = records.next().getField(1, IntValue.class).getValue();
				
				if (nextValue < lastValue) {
					throw new Exception("Group Order is violated!");
				}
				
				lastValue = nextValue;
			}
		}
	}
}
