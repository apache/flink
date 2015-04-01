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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.Program;
import org.apache.flink.api.common.distributions.UniformIntegerDistribution;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.Ordering;
import org.apache.flink.api.java.record.io.CsvInputFormat;
import org.apache.flink.api.java.record.io.CsvOutputFormat;
import org.apache.flink.api.java.record.operators.FileDataSink;
import org.apache.flink.api.java.record.operators.FileDataSource;
import org.apache.flink.test.util.RecordAPITestBase;
import org.apache.flink.types.IntValue;

@SuppressWarnings("deprecation")
public class GlobalSortingITCase extends RecordAPITestBase {
	
	private static final int NUM_RECORDS = 100000;
	
	private String recordsPath;
	private String resultPath;

	private String sortedRecords;

	public GlobalSortingITCase(){
		setTaskManagerNumSlots(parallelism);
	}

	@Override
	protected void preSubmit() throws Exception {
		
		ArrayList<Integer> records = new ArrayList<Integer>();
		
		//Generate records
		Random rnd = new Random(1988);
		
		StringBuilder sb = new StringBuilder(NUM_RECORDS * 7);
		
		for (int i = 0; i < NUM_RECORDS; i++) {
			int number = rnd.nextInt();
			
			records.add(number);
			
			sb.append(number);
			sb.append('\n');
		}
		
		recordsPath = createTempFile("records", sb.toString());
		resultPath = getTempDirPath("result");
		
		
		// create the expected sorted result
		Collections.sort(records);
		sb.setLength(0);
		
		for (Integer i : records) {
			sb.append(i.intValue());
			sb.append('\n');
		}
		
		this.sortedRecords = sb.toString();
	}

	@Override
	protected Plan getTestJob() {
		GlobalSort globalSort = new GlobalSort();
		return globalSort.getPlan(Integer.valueOf(parallelism).toString(), recordsPath, resultPath);
	}

	@Override
	protected void postSubmit() throws Exception {
		// Test results
		compareResultsByLinesInMemoryWithStrictOrder(this.sortedRecords, this.resultPath);
	}
	
	
	private static class GlobalSort implements Program {
		
		private static final long serialVersionUID = 1L;

		@Override
		public Plan getPlan(String... args) throws IllegalArgumentException {
			// parse program parameters
			int numSubtasks       = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
			String recordsPath    = (args.length > 1 ? args[1] : "");
			String output        = (args.length > 2 ? args[2] : "");
			
			FileDataSource source = new FileDataSource(CsvInputFormat.class, recordsPath);
			source.setParallelism(numSubtasks);
			CsvInputFormat.configureRecordFormat(source)
				.recordDelimiter('\n')
				.fieldDelimiter('|')
				.field(IntValue.class, 0);
			
			FileDataSink sink =
				new FileDataSink(CsvOutputFormat.class, output);
			sink.setParallelism(numSubtasks);
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
