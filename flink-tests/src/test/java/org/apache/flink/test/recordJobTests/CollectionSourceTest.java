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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.java.record.functions.JoinFunction;
import org.apache.flink.api.java.record.io.CsvOutputFormat;
import org.apache.flink.api.java.record.operators.CollectionDataSource;
import org.apache.flink.api.java.record.operators.FileDataSink;
import org.apache.flink.api.java.record.operators.JoinOperator;
import org.apache.flink.test.testdata.WordCountData;
import org.apache.flink.test.util.RecordAPITestBase;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.Record;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.Collector;

/**
 * test the collection and iterator data input using join operator
 */
@SuppressWarnings("deprecation")
public class CollectionSourceTest extends RecordAPITestBase {

	private static final int parallelism = 4;

	protected String resultPath;

	public CollectionSourceTest(){
		setTaskManagerNumSlots(parallelism);
	}

	public static class Join extends JoinFunction {

		private static final long serialVersionUID = 1L;

		@Override
		public void join(Record value1, Record value2, Collector<Record> out) throws Exception {
			out.collect(new Record(value1.getField(1, StringValue.class), value2.getField(1, IntValue.class)));
		}
	}

	public static class SerializableIteratorTest implements Iterator<List<Object>>, Serializable {

		private static final long serialVersionUID = 1L;

		private final String[] s = WordCountData.COUNTS.split("\n");

		private int pos = 0;

		public void remove() {
			throw new UnsupportedOperationException();
		}

		public List<Object> next() {
			List<Object> tmp = new ArrayList<Object>();
			tmp.add(pos);
			tmp.add(s[pos++].split(" ")[0]);
			return tmp;
		}

		public boolean hasNext() {
			return pos < s.length;
		}
	}

	public Plan getPlan(int numSubTasks, String output) {

		List<Object> tmp = new ArrayList<Object>();
		int pos = 0;
		for (String s : WordCountData.COUNTS.split("\n")) {
			List<Object> tmpInner = new ArrayList<Object>();
			tmpInner.add(pos++);
			tmpInner.add(Integer.parseInt(s.split(" ")[1]));
			tmp.add(tmpInner);
		}

		// test serializable iterator input, the input record is {id, word}
		CollectionDataSource source = new CollectionDataSource(new SerializableIteratorTest(), "test_iterator");
		// test collection input, the input record is {id, count}
		CollectionDataSource source2 = new CollectionDataSource(tmp, "test_collection");

		JoinOperator join = JoinOperator.builder(Join.class, IntValue.class, 0, 0)
			.input1(source).input2(source2).build();

		FileDataSink out = new FileDataSink(new CsvOutputFormat(), output, join, "Collection Join");
		CsvOutputFormat.configureRecordFormat(out)
			.recordDelimiter('\n')
			.fieldDelimiter(' ')
			.field(StringValue.class, 0)
			.field(IntValue.class, 1);

		Plan plan = new Plan(out, "CollectionDataSource");
		plan.setExecutionConfig(new ExecutionConfig());
		plan.setDefaultParallelism(numSubTasks);
		return plan;
	}

	@Override
	protected void preSubmit() throws Exception {
		resultPath = getTempDirPath("result");
	}

	@Override
	protected Plan getTestJob() {
		return getPlan(parallelism, resultPath);
	}

	@Override
	protected void postSubmit() throws Exception {
		// Test results
		compareResultsByLinesInMemory(WordCountData.COUNTS, resultPath);
	}
}
