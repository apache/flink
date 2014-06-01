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

package eu.stratosphere.test.recordJobTests;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.java.record.operators.FileDataSink;
import eu.stratosphere.api.java.record.functions.JoinFunction;
import eu.stratosphere.api.java.record.io.CsvOutputFormat;
import eu.stratosphere.api.java.record.operators.CollectionDataSource;
import eu.stratosphere.api.java.record.operators.JoinOperator;
import eu.stratosphere.test.testdata.WordCountData;
import eu.stratosphere.test.util.RecordAPITestBase;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;

/**
 * test the collection and iterator data input using join operator
 */
public class CollectionSourceTest extends RecordAPITestBase {

	private static final int DOP = 4;

	protected String resultPath;

	public CollectionSourceTest(){
		setTaskManagerNumSlots(DOP);
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
		plan.setDefaultParallelism(numSubTasks);
		return plan;
	}

	@Override
	protected void preSubmit() throws Exception {
		resultPath = getTempDirPath("result");
	}

	@Override
	protected Plan getTestJob() {
		return getPlan(DOP, resultPath);
	}

	@Override
	protected void postSubmit() throws Exception {
		// Test results
		compareResultsByLinesInMemory(WordCountData.COUNTS, resultPath);
	}
}
