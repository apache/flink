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

package org.apache.flink.runtime.operators;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.runtime.operators.sort.Sorter;
import org.apache.flink.runtime.operators.sort.ExternalSorter;
import org.apache.flink.types.Value;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.runtime.testutils.recordutils.RecordComparator;
import org.apache.flink.runtime.testutils.recordutils.RecordSerializerFactory;
import org.apache.flink.runtime.operators.testutils.DriverTestBase;
import org.apache.flink.runtime.operators.testutils.UniformRecordGenerator;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.Record;
import org.apache.flink.util.Collector;
import org.junit.Test;

public class ReduceTaskExternalITCase extends DriverTestBase<RichGroupReduceFunction<Record, Record>>
{
	private static final Logger LOG = LoggerFactory.getLogger(ReduceTaskExternalITCase.class);
	
	@SuppressWarnings("unchecked")
	private final RecordComparator comparator = new RecordComparator(
		new int[]{0}, (Class<? extends Value>[])new Class[]{ IntValue.class });
	
	private final List<Record> outList = new ArrayList<>();
	
	
	public ReduceTaskExternalITCase(ExecutionConfig config) {
		super(config, 0, 1, 3*1024*1024);
	}
	
	
	@Test
	public void testSingleLevelMergeReduceTask() {
		final int keyCnt = 8192;
		final int valCnt = 8;
		
		setNumFileHandlesForSort(2);
		
		addDriverComparator(this.comparator);
		setOutput(this.outList);
		getTaskConfig().setDriverStrategy(DriverStrategy.SORTED_GROUP_REDUCE);
		
		try {
			addInputSorted(new UniformRecordGenerator(keyCnt, valCnt, false), this.comparator.duplicate());
			
			GroupReduceDriver<Record, Record> testTask = new GroupReduceDriver<>();
			
			testDriver(testTask, MockReduceStub.class);
		} catch (Exception e) {
			LOG.info("Exception while running the test task.", e);
			Assert.fail("Exception in Test: " + e.getMessage());
		}
		
		Assert.assertTrue("Resultset size was "+this.outList.size()+". Expected was "+keyCnt, this.outList.size() == keyCnt);
		
		for(Record record : this.outList) {
			Assert.assertTrue("Incorrect result", record.getField(1, IntValue.class).getValue() == valCnt-record.getField(0, IntValue.class).getValue());
		}
		
		this.outList.clear();
				
	}
	
	@Test
	public void testMultiLevelMergeReduceTask() {
		final int keyCnt = 32768;
		final int valCnt = 8;

		setNumFileHandlesForSort(2);
		
		addDriverComparator(this.comparator);
		setOutput(this.outList);
		getTaskConfig().setDriverStrategy(DriverStrategy.SORTED_GROUP_REDUCE);
		
		try {
			addInputSorted(new UniformRecordGenerator(keyCnt, valCnt, false), this.comparator.duplicate());
			
			GroupReduceDriver<Record, Record> testTask = new GroupReduceDriver<>();
			
			testDriver(testTask, MockReduceStub.class);
		} catch (Exception e) {
			LOG.info("Exception while running the test task.", e);
			Assert.fail("Exception in Test: " + e.getMessage());
		}
		
		Assert.assertTrue("Resultset size was "+this.outList.size()+". Expected was "+keyCnt, this.outList.size() == keyCnt);
		
		for(Record record : this.outList) {
			Assert.assertTrue("Incorrect result", record.getField(1, IntValue.class).getValue() == valCnt-record.getField(0, IntValue.class).getValue());
		}
		
		this.outList.clear();
				
	}
	
	@Test
	public void testSingleLevelMergeCombiningReduceTask() throws IOException {
		final int keyCnt = 8192;
		final int valCnt = 8;
		
		addDriverComparator(this.comparator);
		setOutput(this.outList);
		getTaskConfig().setDriverStrategy(DriverStrategy.SORTED_GROUP_REDUCE);

		Sorter<Record> sorter = null;
		try {
			sorter =
				ExternalSorter.newBuilder(
						getMemoryManager(),
						getContainingTask(),
						RecordSerializerFactory.get().getSerializer(),
						this.comparator.duplicate())
					.maxNumFileHandles(2)
					.withCombiner(new MockCombiningReduceStub())
					.enableSpilling(getIOManager(), 0.8f)
					.memoryFraction(this.perSortFractionMem)
					.objectReuse(true)
					.largeRecords(true)
					.build(new UniformRecordGenerator(keyCnt, valCnt, false));
			addInput(sorter.getIterator());

			GroupReduceDriver<Record, Record> testTask = new GroupReduceDriver<>();
		
			testDriver(testTask, MockCombiningReduceStub.class);
		} catch (Exception e) {
			LOG.info("Exception while running the test task.", e);
			Assert.fail("Invoke method caused exception: " + e.getMessage());
		} finally {
			if (sorter != null) {
				sorter.close();
			}
		}
		
		int expSum = 0;
		for (int i = 1; i < valCnt; i++) {
			expSum += i;
		}
		
		Assert.assertTrue("Resultset size was "+this.outList.size()+". Expected was "+keyCnt, this.outList.size() == keyCnt);
		
		for (Record record : this.outList) {
			Assert.assertTrue("Incorrect result", record.getField(1, IntValue.class).getValue() == expSum-record.getField(0, IntValue.class).getValue());
		}
		
		this.outList.clear();
	}
	
	
	@Test
	public void testMultiLevelMergeCombiningReduceTask() throws IOException {

		int keyCnt = 32768;
		int valCnt = 8;
		
		addDriverComparator(this.comparator);
		setOutput(this.outList);
		getTaskConfig().setDriverStrategy(DriverStrategy.SORTED_GROUP_REDUCE);
		
		Sorter<Record> sorter = null;
		try {
			sorter =
				ExternalSorter.newBuilder(
						getMemoryManager(),
						getContainingTask(),
						RecordSerializerFactory.get().getSerializer(),
						this.comparator.duplicate())
					.maxNumFileHandles(2)
					.withCombiner(new MockCombiningReduceStub())
					.enableSpilling(getIOManager(), 0.8f)
					.memoryFraction(this.perSortFractionMem)
					.objectReuse(false)
					.largeRecords(true)
					.build(new UniformRecordGenerator(keyCnt, valCnt, false));
			addInput(sorter.getIterator());
			
			GroupReduceDriver<Record, Record> testTask = new GroupReduceDriver<>();
		
			testDriver(testTask, MockCombiningReduceStub.class);
		} catch (Exception e) {
			LOG.info("Exception while running the test task.", e);
			Assert.fail("Invoke method caused exception: " + e.getMessage());
		} finally {
			if (sorter != null) {
				sorter.close();
			}
		}
		
		int expSum = 0;
		for (int i = 1; i < valCnt; i++) {
			expSum += i;
		}
		
		Assert.assertTrue("Resultset size was "+this.outList.size()+". Expected was "+keyCnt, this.outList.size() == keyCnt);
		
		for (Record record : this.outList) {
			Assert.assertTrue("Incorrect result", record.getField(1, IntValue.class).getValue() == expSum-record.getField(0, IntValue.class).getValue());
		}
		
		this.outList.clear();
		
	}
	
	public static class MockReduceStub extends RichGroupReduceFunction<Record, Record> {
		private static final long serialVersionUID = 1L;
		
		private final IntValue key = new IntValue();
		private final IntValue value = new IntValue();

		@Override
		public void reduce(Iterable<Record> records, Collector<Record> out) {
			Record element = null;
			int cnt = 0;
			
			for (Record next : records) {
				element = next;
				cnt++;
			}
			element.getField(0, this.key);
			this.value.setValue(cnt - this.key.getValue());
			element.setField(1, this.value);
			out.collect(element);
		}
	}
	
	public static class MockCombiningReduceStub implements
		GroupReduceFunction<Record, Record>, GroupCombineFunction<Record, Record> {

		private static final long serialVersionUID = 1L;

		private final IntValue key = new IntValue();
		private final IntValue value = new IntValue();
		private final IntValue combineValue = new IntValue();

		@Override
		public void reduce(Iterable<Record> records, Collector<Record> out) {
			Record element = null;
			int sum = 0;

			for (Record next : records) {
				element = next;
				element.getField(1, this.value);
				
				sum += this.value.getValue();
			}
			element.getField(0, this.key);
			this.value.setValue(sum - this.key.getValue());
			element.setField(1, this.value);
			out.collect(element);
		}
		
		@Override
		public void combine(Iterable<Record> records, Collector<Record> out) {
			Record element = null;
			int sum = 0;

			for (Record next : records) {
				element = next;
				element.getField(1, this.combineValue);
				
				sum += this.combineValue.getValue();
			}
			
			this.combineValue.setValue(sum);
			element.setField(1, this.combineValue);
			out.collect(element);
		}
	}
}
