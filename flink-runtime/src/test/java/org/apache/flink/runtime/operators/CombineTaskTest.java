/**
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

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;

import org.apache.flink.api.java.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.record.operators.ReduceOperator.Combinable;
import org.apache.flink.api.java.typeutils.runtime.record.RecordComparator;
import org.apache.flink.runtime.operators.DriverStrategy;
import org.apache.flink.runtime.operators.GroupReduceCombineDriver;
import org.apache.flink.runtime.operators.testutils.DelayingInfinitiveInputIterator;
import org.apache.flink.runtime.operators.testutils.DiscardingOutputCollector;
import org.apache.flink.runtime.operators.testutils.DriverTestBase;
import org.apache.flink.runtime.operators.testutils.ExpectedTestException;
import org.apache.flink.runtime.operators.testutils.TaskCancelThread;
import org.apache.flink.runtime.operators.testutils.UniformRecordGenerator;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.Key;
import org.apache.flink.types.Record;
import org.apache.flink.util.Collector;
import org.junit.Test;

public class CombineTaskTest extends DriverTestBase<RichGroupReduceFunction<Record, ?>>
{
	private static final long COMBINE_MEM = 3 * 1024 * 1024;

	private final double combine_frac;
	
	private final ArrayList<Record> outList = new ArrayList<Record>();
	
	@SuppressWarnings("unchecked")
	private final RecordComparator comparator = new RecordComparator(
		new int[]{0}, (Class<? extends Key<?>>[])new Class[]{ IntValue.class });

	public CombineTaskTest() {
		super(COMBINE_MEM, 0);

		combine_frac = (double)COMBINE_MEM/this.getMemoryManager().getMemorySize();
	}
	
	@Test
	public void testCombineTask() {
		int keyCnt = 100;
		int valCnt = 20;
		
		addInput(new UniformRecordGenerator(keyCnt, valCnt, false));
		addInputComparator(this.comparator);
		setOutput(this.outList);
		
		getTaskConfig().setDriverStrategy(DriverStrategy.SORTED_GROUP_COMBINE);
		getTaskConfig().setRelativeMemoryDriver(combine_frac);
		getTaskConfig().setFilehandlesDriver(2);
		
		final GroupReduceCombineDriver<Record> testTask = new GroupReduceCombineDriver<Record>();
		
		try {
			testDriver(testTask, MockCombiningReduceStub.class);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("Invoke method caused exception.");
		}
		
		int expSum = 0;
		for (int i = 1;i < valCnt; i++) {
			expSum += i;
		}
		
		Assert.assertTrue("Resultset size was "+this.outList.size()+". Expected was "+keyCnt, this.outList.size() == keyCnt);
		
		for(Record record : this.outList) {
			Assert.assertTrue("Incorrect result", record.getField(1, IntValue.class).getValue() == expSum);
		}
		
		this.outList.clear();
	}
	
	@Test
	public void testFailingCombineTask() {
		int keyCnt = 100;
		int valCnt = 20;
		
		addInput(new UniformRecordGenerator(keyCnt, valCnt, false));
		addInputComparator(this.comparator);
		setOutput(new DiscardingOutputCollector<Record>());
		
		getTaskConfig().setDriverStrategy(DriverStrategy.SORTED_GROUP_COMBINE);
		getTaskConfig().setRelativeMemoryDriver(combine_frac);
		getTaskConfig().setFilehandlesDriver(2);
		
		final GroupReduceCombineDriver<Record> testTask = new GroupReduceCombineDriver<Record>();
		
		try {
			testDriver(testTask, MockFailingCombiningReduceStub.class);
			Assert.fail("Exception not forwarded.");
		} catch (ExpectedTestException etex) {
			// good!
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("Test failed due to an exception.");
		}
	}
	
	@Test
	public void testCancelCombineTaskSorting()
	{
		addInput(new DelayingInfinitiveInputIterator(100));
		addInputComparator(this.comparator);
		setOutput(new DiscardingOutputCollector<Record>());
		
		getTaskConfig().setDriverStrategy(DriverStrategy.SORTED_GROUP_COMBINE);
		getTaskConfig().setRelativeMemoryDriver(combine_frac);
		getTaskConfig().setFilehandlesDriver(2);
		
		final GroupReduceCombineDriver<Record> testTask = new GroupReduceCombineDriver<Record>();
		
		final AtomicBoolean success = new AtomicBoolean(false);
		
		Thread taskRunner = new Thread() {
			@Override
			public void run() {
				try {
					testDriver(testTask, MockFailingCombiningReduceStub.class);
					success.set(true);
				} catch (Exception ie) {
					ie.printStackTrace();
				}
			}
		};
		taskRunner.start();
		
		TaskCancelThread tct = new TaskCancelThread(1, taskRunner, this);
		tct.start();
		
		try {
			tct.join();
			taskRunner.join();		
		} catch(InterruptedException ie) {
			Assert.fail("Joining threads failed");
		}
		
		Assert.assertTrue("Exception was thrown despite proper canceling.", success.get());
	}
	
	@Combinable
	public static class MockCombiningReduceStub extends RichGroupReduceFunction<Record, Record> {
		private static final long serialVersionUID = 1L;
		
		private final IntValue theInteger = new IntValue();

		@Override
		public void reduce(Iterable<Record> records, Collector<Record> out) {
			Record element = null;
			int sum = 0;
			
			for (Record next : records) {
				element = next;
				element.getField(1, this.theInteger);
				
				sum += this.theInteger.getValue();
			}
			this.theInteger.setValue(sum);
			element.setField(1, this.theInteger);
			out.collect(element);
		}
		
		@Override
		public void combine(Iterable<Record> records, Collector<Record> out) throws Exception {
			reduce(records, out);
		}
	}
	
	@Combinable
	public static final class MockFailingCombiningReduceStub extends RichGroupReduceFunction<Record, Record> {
		private static final long serialVersionUID = 1L;
		
		private int cnt = 0;
		
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
			
			if (++this.cnt >= 10) {
				throw new ExpectedTestException();
			}
			
			this.combineValue.setValue(sum);
			element.setField(1, this.combineValue);
			out.collect(element);
		}
	}
}
