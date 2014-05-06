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

package eu.stratosphere.pact.runtime.task;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

import junit.framework.Assert;

import org.junit.Test;

import eu.stratosphere.api.common.functions.GenericGroupReduce;
import eu.stratosphere.api.java.record.functions.ReduceFunction;
import eu.stratosphere.api.java.record.operators.ReduceOperator.Combinable;
import eu.stratosphere.pact.runtime.plugable.pactrecord.RecordComparator;
import eu.stratosphere.pact.runtime.test.util.DelayingInfinitiveInputIterator;
import eu.stratosphere.pact.runtime.test.util.DiscardingOutputCollector;
import eu.stratosphere.pact.runtime.test.util.DriverTestBase;
import eu.stratosphere.pact.runtime.test.util.ExpectedTestException;
import eu.stratosphere.pact.runtime.test.util.TaskCancelThread;
import eu.stratosphere.pact.runtime.test.util.UniformRecordGenerator;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Key;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;

public class CombineTaskTest extends DriverTestBase<GenericGroupReduce<Record, ?>>
{
	private static final long COMBINE_MEM = 3 * 1024 * 1024;
	
	private final ArrayList<Record> outList = new ArrayList<Record>();
	
	@SuppressWarnings("unchecked")
	private final RecordComparator comparator = new RecordComparator(
		new int[]{0}, (Class<? extends Key<?>>[])new Class[]{ IntValue.class });

	public CombineTaskTest() {
		super(COMBINE_MEM, 0);
	}
	
	@Test
	public void testCombineTask() {
		int keyCnt = 100;
		int valCnt = 20;
		
		addInput(new UniformRecordGenerator(keyCnt, valCnt, false));
		addInputComparator(this.comparator);
		setOutput(this.outList);
		
		getTaskConfig().setDriverStrategy(DriverStrategy.PARTIAL_GROUP);
		getTaskConfig().setMemoryDriver(COMBINE_MEM);
		getTaskConfig().setFilehandlesDriver(2);
		
		final CombineDriver<Record> testTask = new CombineDriver<Record>();
		
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
		setOutput(new DiscardingOutputCollector());
		
		getTaskConfig().setDriverStrategy(DriverStrategy.PARTIAL_GROUP);
		getTaskConfig().setMemoryDriver(COMBINE_MEM);
		getTaskConfig().setFilehandlesDriver(2);
		
		final CombineDriver<Record> testTask = new CombineDriver<Record>();
		
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
		setOutput(new DiscardingOutputCollector());
		
		getTaskConfig().setDriverStrategy(DriverStrategy.PARTIAL_GROUP);
		getTaskConfig().setMemoryDriver(COMBINE_MEM);
		getTaskConfig().setFilehandlesDriver(2);
		
		final CombineDriver<Record> testTask = new CombineDriver<Record>();
		
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
	public static class MockCombiningReduceStub extends ReduceFunction {
		private static final long serialVersionUID = 1L;
		
		private final IntValue theInteger = new IntValue();

		@Override
		public void reduce(Iterator<Record> records, Collector<Record> out)
				throws Exception {
			Record element = null;
			int sum = 0;
			while (records.hasNext()) {
				element = records.next();
				element.getField(1, this.theInteger);
				
				sum += this.theInteger.getValue();
			}
			this.theInteger.setValue(sum);
			element.setField(1, this.theInteger);
			out.collect(element);
		}
		
		@Override
		public void combine(Iterator<Record> records, Collector<Record> out) throws Exception {
			reduce(records, out);
		}
	}
	
	@Combinable
	public static final class MockFailingCombiningReduceStub extends ReduceFunction {
		private static final long serialVersionUID = 1L;
		
		private int cnt = 0;
		
		private final IntValue key = new IntValue();
		private final IntValue value = new IntValue();
		private final IntValue combineValue = new IntValue();

		@Override
		public void reduce(Iterator<Record> records, Collector<Record> out)
				throws Exception {
			Record element = null;
			int sum = 0;
			while (records.hasNext()) {
				element = records.next();
				element.getField(1, this.value);
				
				sum += this.value.getValue();
			}
			element.getField(0, this.key);
			this.value.setValue(sum - this.key.getValue());
			element.setField(1, this.value);
			out.collect(element);
		}
		
		@Override
		public void combine(Iterator<Record> records, Collector<Record> out)
				throws Exception {
			Record element = null;
			int sum = 0;
			while (records.hasNext()) {
				element = records.next();
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
