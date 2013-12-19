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
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import eu.stratosphere.api.common.functions.GenericReducer;
import eu.stratosphere.api.common.operators.base.ReduceOperatorBase.Combinable;
import eu.stratosphere.api.java.record.functions.ReduceFunction;
import eu.stratosphere.pact.runtime.plugable.pactrecord.RecordComparator;
import eu.stratosphere.pact.runtime.plugable.pactrecord.RecordSerializer;
import eu.stratosphere.pact.runtime.sort.CombiningUnilateralSortMerger;
import eu.stratosphere.pact.runtime.test.util.DelayingInfinitiveInputIterator;
import eu.stratosphere.pact.runtime.test.util.DriverTestBase;
import eu.stratosphere.pact.runtime.test.util.ExpectedTestException;
import eu.stratosphere.pact.runtime.test.util.NirvanaOutputList;
import eu.stratosphere.pact.runtime.test.util.UniformRecordGenerator;
import eu.stratosphere.pact.runtime.test.util.TaskCancelThread;
import eu.stratosphere.types.Key;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;

public class ReduceTaskTest extends DriverTestBase<GenericReducer<Record, Record>>
{
	private static final Log LOG = LogFactory.getLog(ReduceTaskTest.class);
	
	@SuppressWarnings("unchecked")
	private final RecordComparator comparator = new RecordComparator(
		new int[]{0}, (Class<? extends Key>[])new Class[]{ IntValue.class });
	
	private final List<Record> outList = new ArrayList<Record>();

	public ReduceTaskTest() {
		super(0, 1, 3*1024*1024);
	}
	
	@Test
	public void testReduceTaskWithSortingInput() {
		final int keyCnt = 100;
		final int valCnt = 20;
		
		addInputComparator(this.comparator);
		setOutput(this.outList);
		getTaskConfig().setDriverStrategy(DriverStrategy.SORTED_GROUP);
		
		try {
			addInputSorted(new UniformRecordGenerator(keyCnt, valCnt, false), this.comparator.duplicate());
			
			ReduceDriver<Record, Record> testTask = new ReduceDriver<Record, Record>();
			
			testDriver(testTask, MockReduceStub.class);
		} catch (Exception e) {
			LOG.debug(e);
			Assert.fail("Exception in Test.");
		}
		
		Assert.assertTrue("Resultset size was "+this.outList.size()+". Expected was "+keyCnt, this.outList.size() == keyCnt);
		
		for(Record record : this.outList) {
			Assert.assertTrue("Incorrect result", record.getField(1, IntValue.class).getValue() == valCnt-record.getField(0, IntValue.class).getValue());
		}
		
		this.outList.clear();
	}
	
	@Test
	public void testReduceTaskOnPreSortedInput() {
		final int keyCnt = 100;
		final int valCnt = 20;
		
		addInput(new UniformRecordGenerator(keyCnt, valCnt, true));
		addInputComparator(this.comparator);
		setOutput(this.outList);
		getTaskConfig().setDriverStrategy(DriverStrategy.SORTED_GROUP);
		
		ReduceDriver<Record, Record> testTask = new ReduceDriver<Record, Record>();
		
		try {
			testDriver(testTask, MockReduceStub.class);
		} catch (Exception e) {
			LOG.debug(e);
			Assert.fail("Invoke method caused exception.");
		}
		
		Assert.assertTrue("Resultset size was "+this.outList.size()+". Expected was "+keyCnt, this.outList.size() == keyCnt);
		
		for(Record record : this.outList) {
			Assert.assertTrue("Incorrect result", record.getField(1, IntValue.class).getValue() == valCnt-record.getField(0, IntValue.class).getValue());
		}
		
		this.outList.clear();
	}
	
	@Test
	public void testCombiningReduceTask() {
		final int keyCnt = 100;
		final int valCnt = 20;
		
		addInputComparator(this.comparator);
		setOutput(this.outList);
		getTaskConfig().setDriverStrategy(DriverStrategy.SORTED_GROUP);
		
		CombiningUnilateralSortMerger<Record> sorter = null;
		try {
			sorter = new CombiningUnilateralSortMerger<Record>(new MockCombiningReduceStub(), 
				getMemoryManager(), getIOManager(), new UniformRecordGenerator(keyCnt, valCnt, false), 
				getOwningNepheleTask(), RecordSerializer.get(), this.comparator.duplicate(), this.perSortMem, 4, 0.8f);
			addInput(sorter.getIterator());
			
			ReduceDriver<Record, Record> testTask = new ReduceDriver<Record, Record>();
		
			testDriver(testTask, MockCombiningReduceStub.class);
		} catch (Exception e) {
			LOG.debug(e);
			Assert.fail("Invoke method caused exception.");
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
		
		for(Record record : this.outList) {
			Assert.assertTrue("Incorrect result", record.getField(1, IntValue.class).getValue() == expSum-record.getField(0, IntValue.class).getValue());
		}
		
		this.outList.clear();
		
	}
	
	@Test
	public void testFailingReduceTask() {
		final int keyCnt = 100;
		final int valCnt = 20;
		
		addInput(new UniformRecordGenerator(keyCnt, valCnt, true));
		addInputComparator(this.comparator);
		setOutput(this.outList);
		getTaskConfig().setDriverStrategy(DriverStrategy.SORTED_GROUP);
		
		ReduceDriver<Record, Record> testTask = new ReduceDriver<Record, Record>();
		
		try {
			testDriver(testTask, MockFailingReduceStub.class);
			Assert.fail("Function exception was not forwarded.");
		} catch (ExpectedTestException eetex) {
			// Good!
		} catch (Exception e) {
			LOG.debug(e);
			Assert.fail("Test caused exception.");
		}
		
		this.outList.clear();
	}
	
	@Test
	public void testCancelReduceTaskWhileSorting()
	{
		addInputComparator(this.comparator);
		setOutput(new NirvanaOutputList());
		getTaskConfig().setDriverStrategy(DriverStrategy.SORTED_GROUP);
		
		final ReduceDriver<Record, Record> testTask = new ReduceDriver<Record, Record>();
		
		try {
			addInputSorted(new DelayingInfinitiveInputIterator(100), this.comparator.duplicate());
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
		
		final AtomicBoolean success = new AtomicBoolean(false);
		
		Thread taskRunner = new Thread() {
			@Override
			public void run() {
				try {
					testDriver(testTask, MockReduceStub.class);
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
		
		Assert.assertTrue("Test threw an exception even though it was properly canceled.", success.get());
	}
	
	@Test
	public void testCancelReduceTaskWhileReducing() {
		
		final int keyCnt = 1000;
		final int valCnt = 2;
		
		addInput(new UniformRecordGenerator(keyCnt, valCnt, true));
		addInputComparator(this.comparator);
		setOutput(new NirvanaOutputList());
		getTaskConfig().setDriverStrategy(DriverStrategy.SORTED_GROUP);
		
		final ReduceDriver<Record, Record> testTask = new ReduceDriver<Record, Record>();
		
		final AtomicBoolean success = new AtomicBoolean(false);
		
		Thread taskRunner = new Thread() {
			@Override
			public void run() {
				try {
					testDriver(testTask, MockDelayingReduceStub.class);
					success.set(true);
				} catch (Exception ie) {
					ie.printStackTrace();
				}
			}
		};
		taskRunner.start();
		
		TaskCancelThread tct = new TaskCancelThread(2, taskRunner, this);
		tct.start();
		
		try {
			tct.join();
			taskRunner.join();		
		} catch(InterruptedException ie) {
			Assert.fail("Joining threads failed");
		}
		
	}
	
	public static class MockReduceStub extends ReduceFunction {

		private final IntValue key = new IntValue();
		private final IntValue value = new IntValue();

		@Override
		public void reduce(Iterator<Record> records, Collector<Record> out)
				throws Exception {
			Record element = null;
			int cnt = 0;
			while (records.hasNext()) {
				element = records.next();
				cnt++;
			}
			element.getField(0, this.key);
			this.value.setValue(cnt - this.key.getValue());
			element.setField(1, this.value);
			out.collect(element);
		}
	}
	
	@Combinable
	public static class MockCombiningReduceStub extends ReduceFunction {

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
			
			this.combineValue.setValue(sum);
			element.setField(1, this.combineValue);
			out.collect(element);
		}
		
	}
	
	public static class MockFailingReduceStub extends ReduceFunction {

		private int cnt = 0;
		
		private final IntValue key = new IntValue();
		private final IntValue value = new IntValue();

		@Override
		public void reduce(Iterator<Record> records, Collector<Record> out)
				throws Exception {
			Record element = null;
			int valCnt = 0;
			while (records.hasNext()) {
				element = records.next();
				valCnt++;
			}
			
			if (++this.cnt >= 10) {
				throw new ExpectedTestException();
			}
			
			element.getField(0, this.key);
			this.value.setValue(valCnt - this.key.getValue());
			element.setField(1, this.value);
			out.collect(element);
		}
	}
	
	public static class MockDelayingReduceStub extends ReduceFunction {

		@Override
		public void reduce(Iterator<Record> records, Collector<Record> out) {
			while(records.hasNext()) {
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {}
				records.next();
			}
		}
	}
}
