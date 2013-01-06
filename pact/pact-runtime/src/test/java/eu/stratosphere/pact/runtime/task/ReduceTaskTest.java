/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
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

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.generic.contract.GenericReduceContract.Combinable;
import eu.stratosphere.pact.generic.stub.GenericReducer;
import eu.stratosphere.pact.runtime.plugable.PactRecordComparator;
import eu.stratosphere.pact.runtime.plugable.PactRecordSerializer;
import eu.stratosphere.pact.runtime.sort.CombiningUnilateralSortMerger;
import eu.stratosphere.pact.runtime.test.util.DelayingInfinitiveInputIterator;
import eu.stratosphere.pact.runtime.test.util.DriverTestBase;
import eu.stratosphere.pact.runtime.test.util.ExpectedTestException;
import eu.stratosphere.pact.runtime.test.util.NirvanaOutputList;
import eu.stratosphere.pact.runtime.test.util.UniformPactRecordGenerator;
import eu.stratosphere.pact.runtime.test.util.TaskCancelThread;

public class ReduceTaskTest extends DriverTestBase<GenericReducer<PactRecord, PactRecord>>
{
	private static final Log LOG = LogFactory.getLog(ReduceTaskTest.class);
	
	@SuppressWarnings("unchecked")
	private final PactRecordComparator comparator = new PactRecordComparator(
		new int[]{0}, (Class<? extends Key>[])new Class[]{ PactInteger.class });
	
	private final List<PactRecord> outList = new ArrayList<PactRecord>();

	public ReduceTaskTest() {
		super(0, 1, 3*1024*1024);
	}
	
	@Test
	public void testReduceTaskWithSortingInput() {
		final int keyCnt = 100;
		final int valCnt = 20;
		
		addInputComparator(this.comparator);
		setOutput(this.outList);
		getTaskConfig().setDriverStrategy(DriverStrategy.GROUP_OVER_ORDERED);
		
		try {
			addInputSorted(new UniformPactRecordGenerator(keyCnt, valCnt, false), this.comparator.duplicate());
			
			ReduceDriver<PactRecord, PactRecord> testTask = new ReduceDriver<PactRecord, PactRecord>();
			
			testDriver(testTask, MockReduceStub.class);
		} catch (Exception e) {
			LOG.debug(e);
			Assert.fail("Exception in Test.");
		}
		
		Assert.assertTrue("Resultset size was "+this.outList.size()+". Expected was "+keyCnt, this.outList.size() == keyCnt);
		
		for(PactRecord record : this.outList) {
			Assert.assertTrue("Incorrect result", record.getField(1, PactInteger.class).getValue() == valCnt-record.getField(0, PactInteger.class).getValue());
		}
		
		this.outList.clear();
	}
	
	@Test
	public void testReduceTaskOnPreSortedInput() {
		final int keyCnt = 100;
		final int valCnt = 20;
		
		addInput(new UniformPactRecordGenerator(keyCnt, valCnt, true));
		addInputComparator(this.comparator);
		setOutput(this.outList);
		getTaskConfig().setDriverStrategy(DriverStrategy.GROUP_OVER_ORDERED);
		
		ReduceDriver<PactRecord, PactRecord> testTask = new ReduceDriver<PactRecord, PactRecord>();
		
		try {
			testDriver(testTask, MockReduceStub.class);
		} catch (Exception e) {
			LOG.debug(e);
			Assert.fail("Invoke method caused exception.");
		}
		
		Assert.assertTrue("Resultset size was "+this.outList.size()+". Expected was "+keyCnt, this.outList.size() == keyCnt);
		
		for(PactRecord record : this.outList) {
			Assert.assertTrue("Incorrect result", record.getField(1, PactInteger.class).getValue() == valCnt-record.getField(0, PactInteger.class).getValue());
		}
		
		this.outList.clear();
	}
	
	@Test
	public void testCombiningReduceTask() {
		final int keyCnt = 100;
		final int valCnt = 20;
		
		addInputComparator(this.comparator);
		setOutput(this.outList);
		getTaskConfig().setDriverStrategy(DriverStrategy.GROUP_OVER_ORDERED);
		
		CombiningUnilateralSortMerger<PactRecord> sorter = null;
		try {
			sorter = new CombiningUnilateralSortMerger<PactRecord>(new MockCombiningReduceStub(), 
				getMemoryManager(), getIOManager(), new UniformPactRecordGenerator(keyCnt, valCnt, false), 
				getOwningNepheleTask(), PactRecordSerializer.get(), this.comparator.duplicate(), this.perSortMem, 4, 0.8f, false);
			addInput(sorter.getIterator());
			
			ReduceDriver<PactRecord, PactRecord> testTask = new ReduceDriver<PactRecord, PactRecord>();
		
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
		
		for(PactRecord record : this.outList) {
			Assert.assertTrue("Incorrect result", record.getField(1, PactInteger.class).getValue() == expSum-record.getField(0, PactInteger.class).getValue());
		}
		
		this.outList.clear();
		
	}
	
	@Test
	public void testFailingReduceTask() {
		final int keyCnt = 100;
		final int valCnt = 20;
		
		addInput(new UniformPactRecordGenerator(keyCnt, valCnt, true));
		addInputComparator(this.comparator);
		setOutput(this.outList);
		getTaskConfig().setDriverStrategy(DriverStrategy.GROUP_OVER_ORDERED);
		
		ReduceDriver<PactRecord, PactRecord> testTask = new ReduceDriver<PactRecord, PactRecord>();
		
		try {
			testDriver(testTask, MockFailingReduceStub.class);
			Assert.fail("Stub exception was not forwarded.");
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
		getTaskConfig().setDriverStrategy(DriverStrategy.GROUP_OVER_ORDERED);
		
		final ReduceDriver<PactRecord, PactRecord> testTask = new ReduceDriver<PactRecord, PactRecord>();
		
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
		
		addInput(new UniformPactRecordGenerator(keyCnt, valCnt, true));
		addInputComparator(this.comparator);
		setOutput(new NirvanaOutputList());
		getTaskConfig().setDriverStrategy(DriverStrategy.GROUP_OVER_ORDERED);
		
		final ReduceDriver<PactRecord, PactRecord> testTask = new ReduceDriver<PactRecord, PactRecord>();
		
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
	
	public static class MockReduceStub extends ReduceStub {

		private final PactInteger key = new PactInteger();
		private final PactInteger value = new PactInteger();

		@Override
		public void reduce(Iterator<PactRecord> records, Collector<PactRecord> out)
				throws Exception {
			PactRecord element = null;
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
	public static class MockCombiningReduceStub extends ReduceStub {

		private final PactInteger key = new PactInteger();
		private final PactInteger value = new PactInteger();
		private final PactInteger combineValue = new PactInteger();

		@Override
		public void reduce(Iterator<PactRecord> records, Collector<PactRecord> out)
				throws Exception {
			PactRecord element = null;
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
		public void combine(Iterator<PactRecord> records, Collector<PactRecord> out)
				throws Exception {
			PactRecord element = null;
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
	
	public static class MockFailingReduceStub extends ReduceStub {

		private int cnt = 0;
		
		private final PactInteger key = new PactInteger();
		private final PactInteger value = new PactInteger();

		@Override
		public void reduce(Iterator<PactRecord> records, Collector<PactRecord> out)
				throws Exception {
			PactRecord element = null;
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
	
	public static class MockDelayingReduceStub extends ReduceStub {

		@Override
		public void reduce(Iterator<PactRecord> records, Collector<PactRecord> out) {
			while(records.hasNext()) {
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {}
				records.next();
			}
		}
	}
}
