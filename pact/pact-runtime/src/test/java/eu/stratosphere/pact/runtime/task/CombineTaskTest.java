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

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import eu.stratosphere.pact.common.contract.ReduceContract.Combinable;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.generic.stub.GenericReducer;
import eu.stratosphere.pact.runtime.plugable.PactRecordComparator;
import eu.stratosphere.pact.runtime.task.util.LocalStrategy;
import eu.stratosphere.pact.runtime.test.util.DelayingInfinitiveInputIterator;
import eu.stratosphere.pact.runtime.test.util.DriverTestBase;
import eu.stratosphere.pact.runtime.test.util.NirvanaOutputList;
import eu.stratosphere.pact.runtime.test.util.UniformPactRecordGenerator;
import eu.stratosphere.pact.runtime.test.util.TaskCancelThread;


public class CombineTaskTest extends DriverTestBase<GenericReducer<PactRecord, ?>>
{
	private static final Log LOG = LogFactory.getLog(CombineTaskTest.class);
	
	final List<PactRecord> outList = new ArrayList<PactRecord>();

	public CombineTaskTest() {
		super(3*1024*1024);
	}
	
	@Test
	public void testCombineTask() {

		int keyCnt = 100;
		int valCnt = 20;
		
		addInput(new UniformPactRecordGenerator(keyCnt, valCnt, false));
		addOutput(this.outList);
		
		CombineDriver<PactRecord> testTask = new CombineDriver<PactRecord>();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.COMBININGSORT);
		super.getTaskConfig().setMemorySize(3 * 1024 * 1024);
		super.getTaskConfig().setNumFilehandles(2);
		
		final int[] keyPos = new int[]{0};
		@SuppressWarnings("unchecked")
		final Class<? extends Key>[] keyClasses = (Class<? extends Key>[]) new Class[]{ PactInteger.class };
		addInputComparator(new PactRecordComparator(keyPos, keyClasses));
		
		try {
			testDriver(testTask, MockCombiningReduceStub.class);
		} catch (Exception e) {
			LOG.debug(e);
			Assert.fail("Invoke method caused exception.");
		}
		
		int expSum = 0;
		for(int i=1;i<valCnt;i++) {
			expSum+=i;
		}
		
		Assert.assertTrue("Resultset size was "+this.outList.size()+". Expected was "+keyCnt, this.outList.size() == keyCnt);
		
		for(PactRecord record : this.outList) {
			Assert.assertTrue("Incorrect result", record.getField(1, PactInteger.class).getValue() == expSum);
		}
		
		this.outList.clear();
		
	}
	
	@Test
	public void testFailingCombineTask() {

		int keyCnt = 100;
		int valCnt = 20;
		
		addInput(new UniformPactRecordGenerator(keyCnt, valCnt, false));
		addOutput(this.outList);
		
		CombineDriver<PactRecord> testTask = new CombineDriver<PactRecord>();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.COMBININGSORT);
		super.getTaskConfig().setMemorySize(3 * 1024 * 1024);
		super.getTaskConfig().setNumFilehandles(2);
		
		final int[] keyPos = new int[]{0};
		@SuppressWarnings("unchecked")
		final Class<? extends Key>[] keyClasses = (Class<? extends Key>[]) new Class[]{ PactInteger.class };
		addInputComparator(new PactRecordComparator(keyPos, keyClasses));
		
		boolean stubFailed = false;
		
		try {
			testDriver(testTask, MockFailingCombiningReduceStub.class);
		} catch (Exception e) {
			stubFailed = true;
		}
		
		Assert.assertTrue("Stub exception was not forwarded.", stubFailed);
				
		this.outList.clear();
		
	}
	
	@Test
	public void testCancelCombineTaskSorting()
	{
		addInput(new DelayingInfinitiveInputIterator(100));
		addOutput(new NirvanaOutputList());
		
		final CombineDriver<PactRecord> testTask = new CombineDriver<PactRecord>();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.COMBININGSORT);
		super.getTaskConfig().setMemorySize(3 * 1024 * 1024);
		super.getTaskConfig().setNumFilehandles(2);
		
		final int[] keyPos = new int[]{0};
		@SuppressWarnings("unchecked")
		final Class<? extends Key>[] keyClasses = (Class<? extends Key>[]) new Class[]{ PactInteger.class };
		addInputComparator(new PactRecordComparator(keyPos, keyClasses));
		
		Thread taskRunner = new Thread() {
			@Override
			public void run() {
				try {
					testDriver(testTask, MockFailingCombiningReduceStub.class);
				} catch (Exception ie) {
					ie.printStackTrace();
					Assert.fail("Task threw exception although it was properly canceled");
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
		
	}
	
	@Combinable
	public static class MockCombiningReduceStub extends ReduceStub
	{
		private final PactInteger theInteger = new PactInteger();

		@Override
		public void reduce(Iterator<PactRecord> records, Collector<PactRecord> out)
				throws Exception {
			PactRecord element = null;
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
		public void combine(Iterator<PactRecord> records, Collector<PactRecord> out) throws Exception {
			reduce(records, out);
		}
		
	}
	
	@Combinable
	public static class MockFailingCombiningReduceStub extends ReduceStub {

		int cnt = 0;
		
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
			
			if(++this.cnt>=10) {
				throw new RuntimeException("Expected Test Exception");
			}
			
			this.combineValue.setValue(sum);
			element.setField(1, this.combineValue);
			out.collect(element);
		}
		
	}
	
}
