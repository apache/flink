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

import eu.stratosphere.pact.common.generic.GenericCoGrouper;
import eu.stratosphere.pact.common.stubs.CoGroupStub;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.runtime.plugable.PactRecordComparator;
import eu.stratosphere.pact.runtime.task.util.LocalStrategy;
import eu.stratosphere.pact.runtime.test.util.DelayingInfinitiveInputIterator;
import eu.stratosphere.pact.runtime.test.util.DriverTestBase;
import eu.stratosphere.pact.runtime.test.util.NirvanaOutputList;
import eu.stratosphere.pact.runtime.test.util.UniformPactRecordGenerator;
import eu.stratosphere.pact.runtime.test.util.TaskCancelThread;

public class CoGroupTaskTest extends DriverTestBase<GenericCoGrouper<PactRecord, PactRecord, PactRecord>>
{
	private static final Log LOG = LogFactory.getLog(CoGroupTaskTest.class);
	
	private final List<PactRecord> outList = new ArrayList<PactRecord>();

	
	public CoGroupTaskTest() {
		super(6*1024*1024);
	}
	
	@Test
	public void testSortBoth1CoGroupTask() {

		int keyCnt1 = 100;
		int valCnt1 = 2;
		
		int keyCnt2 = 200;
		int valCnt2 = 1;
		
		super.addInput(new UniformPactRecordGenerator(keyCnt1, valCnt1, false));
		super.addInput(new UniformPactRecordGenerator(keyCnt2, valCnt2, false));
		super.addOutput(this.outList);
		
		CoGroupDriver<PactRecord, PactRecord, PactRecord> testTask = new CoGroupDriver<PactRecord, PactRecord, PactRecord>();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.SORT_BOTH_MERGE);
		super.getTaskConfig().setMemorySize(6 * 1024 * 1024);
		super.getTaskConfig().setNumFilehandles(4);
		
		final int[] keyPos1 = new int[]{0};
		final int[] keyPos2 = new int[]{0};
		@SuppressWarnings("unchecked")
		final Class<? extends Key>[] keyClasses = (Class<? extends Key>[]) new Class[]{ PactInteger.class };
		
		addInputComparator(new PactRecordComparator(keyPos1, keyClasses));
		addInputComparator(new PactRecordComparator(keyPos2, keyClasses));
		
		try {
			testDriver(testTask, MockCoGroupStub.class);
		} catch (Exception e) {
			LOG.debug(e);
			Assert.fail("Invoke method caused exception.");
		}
		
		int expCnt = valCnt1*valCnt2*Math.min(keyCnt1, keyCnt2) + Math.max(keyCnt1, keyCnt2) - Math.min(keyCnt1, keyCnt2);
		
		Assert.assertTrue("Resultset size was "+this.outList.size()+". Expected was "+expCnt, this.outList.size() == expCnt);
		
		this.outList.clear();
				
	}
	
	@Test
	public void testSortBoth2CoGroupTask() {

		int keyCnt1 = 200;
		int valCnt1 = 2;
		
		int keyCnt2 = 200;
		int valCnt2 = 4;
		
		super.addInput(new UniformPactRecordGenerator(keyCnt1, valCnt1, false));
		super.addInput(new UniformPactRecordGenerator(keyCnt2, valCnt2, false));
		super.addOutput(this.outList);
		
		CoGroupDriver<PactRecord, PactRecord, PactRecord> testTask = new CoGroupDriver<PactRecord, PactRecord, PactRecord>();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.SORT_BOTH_MERGE);
		super.getTaskConfig().setMemorySize(6 * 1024 * 1024);
		super.getTaskConfig().setNumFilehandles(4);
		
		final int[] keyPos1 = new int[]{0};
		final int[] keyPos2 = new int[]{0};
		@SuppressWarnings("unchecked")
		final Class<? extends Key>[] keyClasses = (Class<? extends Key>[]) new Class[]{ PactInteger.class };
		
		addInputComparator(new PactRecordComparator(keyPos1, keyClasses));
		addInputComparator(new PactRecordComparator(keyPos2, keyClasses));
		
		try {
			testDriver(testTask, MockCoGroupStub.class);
		} catch (Exception e) {
			LOG.debug(e);
			Assert.fail("Invoke method caused exception.");
		}
		
		int expCnt = valCnt1*valCnt2*Math.min(keyCnt1, keyCnt2) + Math.max(keyCnt1, keyCnt2) - Math.min(keyCnt1, keyCnt2);
		
		Assert.assertTrue("Resultset size was "+this.outList.size()+". Expected was "+expCnt, this.outList.size() == expCnt);
		
		this.outList.clear();
				
	}
	
	@Test
	public void testSortFirstCoGroupTask() {

		int keyCnt1 = 200;
		int valCnt1 = 2;
		
		int keyCnt2 = 200;
		int valCnt2 = 4;
		
		super.addInput(new UniformPactRecordGenerator(keyCnt1, valCnt1, false));
		super.addInput(new UniformPactRecordGenerator(keyCnt2, valCnt2, true));
		super.addOutput(this.outList);
		
		CoGroupDriver<PactRecord, PactRecord, PactRecord> testTask = new CoGroupDriver<PactRecord, PactRecord, PactRecord>();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.SORT_FIRST_MERGE);
		super.getTaskConfig().setMemorySize(5 * 1024 * 1024);
		super.getTaskConfig().setNumFilehandles(4);
		
		final int[] keyPos1 = new int[]{0};
		final int[] keyPos2 = new int[]{0};
		@SuppressWarnings("unchecked")
		final Class<? extends Key>[] keyClasses = (Class<? extends Key>[]) new Class[]{ PactInteger.class };
		
		addInputComparator(new PactRecordComparator(keyPos1, keyClasses));
		addInputComparator(new PactRecordComparator(keyPos2, keyClasses));
		
		try {
			testDriver(testTask, MockCoGroupStub.class);
		} catch (Exception e) {
			LOG.debug(e);
			Assert.fail("Invoke method caused exception.");
		}
		
		int expCnt = valCnt1*valCnt2*Math.min(keyCnt1, keyCnt2) + Math.max(keyCnt1, keyCnt2) - Math.min(keyCnt1, keyCnt2);
		
		Assert.assertTrue("Resultset size was "+this.outList.size()+". Expected was "+expCnt, this.outList.size() == expCnt);
		
		this.outList.clear();
				
	}
	
	@Test
	public void testSortSecondCoGroupTask() {

		int keyCnt1 = 200;
		int valCnt1 = 2;
		
		int keyCnt2 = 200;
		int valCnt2 = 4;
		
		super.addInput(new UniformPactRecordGenerator(keyCnt1, valCnt1, true));
		super.addInput(new UniformPactRecordGenerator(keyCnt2, valCnt2, false));
		super.addOutput(this.outList);
		
		CoGroupDriver<PactRecord, PactRecord, PactRecord> testTask = new CoGroupDriver<PactRecord, PactRecord, PactRecord>();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.SORT_SECOND_MERGE);
		super.getTaskConfig().setMemorySize(5 * 1024 * 1024);
		super.getTaskConfig().setNumFilehandles(4);
		
		final int[] keyPos1 = new int[]{0};
		final int[] keyPos2 = new int[]{0};
		@SuppressWarnings("unchecked")
		final Class<? extends Key>[] keyClasses = (Class<? extends Key>[]) new Class[]{ PactInteger.class };
		
		addInputComparator(new PactRecordComparator(keyPos1, keyClasses));
		addInputComparator(new PactRecordComparator(keyPos2, keyClasses));
		
		try {
			testDriver(testTask, MockCoGroupStub.class);
		} catch (Exception e) {
			LOG.debug(e);
			Assert.fail("Invoke method caused exception.");
		}
		
		int expCnt = valCnt1*valCnt2*Math.min(keyCnt1, keyCnt2) + Math.max(keyCnt1, keyCnt2) - Math.min(keyCnt1, keyCnt2);
		
		Assert.assertTrue("Resultset size was "+this.outList.size()+". Expected was "+expCnt, this.outList.size() == expCnt);
		
		this.outList.clear();
				
	}
	
	@Test
	public void testMergeCoGroupTask() {

		int keyCnt1 = 200;
		int valCnt1 = 2;
		
		int keyCnt2 = 200;
		int valCnt2 = 4;
		
		super.addInput(new UniformPactRecordGenerator(keyCnt1, valCnt1, true));
		super.addInput(new UniformPactRecordGenerator(keyCnt2, valCnt2, true));
		super.addOutput(this.outList);
		
		CoGroupDriver<PactRecord, PactRecord, PactRecord> testTask = new CoGroupDriver<PactRecord, PactRecord, PactRecord>();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.MERGE);
		super.getTaskConfig().setMemorySize(0);
		super.getTaskConfig().setNumFilehandles(4);
		
		final int[] keyPos1 = new int[]{0};
		final int[] keyPos2 = new int[]{0};
		@SuppressWarnings("unchecked")
		final Class<? extends Key>[] keyClasses = (Class<? extends Key>[]) new Class[]{ PactInteger.class };
		
		addInputComparator(new PactRecordComparator(keyPos1, keyClasses));
		addInputComparator(new PactRecordComparator(keyPos2, keyClasses));
		
		try {
			testDriver(testTask, MockCoGroupStub.class);
		} catch (Exception e) {
			LOG.debug(e);
			Assert.fail("Invoke method caused exception.");
		}
		
		int expCnt = valCnt1*valCnt2*Math.min(keyCnt1, keyCnt2) + Math.max(keyCnt1, keyCnt2) - Math.min(keyCnt1, keyCnt2);
		
		Assert.assertTrue("Resultset size was "+this.outList.size()+". Expected was "+expCnt, this.outList.size() == expCnt);
		
		this.outList.clear();
				
	}
	
	@Test
	public void testFailingSortCoGroupTask() {

		int keyCnt1 = 100;
		int valCnt1 = 2;
		
		int keyCnt2 = 200;
		int valCnt2 = 1;
		
		super.addInput(new UniformPactRecordGenerator(keyCnt1, valCnt1, false));
		super.addInput(new UniformPactRecordGenerator(keyCnt2, valCnt2, false));
		super.addOutput(this.outList);
		
		CoGroupDriver<PactRecord, PactRecord, PactRecord> testTask = new CoGroupDriver<PactRecord, PactRecord, PactRecord>();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.SORT_BOTH_MERGE);
		super.getTaskConfig().setMemorySize(6 * 1024 * 1024);
		super.getTaskConfig().setNumFilehandles(4);
		
		final int[] keyPos1 = new int[]{0};
		final int[] keyPos2 = new int[]{0};
		@SuppressWarnings("unchecked")
		final Class<? extends Key>[] keyClasses = (Class<? extends Key>[]) new Class[]{ PactInteger.class };
		
		addInputComparator(new PactRecordComparator(keyPos1, keyClasses));
		addInputComparator(new PactRecordComparator(keyPos2, keyClasses));
		
		boolean stubFailed = false;
		
		try {
			testDriver(testTask, MockFailingCoGroupStub.class);
		} catch (Exception e) {
			stubFailed = true;
		}
		
		Assert.assertTrue("Stub exception was not forwarded.", stubFailed);
		
		this.outList.clear();
	}
	
	@Test
	public void testCancelCoGroupTaskWhileSorting1() {
		
		int keyCnt = 10;
		int valCnt = 2;
		
		super.addInput(new UniformPactRecordGenerator(keyCnt, valCnt, false));
		super.addInput(new DelayingInfinitiveInputIterator(1000));
		super.addOutput(new NirvanaOutputList());
		
		final CoGroupDriver<PactRecord, PactRecord, PactRecord> testTask = new CoGroupDriver<PactRecord, PactRecord, PactRecord>();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.SORT_BOTH_MERGE);
		super.getTaskConfig().setMemorySize(6 * 1024 * 1024);
		super.getTaskConfig().setNumFilehandles(4);
		
		final int[] keyPos1 = new int[]{0};
		final int[] keyPos2 = new int[]{0};
		@SuppressWarnings("unchecked")
		final Class<? extends Key>[] keyClasses = (Class<? extends Key>[]) new Class[]{ PactInteger.class };
		
		addInputComparator(new PactRecordComparator(keyPos1, keyClasses));
		addInputComparator(new PactRecordComparator(keyPos2, keyClasses));
		
		Thread taskRunner = new Thread() {
			@Override
			public void run() {
				try {
					testDriver(testTask, MockCoGroupStub.class);
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
	
	@Test
	public void testCancelCoGroupTaskWhileSorting2() {
		
		int keyCnt = 10;
		int valCnt = 2;
		
		super.addInput(new DelayingInfinitiveInputIterator(1000));
		super.addInput(new UniformPactRecordGenerator(keyCnt, valCnt, false));
		super.addOutput(new NirvanaOutputList());
		
		final CoGroupDriver<PactRecord, PactRecord, PactRecord> testTask = new CoGroupDriver<PactRecord, PactRecord, PactRecord>();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.SORT_BOTH_MERGE);
		super.getTaskConfig().setMemorySize(6 * 1024 * 1024);
		super.getTaskConfig().setNumFilehandles(4);
		
		final int[] keyPos1 = new int[]{0};
		final int[] keyPos2 = new int[]{0};
		@SuppressWarnings("unchecked")
		final Class<? extends Key>[] keyClasses = (Class<? extends Key>[]) new Class[]{ PactInteger.class };
		
		addInputComparator(new PactRecordComparator(keyPos1, keyClasses));
		addInputComparator(new PactRecordComparator(keyPos2, keyClasses));
		
		Thread taskRunner = new Thread() {
			@Override
			public void run() {
				try {
					testDriver(testTask, MockCoGroupStub.class);
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
	
	@Test
	public void testCancelCoGroupTaskWhileCoGrouping()
	{
		int keyCnt = 100;
		int valCnt = 5;
		
		super.addInput(new UniformPactRecordGenerator(keyCnt, valCnt, false));
		super.addInput(new UniformPactRecordGenerator(keyCnt, valCnt, false));
		super.addOutput(new NirvanaOutputList());
		
		final CoGroupDriver<PactRecord, PactRecord, PactRecord> testTask = new CoGroupDriver<PactRecord, PactRecord, PactRecord>();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.SORT_BOTH_MERGE);
		super.getTaskConfig().setMemorySize(6 * 1024 * 1024);
		super.getTaskConfig().setNumFilehandles(4);
		
		final int[] keyPos1 = new int[]{0};
		final int[] keyPos2 = new int[]{0};
		@SuppressWarnings("unchecked")
		final Class<? extends Key>[] keyClasses = (Class<? extends Key>[]) new Class[]{ PactInteger.class };
		
		addInputComparator(new PactRecordComparator(keyPos1, keyClasses));
		addInputComparator(new PactRecordComparator(keyPos2, keyClasses));
		
		Thread taskRunner = new Thread() {
			@Override
			public void run() {
				try {
					testDriver(testTask, MockDelayingCoGroupStub.class);
				} catch (Exception ie) {
					ie.printStackTrace();
					Assert.fail("Task threw exception although it was properly canceled");
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
	
	public static class MockCoGroupStub extends CoGroupStub {

		@Override
		public void coGroup(Iterator<PactRecord> records1,
				Iterator<PactRecord> records2, Collector<PactRecord> out) {
			int val1Cnt = 0;
			
			while (records1.hasNext()) {
				val1Cnt++;
				records1.next();
			}
			
			while (records2.hasNext()) {
				PactRecord record2 = records2.next();
				
				if (val1Cnt == 0) {
					out.collect(record2);
				} else {
					for (int i=0; i<val1Cnt; i++) {
						out.collect(record2);
					}
				}
			}
		}
	
	}
	
	public static class MockFailingCoGroupStub extends CoGroupStub {
		
		int cnt = 0;
		
		@Override
		public void coGroup(Iterator<PactRecord> records1,
				Iterator<PactRecord> records2, Collector<PactRecord> out) throws RuntimeException {
			int val1Cnt = 0;
			
			while (records1.hasNext()) {
				val1Cnt++;
				records1.next();
			}
			
			while (records2.hasNext()) {
				PactRecord record2 = records2.next();
				if (val1Cnt == 0) {
					
					if(++this.cnt>=10) {
						throw new RuntimeException("Expected Test Exception");
					}
					
					out.collect(record2);
				} else {
					for (int i=0; i<val1Cnt; i++) {
						
						if(++this.cnt>=10) {
							throw new RuntimeException("Expected Test Exception");
						}
						
						out.collect(record2);
					}
				}
			}
		}
	
	}
	
	public static class MockDelayingCoGroupStub extends CoGroupStub {

		@Override
		public void coGroup(Iterator<PactRecord> records1,
				Iterator<PactRecord> records2, Collector<PactRecord> out) {
			
			while(records1.hasNext()) {
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) { }
				records1.next();
			}
			
			while(records2.hasNext()) {
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) { }
				records2.next();
			}
			
		}
	
	}
	
	
		
}
