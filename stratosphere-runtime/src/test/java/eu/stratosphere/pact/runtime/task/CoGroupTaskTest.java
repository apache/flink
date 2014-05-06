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

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

import junit.framework.Assert;

import org.junit.Test;

import eu.stratosphere.api.common.functions.GenericCoGrouper;
import eu.stratosphere.api.java.record.functions.CoGroupFunction;
import eu.stratosphere.pact.runtime.plugable.pactrecord.RecordComparator;
import eu.stratosphere.pact.runtime.plugable.pactrecord.RecordPairComparatorFactory;
import eu.stratosphere.pact.runtime.task.CoGroupTaskExternalITCase.MockCoGroupStub;
import eu.stratosphere.pact.runtime.test.util.DelayingInfinitiveInputIterator;
import eu.stratosphere.pact.runtime.test.util.DriverTestBase;
import eu.stratosphere.pact.runtime.test.util.ExpectedTestException;
import eu.stratosphere.pact.runtime.test.util.TaskCancelThread;
import eu.stratosphere.pact.runtime.test.util.UniformRecordGenerator;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Key;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;

public class CoGroupTaskTest extends DriverTestBase<GenericCoGrouper<Record, Record, Record>>
{
	private static final long SORT_MEM = 3*1024*1024;
	
	@SuppressWarnings("unchecked")
	private final RecordComparator comparator1 = new RecordComparator(
		new int[]{0}, (Class<? extends Key<?>>[])new Class[]{ IntValue.class });
	
	@SuppressWarnings("unchecked")
	private final RecordComparator comparator2 = new RecordComparator(
		new int[]{0}, (Class<? extends Key<?>>[])new Class[]{ IntValue.class });
	
	private final CountingOutputCollector output = new CountingOutputCollector();
	
	
	public CoGroupTaskTest() {
		super(0, 2, SORT_MEM);
	}
	
	@Test
	public void testSortBoth1CoGroupTask() {
		int keyCnt1 = 100;
		int valCnt1 = 2;
		
		int keyCnt2 = 200;
		int valCnt2 = 1;
		
		final int expCnt = valCnt1*valCnt2*Math.min(keyCnt1, keyCnt2) + 
			(keyCnt1 > keyCnt2 ? (keyCnt1 - keyCnt2) * valCnt1 : (keyCnt2 - keyCnt1) * valCnt2);
		
		setOutput(this.output);
		addInputComparator(this.comparator1);
		addInputComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
		getTaskConfig().setDriverStrategy(DriverStrategy.CO_GROUP);
		
		final CoGroupDriver<Record, Record, Record> testTask = new CoGroupDriver<Record, Record, Record>();
		
		try {
			addInputSorted(new UniformRecordGenerator(keyCnt1, valCnt1, false), this.comparator1.duplicate());
			addInputSorted(new UniformRecordGenerator(keyCnt2, valCnt2, false), this.comparator2.duplicate());
			testDriver(testTask, MockCoGroupStub.class);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("The test caused an exception.");
		}
		
		Assert.assertEquals("Wrong result set size.", expCnt, this.output.getNumberOfRecords());
	}
	
	@Test
	public void testSortBoth2CoGroupTask() {
		int keyCnt1 = 200;
		int valCnt1 = 2;
		
		int keyCnt2 = 200;
		int valCnt2 = 4;
		
		final int expCnt = valCnt1*valCnt2*Math.min(keyCnt1, keyCnt2) + 
			(keyCnt1 > keyCnt2 ? (keyCnt1 - keyCnt2) * valCnt1 : (keyCnt2 - keyCnt1) * valCnt2);
		
		setOutput(this.output);
		addInputComparator(this.comparator1);
		addInputComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
		getTaskConfig().setDriverStrategy(DriverStrategy.CO_GROUP);
		
		final CoGroupDriver<Record, Record, Record> testTask = new CoGroupDriver<Record, Record, Record>();
		
		try {
			addInputSorted(new UniformRecordGenerator(keyCnt1, valCnt1, false), this.comparator1.duplicate());
			addInputSorted(new UniformRecordGenerator(keyCnt2, valCnt2, false), this.comparator2.duplicate());
			testDriver(testTask, MockCoGroupStub.class);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("The test caused an exception.");
		}
		
		Assert.assertEquals("Wrong result set size.", expCnt, this.output.getNumberOfRecords());
	}
	
	@Test
	public void testSortFirstCoGroupTask() {
		int keyCnt1 = 200;
		int valCnt1 = 2;
		
		int keyCnt2 = 200;
		int valCnt2 = 4;
		
		final int expCnt = valCnt1*valCnt2*Math.min(keyCnt1, keyCnt2) + 
			(keyCnt1 > keyCnt2 ? (keyCnt1 - keyCnt2) * valCnt1 : (keyCnt2 - keyCnt1) * valCnt2);
		
		setOutput(this.output);
		addInputComparator(this.comparator1);
		addInputComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
		getTaskConfig().setDriverStrategy(DriverStrategy.CO_GROUP);
		
		final CoGroupDriver<Record, Record, Record> testTask = new CoGroupDriver<Record, Record, Record>();
		
		try {
			addInputSorted(new UniformRecordGenerator(keyCnt1, valCnt1, false), this.comparator1.duplicate());
			addInput(new UniformRecordGenerator(keyCnt2, valCnt2, true));
			testDriver(testTask, MockCoGroupStub.class);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("The test caused an exception.");
		}
		
		Assert.assertEquals("Wrong result set size.", expCnt, this.output.getNumberOfRecords());
	}
	
	@Test
	public void testSortSecondCoGroupTask() {
		int keyCnt1 = 200;
		int valCnt1 = 2;
		
		int keyCnt2 = 200;
		int valCnt2 = 4;
		
		final int expCnt = valCnt1*valCnt2*Math.min(keyCnt1, keyCnt2) + 
			(keyCnt1 > keyCnt2 ? (keyCnt1 - keyCnt2) * valCnt1 : (keyCnt2 - keyCnt1) * valCnt2);
		
		setOutput(this.output);
		addInputComparator(this.comparator1);
		addInputComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
		getTaskConfig().setDriverStrategy(DriverStrategy.CO_GROUP);
		
		final CoGroupDriver<Record, Record, Record> testTask = new CoGroupDriver<Record, Record, Record>();
		
		try {
			addInput(new UniformRecordGenerator(keyCnt1, valCnt1, true));
			addInputSorted(new UniformRecordGenerator(keyCnt2, valCnt2, false), this.comparator2.duplicate());
			testDriver(testTask, MockCoGroupStub.class);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("The test caused an exception.");
		}
		
		Assert.assertEquals("Wrong result set size.", expCnt, this.output.getNumberOfRecords());
	}
	
	@Test
	public void testMergeCoGroupTask() {
		int keyCnt1 = 200;
		int valCnt1 = 2;
		
		int keyCnt2 = 200;
		int valCnt2 = 4;
		
		final int expCnt = valCnt1*valCnt2*Math.min(keyCnt1, keyCnt2) + 
			(keyCnt1 > keyCnt2 ? (keyCnt1 - keyCnt2) * valCnt1 : (keyCnt2 - keyCnt1) * valCnt2);
		
		setOutput(this.output);
		
		addInput(new UniformRecordGenerator(keyCnt1, valCnt1, true));
		addInput(new UniformRecordGenerator(keyCnt2, valCnt2, true));
		addInputComparator(this.comparator1);
		addInputComparator(this.comparator2);
		
		getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
		getTaskConfig().setDriverStrategy(DriverStrategy.CO_GROUP);
		
		final CoGroupDriver<Record, Record, Record> testTask = new CoGroupDriver<Record, Record, Record>();
		
		try {
			testDriver(testTask, MockCoGroupStub.class);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("The test caused an exception.");
		}
		
		Assert.assertEquals("Wrong result set size.", expCnt, this.output.getNumberOfRecords());
	}
	
	@Test
	public void testFailingSortCoGroupTask() {
		int keyCnt1 = 100;
		int valCnt1 = 2;
		
		int keyCnt2 = 200;
		int valCnt2 = 1;
		
		setOutput(this.output);
		
		addInput(new UniformRecordGenerator(keyCnt1, valCnt1, true));
		addInput(new UniformRecordGenerator(keyCnt2, valCnt2, true));
		addInputComparator(this.comparator1);
		addInputComparator(this.comparator2);
		
		getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
		getTaskConfig().setDriverStrategy(DriverStrategy.CO_GROUP);
		
		final CoGroupDriver<Record, Record, Record> testTask = new CoGroupDriver<Record, Record, Record>();
		
		try {
			testDriver(testTask, MockFailingCoGroupStub.class);
			Assert.fail("Function exception was not forwarded.");
		} catch (ExpectedTestException etex) {
			// good!
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("The test caused an exception.");
		}
	}
	
	@Test
	public void testCancelCoGroupTaskWhileSorting1() {
		int keyCnt = 10;
		int valCnt = 2;
		
		setOutput(this.output);
		
		addInputComparator(this.comparator1);
		addInputComparator(this.comparator2);
		
		getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
		getTaskConfig().setDriverStrategy(DriverStrategy.CO_GROUP);
		
		final CoGroupDriver<Record, Record, Record> testTask = new CoGroupDriver<Record, Record, Record>();
		
		try {
			addInputSorted(new DelayingInfinitiveInputIterator(1000), this.comparator1.duplicate());
			addInput(new UniformRecordGenerator(keyCnt, valCnt, true));
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("The test caused an exception.");
		}
		
		final AtomicBoolean success = new AtomicBoolean(false);
		
		Thread taskRunner = new Thread() {
			@Override
			public void run() {
				try {
					testDriver(testTask, MockCoGroupStub.class);
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
	public void testCancelCoGroupTaskWhileSorting2() {
		int keyCnt = 10;
		int valCnt = 2;
		
		setOutput(this.output);
		
		addInputComparator(this.comparator1);
		addInputComparator(this.comparator2);
		
		getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
		getTaskConfig().setDriverStrategy(DriverStrategy.CO_GROUP);
		
		final CoGroupDriver<Record, Record, Record> testTask = new CoGroupDriver<Record, Record, Record>();
		
		try {
			addInput(new UniformRecordGenerator(keyCnt, valCnt, true));
			addInputSorted(new DelayingInfinitiveInputIterator(1000), this.comparator2.duplicate());
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("The test caused an exception.");
		}
		
		final AtomicBoolean success = new AtomicBoolean(false);
		
		Thread taskRunner = new Thread() {
			@Override
			public void run() {
				try {
					testDriver(testTask, MockCoGroupStub.class);
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
	public void testCancelCoGroupTaskWhileCoGrouping()
	{
		int keyCnt = 100;
		int valCnt = 5;
		
		setOutput(this.output);
		
		addInputComparator(this.comparator1);
		addInputComparator(this.comparator2);
		
		getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
		getTaskConfig().setDriverStrategy(DriverStrategy.CO_GROUP);
		
		final CoGroupDriver<Record, Record, Record> testTask = new CoGroupDriver<Record, Record, Record>();
		
		try {
			addInput(new UniformRecordGenerator(keyCnt, valCnt, true));
			addInput(new UniformRecordGenerator(keyCnt, valCnt, true));
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("The test caused an exception.");
		}
		
		final AtomicBoolean success = new AtomicBoolean(false);
		
		Thread taskRunner = new Thread() {
			@Override
			public void run() {
				try {
					testDriver(testTask, MockDelayingCoGroupStub.class);
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
	
	public static class MockFailingCoGroupStub extends CoGroupFunction {
		private static final long serialVersionUID = 1L;
		
		private int cnt = 0;
		
		@Override
		public void coGroup(Iterator<Record> records1,
				Iterator<Record> records2, Collector<Record> out) throws RuntimeException
		{
			int val1Cnt = 0;
			
			while (records1.hasNext()) {
				val1Cnt++;
				records1.next();
			}
			
			while (records2.hasNext()) {
				Record record2 = records2.next();
				if (val1Cnt == 0) {
					
					if(++this.cnt>=10) {
						throw new ExpectedTestException();
					}
					
					out.collect(record2);
				} else {
					for (int i=0; i<val1Cnt; i++) {
						
						if(++this.cnt>=10) {
							throw new ExpectedTestException();
						}
						
						out.collect(record2);
					}
				}
			}
		}
	
	}
	
	public static final class MockDelayingCoGroupStub extends CoGroupFunction {
		private static final long serialVersionUID = 1L;
		
		@Override
		public void coGroup(Iterator<Record> records1,
				Iterator<Record> records2, Collector<Record> out) {
			
			while (records1.hasNext()) {
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) { }
				records1.next();
			}
			
			while (records2.hasNext()) {
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) { }
				records2.next();
			}
		}
	}
}
