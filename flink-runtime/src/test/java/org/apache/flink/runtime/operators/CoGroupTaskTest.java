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

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.types.Value;
import org.junit.Assert;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.runtime.testutils.recordutils.RecordComparator;
import org.apache.flink.runtime.testutils.recordutils.RecordPairComparatorFactory;
import org.apache.flink.runtime.operators.CoGroupTaskExternalITCase.MockCoGroupStub;
import org.apache.flink.runtime.operators.testutils.DelayingInfinitiveInputIterator;
import org.apache.flink.runtime.operators.testutils.DriverTestBase;
import org.apache.flink.runtime.operators.testutils.ExpectedTestException;
import org.apache.flink.runtime.operators.testutils.TaskCancelThread;
import org.apache.flink.runtime.operators.testutils.UniformRecordGenerator;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.Record;
import org.apache.flink.util.Collector;
import org.junit.Test;

public class CoGroupTaskTest extends DriverTestBase<CoGroupFunction<Record, Record, Record>>
{
	private static final long SORT_MEM = 3*1024*1024;
	
	@SuppressWarnings("unchecked")
	private final RecordComparator comparator1 = new RecordComparator(
		new int[]{0}, (Class<? extends Value>[])new Class[]{ IntValue.class });
	
	@SuppressWarnings("unchecked")
	private final RecordComparator comparator2 = new RecordComparator(
		new int[]{0}, (Class<? extends Value>[])new Class[]{ IntValue.class });
	
	private final CountingOutputCollector output = new CountingOutputCollector();
	
	
	public CoGroupTaskTest(ExecutionConfig config) {
		super(config, 0, 2, SORT_MEM);
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
		addDriverComparator(this.comparator1);
		addDriverComparator(this.comparator2);
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
		addDriverComparator(this.comparator1);
		addDriverComparator(this.comparator2);
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
		addDriverComparator(this.comparator1);
		addDriverComparator(this.comparator2);
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
		addDriverComparator(this.comparator1);
		addDriverComparator(this.comparator2);
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
		addDriverComparator(this.comparator1);
		addDriverComparator(this.comparator2);
		
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
		addDriverComparator(this.comparator1);
		addDriverComparator(this.comparator2);
		
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
		
		addDriverComparator(this.comparator1);
		addDriverComparator(this.comparator2);
		
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
		
		addDriverComparator(this.comparator1);
		addDriverComparator(this.comparator2);
		
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
		
		addDriverComparator(this.comparator1);
		addDriverComparator(this.comparator2);
		
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
	
	public static class MockFailingCoGroupStub extends RichCoGroupFunction<Record, Record, Record> {
		private static final long serialVersionUID = 1L;
		
		private int cnt = 0;
		
		@Override
		public void coGroup(Iterable<Record> records1, Iterable<Record> records2, Collector<Record> out) {
			int val1Cnt = 0;
			
			for (@SuppressWarnings("unused") Record r : records1) { 
				val1Cnt++;
			}
			
			for (Record record2 : records2) { 
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
	
	public static final class MockDelayingCoGroupStub extends RichCoGroupFunction<Record, Record, Record> {
		private static final long serialVersionUID = 1L;
		
		@SuppressWarnings("unused")
		@Override
		public void coGroup(Iterable<Record> records1, Iterable<Record> records2, Collector<Record> out) {
			
			for (Record r : records1) { 
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) { }
			}
			
			for (Record r : records2) { 
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) { }
			}
		}
	}
}
