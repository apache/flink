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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.runtime.testutils.recordutils.RecordComparator;
import org.apache.flink.runtime.testutils.recordutils.RecordPairComparatorFactory;
import org.apache.flink.runtime.operators.testutils.DelayingInfinitiveInputIterator;
import org.apache.flink.runtime.operators.testutils.DriverTestBase;
import org.apache.flink.runtime.operators.testutils.ExpectedTestException;
import org.apache.flink.runtime.operators.testutils.NirvanaOutputList;
import org.apache.flink.runtime.operators.testutils.TaskCancelThread;
import org.apache.flink.runtime.operators.testutils.UniformRecordGenerator;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.Record;
import org.apache.flink.types.Value;
import org.apache.flink.util.Collector;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class JoinTaskTest extends DriverTestBase<FlatJoinFunction<Record, Record, Record>> {
	
	private static final long HASH_MEM = 6*1024*1024;
	
	private static final long SORT_MEM = 3*1024*1024;

	private static final int NUM_SORTER = 2;
	
	private static final long BNLJN_MEM = 10 * PAGE_SIZE;

	private final double bnljn_frac;

	private final double hash_frac;
	
	@SuppressWarnings("unchecked")
	private final RecordComparator comparator1 = new RecordComparator(
		new int[]{0}, (Class<? extends Value>[])new Class<?>[]{ IntValue.class });
	
	@SuppressWarnings("unchecked")
	private final RecordComparator comparator2 = new RecordComparator(
		new int[]{0}, (Class<? extends Value>[])new Class<?>[]{ IntValue.class });
	
	private final List<Record> outList = new ArrayList<>();
	
	
	public JoinTaskTest(ExecutionConfig config) {
		super(config, HASH_MEM, NUM_SORTER, SORT_MEM);
		bnljn_frac = (double)BNLJN_MEM/this.getMemoryManager().getMemorySize();
		hash_frac = (double)HASH_MEM/this.getMemoryManager().getMemorySize();
	}
	
	
	@Test
	public void testSortBoth1MatchTask() {
		final int keyCnt1 = 20;
		final int valCnt1 = 1;
		
		final int keyCnt2 = 10;
		final int valCnt2 = 2;
		
		setOutput(this.outList);
		addDriverComparator(this.comparator1);
		addDriverComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
		getTaskConfig().setDriverStrategy(DriverStrategy.INNER_MERGE);
		getTaskConfig().setRelativeMemoryDriver(bnljn_frac);
		setNumFileHandlesForSort(4);
		
		final JoinDriver<Record, Record, Record> testTask = new JoinDriver<>();
		
		try {
			addInputSorted(new UniformRecordGenerator(keyCnt1, valCnt1, false), this.comparator1.duplicate());
			addInputSorted(new UniformRecordGenerator(keyCnt2, valCnt2, false), this.comparator2.duplicate());
			testDriver(testTask, MockMatchStub.class);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("The test caused an exception.");
		}
		
		final int expCnt = valCnt1*valCnt2*Math.min(keyCnt1, keyCnt2);
		Assert.assertTrue("Resultset size was " + this.outList.size() + ". Expected was " + expCnt, this.outList.size() == expCnt);
		
		this.outList.clear();
	}
	
	@Test
	public void testSortBoth2MatchTask() {

		int keyCnt1 = 20;
		int valCnt1 = 1;
		
		int keyCnt2 = 20;
		int valCnt2 = 1;
		
		setOutput(this.outList);
		addDriverComparator(this.comparator1);
		addDriverComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
		getTaskConfig().setDriverStrategy(DriverStrategy.INNER_MERGE);
		getTaskConfig().setRelativeMemoryDriver(bnljn_frac);
		setNumFileHandlesForSort(4);
		
		final JoinDriver<Record, Record, Record> testTask = new JoinDriver<>();
		
		try {
			addInputSorted(new UniformRecordGenerator(keyCnt1, valCnt1, false), this.comparator1.duplicate());
			addInputSorted(new UniformRecordGenerator(keyCnt2, valCnt2, false), this.comparator2.duplicate());
			testDriver(testTask, MockMatchStub.class);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("The test caused an exception.");
		}
		
		int expCnt = valCnt1*valCnt2*Math.min(keyCnt1, keyCnt2);
		
		Assert.assertTrue("Resultset size was "+this.outList.size()+". Expected was "+expCnt, this.outList.size() == expCnt);
		
		this.outList.clear();
		
	}
	
	@Test
	public void testSortBoth3MatchTask() {

		int keyCnt1 = 20;
		int valCnt1 = 1;
		
		int keyCnt2 = 20;
		int valCnt2 = 20;
		
		setOutput(this.outList);
		addDriverComparator(this.comparator1);
		addDriverComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
		getTaskConfig().setDriverStrategy(DriverStrategy.INNER_MERGE);
		getTaskConfig().setRelativeMemoryDriver(bnljn_frac);
		setNumFileHandlesForSort(4);
		
		final JoinDriver<Record, Record, Record> testTask = new JoinDriver<>();
		
		try {
			addInputSorted(new UniformRecordGenerator(keyCnt1, valCnt1, false), this.comparator1.duplicate());
			addInputSorted(new UniformRecordGenerator(keyCnt2, valCnt2, false), this.comparator2.duplicate());
			testDriver(testTask, MockMatchStub.class);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("The test caused an exception.");
		}
		
		int expCnt = valCnt1*valCnt2*Math.min(keyCnt1, keyCnt2);
		
		Assert.assertTrue("Resultset size was "+this.outList.size()+". Expected was "+expCnt, this.outList.size() == expCnt);
		
		this.outList.clear();
		
	}
	
	@Test
	public void testSortBoth4MatchTask() {

		int keyCnt1 = 20;
		int valCnt1 = 20;
		
		int keyCnt2 = 20;
		int valCnt2 = 1;
		
		setOutput(this.outList);
		addDriverComparator(this.comparator1);
		addDriverComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
		getTaskConfig().setDriverStrategy(DriverStrategy.INNER_MERGE);
		getTaskConfig().setRelativeMemoryDriver(bnljn_frac);
		setNumFileHandlesForSort(4);
		
		final JoinDriver<Record, Record, Record> testTask = new JoinDriver<>();
		
		try {
			addInputSorted(new UniformRecordGenerator(keyCnt1, valCnt1, false), this.comparator1.duplicate());
			addInputSorted(new UniformRecordGenerator(keyCnt2, valCnt2, false), this.comparator2.duplicate());
			testDriver(testTask, MockMatchStub.class);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("The test caused an exception.");
		}
		
		int expCnt = valCnt1*valCnt2*Math.min(keyCnt1, keyCnt2);
		
		Assert.assertTrue("Resultset size was "+this.outList.size()+". Expected was "+expCnt, this.outList.size() == expCnt);
		
		this.outList.clear();
		
	}
	
	@Test
	public void testSortBoth5MatchTask() {

		int keyCnt1 = 20;
		int valCnt1 = 20;
		
		int keyCnt2 = 20;
		int valCnt2 = 20;
		
		setOutput(this.outList);
		addDriverComparator(this.comparator1);
		addDriverComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
		getTaskConfig().setDriverStrategy(DriverStrategy.INNER_MERGE);
		getTaskConfig().setRelativeMemoryDriver(bnljn_frac);
		setNumFileHandlesForSort(4);
		
		final JoinDriver<Record, Record, Record> testTask = new JoinDriver<>();
		
		try {
			addInputSorted(new UniformRecordGenerator(keyCnt1, valCnt1, false), this.comparator1.duplicate());
			addInputSorted(new UniformRecordGenerator(keyCnt2, valCnt2, false), this.comparator2.duplicate());
			testDriver(testTask, MockMatchStub.class);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("The test caused an exception.");
		}
		
		int expCnt = valCnt1*valCnt2*Math.min(keyCnt1, keyCnt2);
		
		Assert.assertTrue("Resultset size was "+this.outList.size()+". Expected was "+expCnt, this.outList.size() == expCnt);
		
		this.outList.clear();
		
	}
	
	@Test
	public void testSortFirstMatchTask() {

		int keyCnt1 = 20;
		int valCnt1 = 20;
		
		int keyCnt2 = 20;
		int valCnt2 = 20;
		
		setOutput(this.outList);
		addDriverComparator(this.comparator1);
		addDriverComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
		getTaskConfig().setDriverStrategy(DriverStrategy.INNER_MERGE);
		getTaskConfig().setRelativeMemoryDriver(bnljn_frac);
		setNumFileHandlesForSort(4);
		
		final JoinDriver<Record, Record, Record> testTask = new JoinDriver<>();
		
		try {
			addInputSorted(new UniformRecordGenerator(keyCnt1, valCnt1, false), this.comparator1.duplicate());
			addInput(new UniformRecordGenerator(keyCnt2, valCnt2, true));
			testDriver(testTask, MockMatchStub.class);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("The test caused an exception.");
		}
		
		int expCnt = valCnt1*valCnt2*Math.min(keyCnt1, keyCnt2);
		
		Assert.assertTrue("Resultset size was "+this.outList.size()+". Expected was "+expCnt, this.outList.size() == expCnt);
		
		this.outList.clear();
		
	}
	
	@Test
	public void testSortSecondMatchTask() {

		int keyCnt1 = 20;
		int valCnt1 = 20;
		
		int keyCnt2 = 20;
		int valCnt2 = 20;
		
		setOutput(this.outList);
		addDriverComparator(this.comparator1);
		addDriverComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
		getTaskConfig().setDriverStrategy(DriverStrategy.INNER_MERGE);
		getTaskConfig().setRelativeMemoryDriver(bnljn_frac);
		setNumFileHandlesForSort(4);
		
		final JoinDriver<Record, Record, Record> testTask = new JoinDriver<>();
		
		try {
			addInput(new UniformRecordGenerator(keyCnt1, valCnt1, true));
			addInputSorted(new UniformRecordGenerator(keyCnt2, valCnt2, false), this.comparator2.duplicate());
			testDriver(testTask, MockMatchStub.class);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("The test caused an exception.");
		}
		
		int expCnt = valCnt1*valCnt2*Math.min(keyCnt1, keyCnt2);
		
		Assert.assertTrue("Resultset size was "+this.outList.size()+". Expected was "+expCnt, this.outList.size() == expCnt);
		
		this.outList.clear();
		
	}
	
	@Test
	public void testMergeMatchTask() {
		int keyCnt1 = 20;
		int valCnt1 = 20;
		
		int keyCnt2 = 20;
		int valCnt2 = 20;
		
		setOutput(this.outList);
		addDriverComparator(this.comparator1);
		addDriverComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
		getTaskConfig().setDriverStrategy(DriverStrategy.INNER_MERGE);
		getTaskConfig().setRelativeMemoryDriver(bnljn_frac);
		setNumFileHandlesForSort(4);
		
		final JoinDriver<Record, Record, Record> testTask = new JoinDriver<>();
		
		addInput(new UniformRecordGenerator(keyCnt1, valCnt1, true));
		addInput(new UniformRecordGenerator(keyCnt2, valCnt2, true));
		
		try {
			testDriver(testTask, MockMatchStub.class);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("The test caused an exception.");
		}
		
		int expCnt = valCnt1*valCnt2*Math.min(keyCnt1, keyCnt2);
		
		Assert.assertTrue("Resultset size was "+this.outList.size()+". Expected was "+expCnt, this.outList.size() == expCnt);
		
		this.outList.clear();
		
	}
	
	@Test
	public void testFailingMatchTask() {
		int keyCnt1 = 20;
		int valCnt1 = 20;
		
		int keyCnt2 = 20;
		int valCnt2 = 20;
		
		setOutput(new NirvanaOutputList());
		addDriverComparator(this.comparator1);
		addDriverComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
		getTaskConfig().setDriverStrategy(DriverStrategy.INNER_MERGE);
		getTaskConfig().setRelativeMemoryDriver(bnljn_frac);
		setNumFileHandlesForSort(4);
		
		final JoinDriver<Record, Record, Record> testTask = new JoinDriver<>();
		
		addInput(new UniformRecordGenerator(keyCnt1, valCnt1, true));
		addInput(new UniformRecordGenerator(keyCnt2, valCnt2, true));
		
		try {
			testDriver(testTask, MockFailingMatchStub.class);
			Assert.fail("Driver did not forward Exception.");
		} catch (ExpectedTestException e) {
			// good!
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("The test caused an exception.");
		}
	}
	
	@Test
	public void testCancelMatchTaskWhileSort1() {
		final int keyCnt = 20;
		final int valCnt = 20;
		
		try {
			setOutput(new NirvanaOutputList());
			addDriverComparator(this.comparator1);
			addDriverComparator(this.comparator2);
			getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
			getTaskConfig().setDriverStrategy(DriverStrategy.INNER_MERGE);
			getTaskConfig().setRelativeMemoryDriver(bnljn_frac);
			setNumFileHandlesForSort(4);
			
			final JoinDriver<Record, Record, Record> testTask = new JoinDriver<>();
			
			try {
				addInputSorted(new DelayingInfinitiveInputIterator(100), this.comparator1.duplicate());
				addInput(new UniformRecordGenerator(keyCnt, valCnt, true));
			} catch (Exception e) {
				e.printStackTrace();
				Assert.fail("The test caused an exception.");
			}
	
			final AtomicReference<Throwable> error = new AtomicReference<>();

			Thread taskRunner = new Thread("Task runner for testCancelMatchTaskWhileSort1()") {
				@Override
				public void run() {
					try {
						testDriver(testTask, MockMatchStub.class);
					}
					catch (Throwable t) {
						error.set(t);
					}
				}
			};
			taskRunner.start();

			Thread.sleep(1000);

			cancel();
			taskRunner.interrupt();

			taskRunner.join(60000);

			assertFalse("Task thread did not finish within 60 seconds", taskRunner.isAlive());

			Throwable taskError = error.get();
			if (taskError != null) {
				taskError.printStackTrace();
				fail("Error in task while canceling: " + taskError.getMessage());
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testCancelMatchTaskWhileSort2() {
		final int keyCnt = 20;
		final int valCnt = 20;
		
		try {
			setOutput(new NirvanaOutputList());
			addDriverComparator(this.comparator1);
			addDriverComparator(this.comparator2);
			getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
			getTaskConfig().setDriverStrategy(DriverStrategy.INNER_MERGE);
			getTaskConfig().setRelativeMemoryDriver(bnljn_frac);
			setNumFileHandlesForSort(4);
			
			final JoinDriver<Record, Record, Record> testTask = new JoinDriver<>();
			
			try {
				addInput(new UniformRecordGenerator(keyCnt, valCnt, true));
				addInputSorted(new DelayingInfinitiveInputIterator(100), this.comparator1.duplicate());
			} catch (Exception e) {
				e.printStackTrace();
				Assert.fail("The test caused an exception.");
			}

			final AtomicReference<Throwable> error = new AtomicReference<>();

			Thread taskRunner = new Thread("Task runner for testCancelMatchTaskWhileSort2()") {
				@Override
				public void run() {
					try {
						testDriver(testTask, MockMatchStub.class);
					}
					catch (Throwable t) {
						error.set(t);
					}
				}
			};
			taskRunner.start();

			Thread.sleep(1000);

			cancel();
			taskRunner.interrupt();

			taskRunner.join(60000);

			assertFalse("Task thread did not finish within 60 seconds", taskRunner.isAlive());

			Throwable taskError = error.get();
			if (taskError != null) {
				taskError.printStackTrace();
				fail("Error in task while canceling: " + taskError.getMessage());
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testCancelMatchTaskWhileMatching() {
		final int keyCnt = 20;
		final int valCnt = 20;
		
		try {
			setOutput(new NirvanaOutputList());
			addDriverComparator(this.comparator1);
			addDriverComparator(this.comparator2);
			getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
			getTaskConfig().setDriverStrategy(DriverStrategy.INNER_MERGE);
			getTaskConfig().setRelativeMemoryDriver(bnljn_frac);
			setNumFileHandlesForSort(4);
			
			final JoinDriver<Record, Record, Record> testTask = new JoinDriver<>();
			
			addInput(new UniformRecordGenerator(keyCnt, valCnt, true));
			addInput(new UniformRecordGenerator(keyCnt, valCnt, true));
			
			final AtomicReference<Throwable> error = new AtomicReference<>();
			
			Thread taskRunner = new Thread("Task runner for testCancelMatchTaskWhileMatching()") {
				@Override
				public void run() {
					try {
						testDriver(testTask, MockDelayingMatchStub.class);
					}
					catch (Throwable t) {
						error.set(t);
					}
				}
			};
			taskRunner.start();
			
			Thread.sleep(1000);
			
			cancel();
			taskRunner.interrupt();
			
			taskRunner.join(60000);
			
			assertFalse("Task thread did not finish within 60 seconds", taskRunner.isAlive());
			
			Throwable taskError = error.get();
			if (taskError != null) {
				taskError.printStackTrace();
				fail("Error in task while canceling: " + taskError.getMessage());
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testHash1MatchTask() {
		int keyCnt1 = 20;
		int valCnt1 = 1;
		
		int keyCnt2 = 10;
		int valCnt2 = 2;
				
		addInput(new UniformRecordGenerator(keyCnt1, valCnt1, false));
		addInput(new UniformRecordGenerator(keyCnt2, valCnt2, false));
		addDriverComparator(this.comparator1);
		addDriverComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
		setOutput(this.outList);
		getTaskConfig().setDriverStrategy(DriverStrategy.HYBRIDHASH_BUILD_FIRST);
		getTaskConfig().setRelativeMemoryDriver(hash_frac);
		
		JoinDriver<Record, Record, Record> testTask = new JoinDriver<>();
		
		try {
			testDriver(testTask, MockMatchStub.class);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("Test caused an exception.");
		}
		
		final int expCnt = valCnt1*valCnt2*Math.min(keyCnt1, keyCnt2);
		Assert.assertEquals("Wrong result set size.", expCnt, this.outList.size());
		this.outList.clear();
	}
	
	@Test
	public void testHash2MatchTask() {
		int keyCnt1 = 20;
		int valCnt1 = 1;
		
		int keyCnt2 = 20;
		int valCnt2 = 1;
		
		addInput(new UniformRecordGenerator(keyCnt1, valCnt1, false));
		addInput(new UniformRecordGenerator(keyCnt2, valCnt2, false));
		addDriverComparator(this.comparator1);
		addDriverComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
		setOutput(this.outList);
		getTaskConfig().setDriverStrategy(DriverStrategy.HYBRIDHASH_BUILD_SECOND);
		getTaskConfig().setRelativeMemoryDriver(hash_frac);
		
		JoinDriver<Record, Record, Record> testTask = new JoinDriver<>();
		
		try {
			testDriver(testTask, MockMatchStub.class);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("Test caused an exception.");
		}
		
		final int expCnt = valCnt1*valCnt2*Math.min(keyCnt1, keyCnt2);
		Assert.assertEquals("Wrong result set size.", expCnt, this.outList.size());
		this.outList.clear();
	}
	
	@Test
	public void testHash3MatchTask() {
		int keyCnt1 = 20;
		int valCnt1 = 1;
		
		int keyCnt2 = 20;
		int valCnt2 = 20;
		
		addInput(new UniformRecordGenerator(keyCnt1, valCnt1, false));
		addInput(new UniformRecordGenerator(keyCnt2, valCnt2, false));
		addDriverComparator(this.comparator1);
		addDriverComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
		setOutput(this.outList);
		getTaskConfig().setDriverStrategy(DriverStrategy.HYBRIDHASH_BUILD_FIRST);
		getTaskConfig().setRelativeMemoryDriver(hash_frac);
		
		JoinDriver<Record, Record, Record> testTask = new JoinDriver<>();
		
		try {
			testDriver(testTask, MockMatchStub.class);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("Test caused an exception.");
		}
		
		final int expCnt = valCnt1*valCnt2*Math.min(keyCnt1, keyCnt2);
		Assert.assertEquals("Wrong result set size.", expCnt, this.outList.size());
		this.outList.clear();
	}
	
	@Test
	public void testHash4MatchTask() {
		int keyCnt1 = 20;
		int valCnt1 = 20;
		
		int keyCnt2 = 20;
		int valCnt2 = 1;
		
		addInput(new UniformRecordGenerator(keyCnt1, valCnt1, false));
		addInput(new UniformRecordGenerator(keyCnt2, valCnt2, false));
		addDriverComparator(this.comparator1);
		addDriverComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
		setOutput(this.outList);
		getTaskConfig().setDriverStrategy(DriverStrategy.HYBRIDHASH_BUILD_SECOND);
		getTaskConfig().setRelativeMemoryDriver(hash_frac);
		
		JoinDriver<Record, Record, Record> testTask = new JoinDriver<>();
		
		try {
			testDriver(testTask, MockMatchStub.class);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("Test caused an exception.");
		}
		
		final int expCnt = valCnt1*valCnt2*Math.min(keyCnt1, keyCnt2);
		Assert.assertEquals("Wrong result set size.", expCnt, this.outList.size());
		this.outList.clear();
	}
	
	@Test
	public void testHash5MatchTask() {
		int keyCnt1 = 20;
		int valCnt1 = 20;
		
		int keyCnt2 = 20;
		int valCnt2 = 20;
		
		addInput(new UniformRecordGenerator(keyCnt1, valCnt1, false));
		addInput(new UniformRecordGenerator(keyCnt2, valCnt2, false));
		addDriverComparator(this.comparator1);
		addDriverComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
		setOutput(this.outList);
		getTaskConfig().setDriverStrategy(DriverStrategy.HYBRIDHASH_BUILD_FIRST);
		getTaskConfig().setRelativeMemoryDriver(hash_frac);
		
		JoinDriver<Record, Record, Record> testTask = new JoinDriver<>();
		
		try {
			testDriver(testTask, MockMatchStub.class);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("Test caused an exception.");
		}
		
		final int expCnt = valCnt1*valCnt2*Math.min(keyCnt1, keyCnt2);
		Assert.assertEquals("Wrong result set size.", expCnt, this.outList.size());
		this.outList.clear();
	}
	
	@Test
	public void testFailingHashFirstMatchTask() {
		int keyCnt1 = 20;
		int valCnt1 = 20;
		
		int keyCnt2 = 20;
		int valCnt2 = 20;
		
		addInput(new UniformRecordGenerator(keyCnt1, valCnt1, false));
		addInput(new UniformRecordGenerator(keyCnt2, valCnt2, false));
		addDriverComparator(this.comparator1);
		addDriverComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
		setOutput(new NirvanaOutputList());
		getTaskConfig().setDriverStrategy(DriverStrategy.HYBRIDHASH_BUILD_FIRST);
		getTaskConfig().setRelativeMemoryDriver(hash_frac);
		
		JoinDriver<Record, Record, Record> testTask = new JoinDriver<>();
		
		try {
			testDriver(testTask, MockFailingMatchStub.class);
			Assert.fail("Function exception was not forwarded.");
		} catch (ExpectedTestException etex) {
			// good!
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("Test caused an exception.");
		}
	}
	
	@Test
	public void testFailingHashSecondMatchTask() {
		int keyCnt1 = 20;
		int valCnt1 = 20;
		
		int keyCnt2 = 20;
		int valCnt2 = 20;
		
		addInput(new UniformRecordGenerator(keyCnt1, valCnt1, false));
		addInput(new UniformRecordGenerator(keyCnt2, valCnt2, false));
		addDriverComparator(this.comparator1);
		addDriverComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
		setOutput(new NirvanaOutputList());
		getTaskConfig().setDriverStrategy(DriverStrategy.HYBRIDHASH_BUILD_SECOND);
		getTaskConfig().setRelativeMemoryDriver(hash_frac);
		
		JoinDriver<Record, Record, Record> testTask = new JoinDriver<>();
		
		try {
			testDriver(testTask, MockFailingMatchStub.class);
			Assert.fail("Function exception was not forwarded.");
		} catch (ExpectedTestException etex) {
			// good!
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("Test caused an exception.");
		}
	}
	
	@Test
	public void testCancelHashMatchTaskWhileBuildFirst() {
		final int keyCnt = 20;
		final int valCnt = 20;

		try {
			addInput(new DelayingInfinitiveInputIterator(100));
			addInput(new UniformRecordGenerator(keyCnt, valCnt, false));

			addDriverComparator(this.comparator1);
			addDriverComparator(this.comparator2);

			getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());

			setOutput(new NirvanaOutputList());

			getTaskConfig().setDriverStrategy(DriverStrategy.HYBRIDHASH_BUILD_FIRST);
			getTaskConfig().setRelativeMemoryDriver(hash_frac);

			final JoinDriver<Record, Record, Record> testTask = new JoinDriver<>();

			final AtomicBoolean success = new AtomicBoolean(false);

			Thread taskRunner = new Thread() {
				@Override
				public void run() {
					try {
						testDriver(testTask, MockMatchStub.class);
						success.set(true);
					} catch (Exception ie) {
						ie.printStackTrace();
					}
				}
			};
			taskRunner.start();

			Thread.sleep(1000);
			cancel();

			try {
				taskRunner.join();
			}
			catch (InterruptedException ie) {
				Assert.fail("Joining threads failed");
			}

			Assert.assertTrue("Test threw an exception even though it was properly canceled.", success.get());
		}
		catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}
	
	@Test
	public void testHashCancelMatchTaskWhileBuildSecond() {
		final int keyCnt = 20;
		final int valCnt = 20;

		try {
			addInput(new UniformRecordGenerator(keyCnt, valCnt, false));
			addInput(new DelayingInfinitiveInputIterator(100));

			addDriverComparator(this.comparator1);
			addDriverComparator(this.comparator2);

			getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());

			setOutput(new NirvanaOutputList());

			getTaskConfig().setDriverStrategy(DriverStrategy.HYBRIDHASH_BUILD_SECOND);
			getTaskConfig().setRelativeMemoryDriver(hash_frac);

			final JoinDriver<Record, Record, Record> testTask = new JoinDriver<>();

			final AtomicBoolean success = new AtomicBoolean(false);

			Thread taskRunner = new Thread() {
				@Override
				public void run() {
					try {
						testDriver(testTask, MockMatchStub.class);
						success.set(true);
					} catch (Exception ie) {
						ie.printStackTrace();
					}
				}
			};
			taskRunner.start();

			Thread.sleep(1000);
			cancel();

			try {
				taskRunner.join();
			}
			catch (InterruptedException ie) {
				Assert.fail("Joining threads failed");
			}

			Assert.assertTrue("Test threw an exception even though it was properly canceled.", success.get());
		}
		catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}
	
	@Test
	public void testHashFirstCancelMatchTaskWhileMatching() {
		int keyCnt = 20;
		int valCnt = 20;
		
		addInput(new UniformRecordGenerator(keyCnt, valCnt, false));
		addInput(new UniformRecordGenerator(keyCnt, valCnt, false));
		addDriverComparator(this.comparator1);
		addDriverComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
		setOutput(new NirvanaOutputList());
		getTaskConfig().setDriverStrategy(DriverStrategy.HYBRIDHASH_BUILD_FIRST);
		getTaskConfig().setRelativeMemoryDriver(hash_frac);
		
		final JoinDriver<Record, Record, Record> testTask = new JoinDriver<>();
		
		final AtomicBoolean success = new AtomicBoolean(false);
		
		Thread taskRunner = new Thread() {
			@Override
			public void run() {
				try {
					testDriver(testTask, MockMatchStub.class);
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
	public void testHashSecondCancelMatchTaskWhileMatching() {
		int keyCnt = 20;
		int valCnt = 20;
		
		addInput(new UniformRecordGenerator(keyCnt, valCnt, false));
		addInput(new UniformRecordGenerator(keyCnt, valCnt, false));
		addDriverComparator(this.comparator1);
		addDriverComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
		setOutput(new NirvanaOutputList());
		getTaskConfig().setDriverStrategy(DriverStrategy.HYBRIDHASH_BUILD_SECOND);
		getTaskConfig().setRelativeMemoryDriver(hash_frac);
		
		final JoinDriver<Record, Record, Record> testTask = new JoinDriver<>();
		
		final AtomicBoolean success = new AtomicBoolean(false);
		
		Thread taskRunner = new Thread() {
			@Override
			public void run() {
				try {
					testDriver(testTask, MockMatchStub.class);
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
	
	// =================================================================================================
	
	public static final class MockMatchStub implements FlatJoinFunction<Record, Record, Record> {
		private static final long serialVersionUID = 1L;
		
		@Override
		public void join(Record record1, Record record2, Collector<Record> out) throws Exception {
			out.collect(record1);
		}
	}
	
	public static final class MockFailingMatchStub implements FlatJoinFunction<Record, Record, Record> {
		private static final long serialVersionUID = 1L;
		
		private int cnt = 0;
		
		@Override
		public void join(Record record1, Record record2, Collector<Record> out) throws Exception {
			if (++this.cnt >= 10) {
				throw new ExpectedTestException();
			}
			out.collect(record1);
		}
	}
	
	public static final class MockDelayingMatchStub implements FlatJoinFunction<Record, Record, Record> {
		private static final long serialVersionUID = 1L;
		
		@Override
		public void join(Record record1, Record record2, Collector<Record> out) throws Exception {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
			}
		}
	}
}
