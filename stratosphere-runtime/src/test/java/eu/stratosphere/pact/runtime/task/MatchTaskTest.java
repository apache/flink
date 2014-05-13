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
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.Test;

import eu.stratosphere.api.common.functions.GenericJoiner;
import eu.stratosphere.api.java.record.functions.JoinFunction;
import eu.stratosphere.api.java.typeutils.runtime.record.RecordComparator;
import eu.stratosphere.api.java.typeutils.runtime.record.RecordPairComparatorFactory;
import eu.stratosphere.pact.runtime.test.util.DelayingInfinitiveInputIterator;
import eu.stratosphere.pact.runtime.test.util.DriverTestBase;
import eu.stratosphere.pact.runtime.test.util.ExpectedTestException;
import eu.stratosphere.pact.runtime.test.util.NirvanaOutputList;
import eu.stratosphere.pact.runtime.test.util.TaskCancelThread;
import eu.stratosphere.pact.runtime.test.util.UniformRecordGenerator;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Key;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;

public class MatchTaskTest extends DriverTestBase<GenericJoiner<Record, Record, Record>>
{
	private static final long HASH_MEM = 6*1024*1024;
	
	private static final long SORT_MEM = 3*1024*1024;
	
	private static final long BNLJN_MEM = 10 * PAGE_SIZE;
	
	@SuppressWarnings("unchecked")
	private final RecordComparator comparator1 = new RecordComparator(
		new int[]{0}, (Class<? extends Key<?>>[])new Class[]{ IntValue.class });
	
	@SuppressWarnings("unchecked")
	private final RecordComparator comparator2 = new RecordComparator(
		new int[]{0}, (Class<? extends Key<?>>[])new Class[]{ IntValue.class });
	
	private final List<Record> outList = new ArrayList<Record>();
	
	
	public MatchTaskTest() {
		super(HASH_MEM, 2, SORT_MEM);
	}
	
	
	@Test
	public void testSortBoth1MatchTask() {
		final int keyCnt1 = 20;
		final int valCnt1 = 1;
		
		final int keyCnt2 = 10;
		final int valCnt2 = 2;
		
		setOutput(this.outList);
		addInputComparator(this.comparator1);
		addInputComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
		getTaskConfig().setDriverStrategy(DriverStrategy.MERGE);
		getTaskConfig().setMemoryDriver(BNLJN_MEM);
		setNumFileHandlesForSort(4);
		
		final MatchDriver<Record, Record, Record> testTask = new MatchDriver<Record, Record, Record>();
		
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
		addInputComparator(this.comparator1);
		addInputComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
		getTaskConfig().setDriverStrategy(DriverStrategy.MERGE);
		getTaskConfig().setMemoryDriver(BNLJN_MEM);
		setNumFileHandlesForSort(4);
		
		final MatchDriver<Record, Record, Record> testTask = new MatchDriver<Record, Record, Record>();
		
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
		addInputComparator(this.comparator1);
		addInputComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
		getTaskConfig().setDriverStrategy(DriverStrategy.MERGE);
		getTaskConfig().setMemoryDriver(BNLJN_MEM);
		setNumFileHandlesForSort(4);
		
		final MatchDriver<Record, Record, Record> testTask = new MatchDriver<Record, Record, Record>();
		
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
		addInputComparator(this.comparator1);
		addInputComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
		getTaskConfig().setDriverStrategy(DriverStrategy.MERGE);
		getTaskConfig().setMemoryDriver(BNLJN_MEM);
		setNumFileHandlesForSort(4);
		
		final MatchDriver<Record, Record, Record> testTask = new MatchDriver<Record, Record, Record>();
		
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
		addInputComparator(this.comparator1);
		addInputComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
		getTaskConfig().setDriverStrategy(DriverStrategy.MERGE);
		getTaskConfig().setMemoryDriver(BNLJN_MEM);
		setNumFileHandlesForSort(4);
		
		final MatchDriver<Record, Record, Record> testTask = new MatchDriver<Record, Record, Record>();
		
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
		addInputComparator(this.comparator1);
		addInputComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
		getTaskConfig().setDriverStrategy(DriverStrategy.MERGE);
		getTaskConfig().setMemoryDriver(BNLJN_MEM);
		setNumFileHandlesForSort(4);
		
		final MatchDriver<Record, Record, Record> testTask = new MatchDriver<Record, Record, Record>();
		
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
		addInputComparator(this.comparator1);
		addInputComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
		getTaskConfig().setDriverStrategy(DriverStrategy.MERGE);
		getTaskConfig().setMemoryDriver(BNLJN_MEM);
		setNumFileHandlesForSort(4);
		
		final MatchDriver<Record, Record, Record> testTask = new MatchDriver<Record, Record, Record>();
		
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
		addInputComparator(this.comparator1);
		addInputComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
		getTaskConfig().setDriverStrategy(DriverStrategy.MERGE);
		getTaskConfig().setMemoryDriver(BNLJN_MEM);
		setNumFileHandlesForSort(4);
		
		final MatchDriver<Record, Record, Record> testTask = new MatchDriver<Record, Record, Record>();
		
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
		addInputComparator(this.comparator1);
		addInputComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
		getTaskConfig().setDriverStrategy(DriverStrategy.MERGE);
		getTaskConfig().setMemoryDriver(BNLJN_MEM);
		setNumFileHandlesForSort(4);
		
		final MatchDriver<Record, Record, Record> testTask = new MatchDriver<Record, Record, Record>();
		
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
		int keyCnt = 20;
		int valCnt = 20;
		
		setOutput(new NirvanaOutputList());
		addInputComparator(this.comparator1);
		addInputComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
		getTaskConfig().setDriverStrategy(DriverStrategy.MERGE);
		getTaskConfig().setMemoryDriver(BNLJN_MEM);
		setNumFileHandlesForSort(4);
		
		final MatchDriver<Record, Record, Record> testTask = new MatchDriver<Record, Record, Record>();
		
		try {
			addInputSorted(new DelayingInfinitiveInputIterator(100), this.comparator1.duplicate());
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
	public void testCancelMatchTaskWhileSort2() {
		int keyCnt = 20;
		int valCnt = 20;
		
		setOutput(new NirvanaOutputList());
		addInputComparator(this.comparator1);
		addInputComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
		getTaskConfig().setDriverStrategy(DriverStrategy.MERGE);
		getTaskConfig().setMemoryDriver(BNLJN_MEM);
		setNumFileHandlesForSort(4);
		
		final MatchDriver<Record, Record, Record> testTask = new MatchDriver<Record, Record, Record>();
		
		try {
			addInput(new UniformRecordGenerator(keyCnt, valCnt, true));
			addInputSorted(new DelayingInfinitiveInputIterator(100), this.comparator1.duplicate());
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("The test caused an exception.");
		}
		
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
	public void testCancelMatchTaskWhileMatching() {
		int keyCnt = 20;
		int valCnt = 20;
		
		setOutput(new NirvanaOutputList());
		addInputComparator(this.comparator1);
		addInputComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
		getTaskConfig().setDriverStrategy(DriverStrategy.MERGE);
		getTaskConfig().setMemoryDriver(BNLJN_MEM);
		setNumFileHandlesForSort(4);
		
		final MatchDriver<Record, Record, Record> testTask = new MatchDriver<Record, Record, Record>();
		
		addInput(new UniformRecordGenerator(keyCnt, valCnt, true));
		addInput(new UniformRecordGenerator(keyCnt, valCnt, true));
		
		final AtomicBoolean success = new AtomicBoolean(false);
		
		Thread taskRunner = new Thread() {
			@Override
			public void run() {
				try {
					testDriver(testTask, MockDelayingMatchStub.class);
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
	public void testHash1MatchTask() {
		int keyCnt1 = 20;
		int valCnt1 = 1;
		
		int keyCnt2 = 10;
		int valCnt2 = 2;
				
		addInput(new UniformRecordGenerator(keyCnt1, valCnt1, false));
		addInput(new UniformRecordGenerator(keyCnt2, valCnt2, false));
		addInputComparator(this.comparator1);
		addInputComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
		setOutput(this.outList);
		getTaskConfig().setDriverStrategy(DriverStrategy.HYBRIDHASH_BUILD_FIRST);
		getTaskConfig().setMemoryDriver(HASH_MEM);
		
		MatchDriver<Record, Record, Record> testTask = new MatchDriver<Record, Record, Record>();
		
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
		addInputComparator(this.comparator1);
		addInputComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
		setOutput(this.outList);
		getTaskConfig().setDriverStrategy(DriverStrategy.HYBRIDHASH_BUILD_SECOND);
		getTaskConfig().setMemoryDriver(HASH_MEM);
		
		MatchDriver<Record, Record, Record> testTask = new MatchDriver<Record, Record, Record>();
		
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
		addInputComparator(this.comparator1);
		addInputComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
		setOutput(this.outList);
		getTaskConfig().setDriverStrategy(DriverStrategy.HYBRIDHASH_BUILD_FIRST);
		getTaskConfig().setMemoryDriver(HASH_MEM);
		
		MatchDriver<Record, Record, Record> testTask = new MatchDriver<Record, Record, Record>();
		
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
		addInputComparator(this.comparator1);
		addInputComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
		setOutput(this.outList);
		getTaskConfig().setDriverStrategy(DriverStrategy.HYBRIDHASH_BUILD_SECOND);
		getTaskConfig().setMemoryDriver(HASH_MEM);
		
		MatchDriver<Record, Record, Record> testTask = new MatchDriver<Record, Record, Record>();
		
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
		addInputComparator(this.comparator1);
		addInputComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
		setOutput(this.outList);
		getTaskConfig().setDriverStrategy(DriverStrategy.HYBRIDHASH_BUILD_FIRST);
		getTaskConfig().setMemoryDriver(HASH_MEM);
		
		MatchDriver<Record, Record, Record> testTask = new MatchDriver<Record, Record, Record>();
		
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
		addInputComparator(this.comparator1);
		addInputComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
		setOutput(new NirvanaOutputList());
		getTaskConfig().setDriverStrategy(DriverStrategy.HYBRIDHASH_BUILD_FIRST);
		getTaskConfig().setMemoryDriver(HASH_MEM);
		
		MatchDriver<Record, Record, Record> testTask = new MatchDriver<Record, Record, Record>();
		
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
		addInputComparator(this.comparator1);
		addInputComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
		setOutput(new NirvanaOutputList());
		getTaskConfig().setDriverStrategy(DriverStrategy.HYBRIDHASH_BUILD_SECOND);
		getTaskConfig().setMemoryDriver(HASH_MEM);
		
		MatchDriver<Record, Record, Record> testTask = new MatchDriver<Record, Record, Record>();
		
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
		int keyCnt = 20;
		int valCnt = 20;
		
		addInput(new DelayingInfinitiveInputIterator(100));
		addInput(new UniformRecordGenerator(keyCnt, valCnt, false));
		
		addInputComparator(this.comparator1);
		addInputComparator(this.comparator2);
		
		getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
		
		setOutput(new NirvanaOutputList());
		
		getTaskConfig().setDriverStrategy(DriverStrategy.HYBRIDHASH_BUILD_FIRST);
		getTaskConfig().setMemoryDriver(HASH_MEM);
		
		final MatchDriver<Record, Record, Record> testTask = new MatchDriver<Record, Record, Record>();
		
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
	public void testHashCancelMatchTaskWhileBuildSecond() {
		int keyCnt = 20;
		int valCnt = 20;
		
		addInput(new UniformRecordGenerator(keyCnt, valCnt, false));
		addInput(new DelayingInfinitiveInputIterator(100));
		addInputComparator(this.comparator1);
		addInputComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
		setOutput(new NirvanaOutputList());
		getTaskConfig().setDriverStrategy(DriverStrategy.HYBRIDHASH_BUILD_SECOND);
		getTaskConfig().setMemoryDriver(HASH_MEM);
		
		final MatchDriver<Record, Record, Record> testTask = new MatchDriver<Record, Record, Record>();
		
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
	public void testHashFirstCancelMatchTaskWhileMatching() {
		int keyCnt = 20;
		int valCnt = 20;
		
		addInput(new UniformRecordGenerator(keyCnt, valCnt, false));
		addInput(new UniformRecordGenerator(keyCnt, valCnt, false));
		addInputComparator(this.comparator1);
		addInputComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
		setOutput(new NirvanaOutputList());
		getTaskConfig().setDriverStrategy(DriverStrategy.HYBRIDHASH_BUILD_FIRST);
		getTaskConfig().setMemoryDriver(HASH_MEM);
		
		final MatchDriver<Record, Record, Record> testTask = new MatchDriver<Record, Record, Record>();
		
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
		addInputComparator(this.comparator1);
		addInputComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
		setOutput(new NirvanaOutputList());
		getTaskConfig().setDriverStrategy(DriverStrategy.HYBRIDHASH_BUILD_SECOND);
		getTaskConfig().setMemoryDriver(HASH_MEM);
		
		final MatchDriver<Record, Record, Record> testTask = new MatchDriver<Record, Record, Record>();
		
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
	
	public static final class MockMatchStub extends JoinFunction {
		private static final long serialVersionUID = 1L;
		
		@Override
		public void join(Record record1, Record record2, Collector<Record> out) throws Exception {
			out.collect(record1);
		}
	}
	
	public static final class MockFailingMatchStub extends JoinFunction {
		private static final long serialVersionUID = 1L;
		
		private int cnt = 0;
		
		@Override
		public void join(Record record1, Record record2, Collector<Record> out) {
			if (++this.cnt >= 10) {
				throw new ExpectedTestException();
			}
			
			out.collect(record1);
		}
	}
	
	public static final class MockDelayingMatchStub extends JoinFunction {
		private static final long serialVersionUID = 1L;
		
		@Override
		public void join(Record record1, Record record2, Collector<Record> out) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) { }
		}
	}
}
