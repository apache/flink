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
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import junit.framework.Assert;

import org.junit.Test;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.generic.stub.GenericMatcher;
import eu.stratosphere.pact.runtime.plugable.PactRecordComparator;
import eu.stratosphere.pact.runtime.plugable.PactRecordPairComparatorFactory;
import eu.stratosphere.pact.runtime.test.util.DelayingInfinitiveInputIterator;
import eu.stratosphere.pact.runtime.test.util.DriverTestBase;
import eu.stratosphere.pact.runtime.test.util.ExpectedTestException;
import eu.stratosphere.pact.runtime.test.util.NirvanaOutputList;
import eu.stratosphere.pact.runtime.test.util.UniformPactRecordGenerator;
import eu.stratosphere.pact.runtime.test.util.TaskCancelThread;

public class MatchTaskTest extends DriverTestBase<GenericMatcher<PactRecord, PactRecord, PactRecord>>
{
	private static final long HASH_MEM = 6*1024*1024;
	
	private static final long SORT_MEM = 3*1024*1024;
	
	private static final long BNLJN_MEM = 10 * PAGE_SIZE;
	
	@SuppressWarnings("unchecked")
	private final PactRecordComparator comparator1 = new PactRecordComparator(
		new int[]{0}, (Class<? extends Key>[])new Class[]{ PactInteger.class });
	
	@SuppressWarnings("unchecked")
	private final PactRecordComparator comparator2 = new PactRecordComparator(
		new int[]{0}, (Class<? extends Key>[])new Class[]{ PactInteger.class });
	
	private final List<PactRecord> outList = new ArrayList<PactRecord>();
	
	
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
		getTaskConfig().setDriverPairComparator(PactRecordPairComparatorFactory.get());
		getTaskConfig().setDriverStrategy(DriverStrategy.MERGE);
		getTaskConfig().setMemoryDriver(BNLJN_MEM);
		setNumFileHandlesForSort(4);
		
		final MatchDriver<PactRecord, PactRecord, PactRecord> testTask = new MatchDriver<PactRecord, PactRecord, PactRecord>();
		
		try {
			addInputSorted(new UniformPactRecordGenerator(keyCnt1, valCnt1, false), this.comparator1.duplicate());
			addInputSorted(new UniformPactRecordGenerator(keyCnt2, valCnt2, false), this.comparator2.duplicate());
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
		getTaskConfig().setDriverPairComparator(PactRecordPairComparatorFactory.get());
		getTaskConfig().setDriverStrategy(DriverStrategy.MERGE);
		getTaskConfig().setMemoryDriver(BNLJN_MEM);
		setNumFileHandlesForSort(4);
		
		final MatchDriver<PactRecord, PactRecord, PactRecord> testTask = new MatchDriver<PactRecord, PactRecord, PactRecord>();
		
		try {
			addInputSorted(new UniformPactRecordGenerator(keyCnt1, valCnt1, false), this.comparator1.duplicate());
			addInputSorted(new UniformPactRecordGenerator(keyCnt2, valCnt2, false), this.comparator2.duplicate());
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
		getTaskConfig().setDriverPairComparator(PactRecordPairComparatorFactory.get());
		getTaskConfig().setDriverStrategy(DriverStrategy.MERGE);
		getTaskConfig().setMemoryDriver(BNLJN_MEM);
		setNumFileHandlesForSort(4);
		
		final MatchDriver<PactRecord, PactRecord, PactRecord> testTask = new MatchDriver<PactRecord, PactRecord, PactRecord>();
		
		try {
			addInputSorted(new UniformPactRecordGenerator(keyCnt1, valCnt1, false), this.comparator1.duplicate());
			addInputSorted(new UniformPactRecordGenerator(keyCnt2, valCnt2, false), this.comparator2.duplicate());
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
		getTaskConfig().setDriverPairComparator(PactRecordPairComparatorFactory.get());
		getTaskConfig().setDriverStrategy(DriverStrategy.MERGE);
		getTaskConfig().setMemoryDriver(BNLJN_MEM);
		setNumFileHandlesForSort(4);
		
		final MatchDriver<PactRecord, PactRecord, PactRecord> testTask = new MatchDriver<PactRecord, PactRecord, PactRecord>();
		
		try {
			addInputSorted(new UniformPactRecordGenerator(keyCnt1, valCnt1, false), this.comparator1.duplicate());
			addInputSorted(new UniformPactRecordGenerator(keyCnt2, valCnt2, false), this.comparator2.duplicate());
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
		getTaskConfig().setDriverPairComparator(PactRecordPairComparatorFactory.get());
		getTaskConfig().setDriverStrategy(DriverStrategy.MERGE);
		getTaskConfig().setMemoryDriver(BNLJN_MEM);
		setNumFileHandlesForSort(4);
		
		final MatchDriver<PactRecord, PactRecord, PactRecord> testTask = new MatchDriver<PactRecord, PactRecord, PactRecord>();
		
		try {
			addInputSorted(new UniformPactRecordGenerator(keyCnt1, valCnt1, false), this.comparator1.duplicate());
			addInputSorted(new UniformPactRecordGenerator(keyCnt2, valCnt2, false), this.comparator2.duplicate());
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
		getTaskConfig().setDriverPairComparator(PactRecordPairComparatorFactory.get());
		getTaskConfig().setDriverStrategy(DriverStrategy.MERGE);
		getTaskConfig().setMemoryDriver(BNLJN_MEM);
		setNumFileHandlesForSort(4);
		
		final MatchDriver<PactRecord, PactRecord, PactRecord> testTask = new MatchDriver<PactRecord, PactRecord, PactRecord>();
		
		try {
			addInputSorted(new UniformPactRecordGenerator(keyCnt1, valCnt1, false), this.comparator1.duplicate());
			addInput(new UniformPactRecordGenerator(keyCnt2, valCnt2, true));
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
		getTaskConfig().setDriverPairComparator(PactRecordPairComparatorFactory.get());
		getTaskConfig().setDriverStrategy(DriverStrategy.MERGE);
		getTaskConfig().setMemoryDriver(BNLJN_MEM);
		setNumFileHandlesForSort(4);
		
		final MatchDriver<PactRecord, PactRecord, PactRecord> testTask = new MatchDriver<PactRecord, PactRecord, PactRecord>();
		
		try {
			addInput(new UniformPactRecordGenerator(keyCnt1, valCnt1, true));
			addInputSorted(new UniformPactRecordGenerator(keyCnt2, valCnt2, false), this.comparator2.duplicate());
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
		getTaskConfig().setDriverPairComparator(PactRecordPairComparatorFactory.get());
		getTaskConfig().setDriverStrategy(DriverStrategy.MERGE);
		getTaskConfig().setMemoryDriver(BNLJN_MEM);
		setNumFileHandlesForSort(4);
		
		final MatchDriver<PactRecord, PactRecord, PactRecord> testTask = new MatchDriver<PactRecord, PactRecord, PactRecord>();
		
		addInput(new UniformPactRecordGenerator(keyCnt1, valCnt1, true));
		addInput(new UniformPactRecordGenerator(keyCnt2, valCnt2, true));
		
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
		getTaskConfig().setDriverPairComparator(PactRecordPairComparatorFactory.get());
		getTaskConfig().setDriverStrategy(DriverStrategy.MERGE);
		getTaskConfig().setMemoryDriver(BNLJN_MEM);
		setNumFileHandlesForSort(4);
		
		final MatchDriver<PactRecord, PactRecord, PactRecord> testTask = new MatchDriver<PactRecord, PactRecord, PactRecord>();
		
		addInput(new UniformPactRecordGenerator(keyCnt1, valCnt1, true));
		addInput(new UniformPactRecordGenerator(keyCnt2, valCnt2, true));
		
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
		getTaskConfig().setDriverPairComparator(PactRecordPairComparatorFactory.get());
		getTaskConfig().setDriverStrategy(DriverStrategy.MERGE);
		getTaskConfig().setMemoryDriver(BNLJN_MEM);
		setNumFileHandlesForSort(4);
		
		final MatchDriver<PactRecord, PactRecord, PactRecord> testTask = new MatchDriver<PactRecord, PactRecord, PactRecord>();
		
		try {
			addInputSorted(new DelayingInfinitiveInputIterator(100), this.comparator1.duplicate());
			addInput(new UniformPactRecordGenerator(keyCnt, valCnt, true));
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
		getTaskConfig().setDriverPairComparator(PactRecordPairComparatorFactory.get());
		getTaskConfig().setDriverStrategy(DriverStrategy.MERGE);
		getTaskConfig().setMemoryDriver(BNLJN_MEM);
		setNumFileHandlesForSort(4);
		
		final MatchDriver<PactRecord, PactRecord, PactRecord> testTask = new MatchDriver<PactRecord, PactRecord, PactRecord>();
		
		try {
			addInput(new UniformPactRecordGenerator(keyCnt, valCnt, true));
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
		getTaskConfig().setDriverPairComparator(PactRecordPairComparatorFactory.get());
		getTaskConfig().setDriverStrategy(DriverStrategy.MERGE);
		getTaskConfig().setMemoryDriver(BNLJN_MEM);
		setNumFileHandlesForSort(4);
		
		final MatchDriver<PactRecord, PactRecord, PactRecord> testTask = new MatchDriver<PactRecord, PactRecord, PactRecord>();
		
		addInput(new UniformPactRecordGenerator(keyCnt, valCnt, true));
		addInput(new UniformPactRecordGenerator(keyCnt, valCnt, true));
		
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
				
		addInput(new UniformPactRecordGenerator(keyCnt1, valCnt1, false));
		addInput(new UniformPactRecordGenerator(keyCnt2, valCnt2, false));
		addInputComparator(this.comparator1);
		addInputComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(PactRecordPairComparatorFactory.get());
		setOutput(this.outList);
		getTaskConfig().setDriverStrategy(DriverStrategy.HYBRIDHASH_FIRST);
		getTaskConfig().setMemoryDriver(HASH_MEM);
		
		MatchDriver<PactRecord, PactRecord, PactRecord> testTask = new MatchDriver<PactRecord, PactRecord, PactRecord>();
		
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
		
		addInput(new UniformPactRecordGenerator(keyCnt1, valCnt1, false));
		addInput(new UniformPactRecordGenerator(keyCnt2, valCnt2, false));
		addInputComparator(this.comparator1);
		addInputComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(PactRecordPairComparatorFactory.get());
		setOutput(this.outList);
		getTaskConfig().setDriverStrategy(DriverStrategy.HYBRIDHASH_SECOND);
		getTaskConfig().setMemoryDriver(HASH_MEM);
		
		MatchDriver<PactRecord, PactRecord, PactRecord> testTask = new MatchDriver<PactRecord, PactRecord, PactRecord>();
		
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
		
		addInput(new UniformPactRecordGenerator(keyCnt1, valCnt1, false));
		addInput(new UniformPactRecordGenerator(keyCnt2, valCnt2, false));
		addInputComparator(this.comparator1);
		addInputComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(PactRecordPairComparatorFactory.get());
		setOutput(this.outList);
		getTaskConfig().setDriverStrategy(DriverStrategy.HYBRIDHASH_FIRST);
		getTaskConfig().setMemoryDriver(HASH_MEM);
		
		MatchDriver<PactRecord, PactRecord, PactRecord> testTask = new MatchDriver<PactRecord, PactRecord, PactRecord>();
		
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
		
		addInput(new UniformPactRecordGenerator(keyCnt1, valCnt1, false));
		addInput(new UniformPactRecordGenerator(keyCnt2, valCnt2, false));
		addInputComparator(this.comparator1);
		addInputComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(PactRecordPairComparatorFactory.get());
		setOutput(this.outList);
		getTaskConfig().setDriverStrategy(DriverStrategy.HYBRIDHASH_SECOND);
		getTaskConfig().setMemoryDriver(HASH_MEM);
		
		MatchDriver<PactRecord, PactRecord, PactRecord> testTask = new MatchDriver<PactRecord, PactRecord, PactRecord>();
		
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
		
		addInput(new UniformPactRecordGenerator(keyCnt1, valCnt1, false));
		addInput(new UniformPactRecordGenerator(keyCnt2, valCnt2, false));
		addInputComparator(this.comparator1);
		addInputComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(PactRecordPairComparatorFactory.get());
		setOutput(this.outList);
		getTaskConfig().setDriverStrategy(DriverStrategy.HYBRIDHASH_FIRST);
		getTaskConfig().setMemoryDriver(HASH_MEM);
		
		MatchDriver<PactRecord, PactRecord, PactRecord> testTask = new MatchDriver<PactRecord, PactRecord, PactRecord>();
		
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
		
		addInput(new UniformPactRecordGenerator(keyCnt1, valCnt1, false));
		addInput(new UniformPactRecordGenerator(keyCnt2, valCnt2, false));
		addInputComparator(this.comparator1);
		addInputComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(PactRecordPairComparatorFactory.get());
		setOutput(new NirvanaOutputList());
		getTaskConfig().setDriverStrategy(DriverStrategy.HYBRIDHASH_FIRST);
		getTaskConfig().setMemoryDriver(HASH_MEM);
		
		MatchDriver<PactRecord, PactRecord, PactRecord> testTask = new MatchDriver<PactRecord, PactRecord, PactRecord>();
		
		try {
			testDriver(testTask, MockFailingMatchStub.class);
			Assert.fail("Stub exception was not forwarded.");
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
		
		addInput(new UniformPactRecordGenerator(keyCnt1, valCnt1, false));
		addInput(new UniformPactRecordGenerator(keyCnt2, valCnt2, false));
		addInputComparator(this.comparator1);
		addInputComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(PactRecordPairComparatorFactory.get());
		setOutput(new NirvanaOutputList());
		getTaskConfig().setDriverStrategy(DriverStrategy.HYBRIDHASH_SECOND);
		getTaskConfig().setMemoryDriver(HASH_MEM);
		
		MatchDriver<PactRecord, PactRecord, PactRecord> testTask = new MatchDriver<PactRecord, PactRecord, PactRecord>();
		
		try {
			testDriver(testTask, MockFailingMatchStub.class);
			Assert.fail("Stub exception was not forwarded.");
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
		addInput(new UniformPactRecordGenerator(keyCnt, valCnt, false));
		addInputComparator(this.comparator1);
		addInputComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(PactRecordPairComparatorFactory.get());
		setOutput(new NirvanaOutputList());
		getTaskConfig().setDriverStrategy(DriverStrategy.HYBRIDHASH_FIRST);
		getTaskConfig().setMemoryDriver(HASH_MEM);
		
		final MatchDriver<PactRecord, PactRecord, PactRecord> testTask = new MatchDriver<PactRecord, PactRecord, PactRecord>();
		
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
		
		addInput(new UniformPactRecordGenerator(keyCnt, valCnt, false));
		addInput(new DelayingInfinitiveInputIterator(100));
		addInputComparator(this.comparator1);
		addInputComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(PactRecordPairComparatorFactory.get());
		setOutput(new NirvanaOutputList());
		getTaskConfig().setDriverStrategy(DriverStrategy.HYBRIDHASH_SECOND);
		getTaskConfig().setMemoryDriver(HASH_MEM);
		
		final MatchDriver<PactRecord, PactRecord, PactRecord> testTask = new MatchDriver<PactRecord, PactRecord, PactRecord>();
		
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
		
		addInput(new UniformPactRecordGenerator(keyCnt, valCnt, false));
		addInput(new UniformPactRecordGenerator(keyCnt, valCnt, false));
		addInputComparator(this.comparator1);
		addInputComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(PactRecordPairComparatorFactory.get());
		setOutput(new NirvanaOutputList());
		getTaskConfig().setDriverStrategy(DriverStrategy.HYBRIDHASH_FIRST);
		getTaskConfig().setMemoryDriver(HASH_MEM);
		
		final MatchDriver<PactRecord, PactRecord, PactRecord> testTask = new MatchDriver<PactRecord, PactRecord, PactRecord>();
		
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
		
		addInput(new UniformPactRecordGenerator(keyCnt, valCnt, false));
		addInput(new UniformPactRecordGenerator(keyCnt, valCnt, false));
		addInputComparator(this.comparator1);
		addInputComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(PactRecordPairComparatorFactory.get());
		setOutput(new NirvanaOutputList());
		getTaskConfig().setDriverStrategy(DriverStrategy.HYBRIDHASH_SECOND);
		getTaskConfig().setMemoryDriver(HASH_MEM);
		
		final MatchDriver<PactRecord, PactRecord, PactRecord> testTask = new MatchDriver<PactRecord, PactRecord, PactRecord>();
		
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
	
	public static final class MockMatchStub extends MatchStub
	{
		@Override
		public void match(PactRecord record1, PactRecord record2, Collector<PactRecord> out) throws Exception {
			out.collect(record1);
		}
	}
	
	public static final class MockFailingMatchStub extends MatchStub
	{
		private int cnt = 0;
		
		@Override
		public void match(PactRecord record1, PactRecord record2, Collector<PactRecord> out) {
			if (++this.cnt >= 10) {
				throw new ExpectedTestException();
			}
			
			out.collect(record1);
		}
	}
	
	public static final class MockDelayingMatchStub extends MatchStub
	{
		@Override
		public void match(PactRecord record1, PactRecord record2, Collector<PactRecord> out) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) { }
		}
	}
}
