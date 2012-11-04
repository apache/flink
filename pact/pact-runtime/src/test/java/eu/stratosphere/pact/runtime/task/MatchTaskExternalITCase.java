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
import eu.stratosphere.pact.runtime.test.util.DriverTestBase;
import eu.stratosphere.pact.runtime.test.util.UniformPactRecordGenerator;

public class MatchTaskExternalITCase extends DriverTestBase<GenericMatcher<PactRecord, PactRecord, PactRecord>>
{
	private static final long HASH_MEM = 4*1024*1024;
	
	private static final long SORT_MEM = 3*1024*1024;
	
	private static final long BNLJN_MEM = 10 * PAGE_SIZE;
	
	@SuppressWarnings("unchecked")
	private final PactRecordComparator comparator1 = new PactRecordComparator(
		new int[]{0}, (Class<? extends Key>[])new Class[]{ PactInteger.class });
	
	@SuppressWarnings("unchecked")
	private final PactRecordComparator comparator2 = new PactRecordComparator(
		new int[]{0}, (Class<? extends Key>[])new Class[]{ PactInteger.class });
	
	private final CountingOutputCollector output = new CountingOutputCollector();
	
	public MatchTaskExternalITCase() {
		super(HASH_MEM, 2, SORT_MEM);
	}
	
	@Test
	public void testExternalSort1MatchTask() {
		final int keyCnt1 = 16384*4;
		final int valCnt1 = 2;
		
		final int keyCnt2 = 8192;
		final int valCnt2 = 4*2;
		
		final int expCnt = valCnt1*valCnt2*Math.min(keyCnt1, keyCnt2);
		
		setOutput(this.output);
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
		
		Assert.assertEquals("Wrong result set size.", expCnt, this.output.getNumberOfRecords());
	}
	
	@Test
	public void testExternalHash1MatchTask() {
		final int keyCnt1 = 32768;
		final int valCnt1 = 8;
		
		final int keyCnt2 = 65536;
		final int valCnt2 = 8;
		
		final int expCnt = valCnt1*valCnt2*Math.min(keyCnt1, keyCnt2);
		
		addInput(new UniformPactRecordGenerator(keyCnt1, valCnt1, false));
		addInput(new UniformPactRecordGenerator(keyCnt2, valCnt2, false));
		addInputComparator(this.comparator1);
		addInputComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(PactRecordPairComparatorFactory.get());
		setOutput(this.output);
		getTaskConfig().setDriverStrategy(DriverStrategy.HYBRIDHASH_FIRST);
		getTaskConfig().setMemoryDriver(HASH_MEM);
		
		MatchDriver<PactRecord, PactRecord, PactRecord> testTask = new MatchDriver<PactRecord, PactRecord, PactRecord>();
		
		try {
			testDriver(testTask, MockMatchStub.class);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("Test caused an exception.");
		}
		
		Assert.assertEquals("Wrong result set size.", expCnt, this.output.getNumberOfRecords());
	}
	
	@Test
	public void testExternalHash2MatchTask() {
		final int keyCnt1 = 32768;
		final int valCnt1 = 8;
		
		final int keyCnt2 = 65536;
		final int valCnt2 = 8;
		
		final int expCnt = valCnt1*valCnt2*Math.min(keyCnt1, keyCnt2);
		
		addInput(new UniformPactRecordGenerator(keyCnt1, valCnt1, false));
		addInput(new UniformPactRecordGenerator(keyCnt2, valCnt2, false));
		addInputComparator(this.comparator1);
		addInputComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(PactRecordPairComparatorFactory.get());
		setOutput(this.output);
		getTaskConfig().setDriverStrategy(DriverStrategy.HYBRIDHASH_FIRST);
		getTaskConfig().setMemoryDriver(HASH_MEM);
		
		MatchDriver<PactRecord, PactRecord, PactRecord> testTask = new MatchDriver<PactRecord, PactRecord, PactRecord>();
		
		try {
			testDriver(testTask, MockMatchStub.class);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("Test caused an exception.");
		}
		
		Assert.assertEquals("Wrong result set size.", expCnt, this.output.getNumberOfRecords());
	}
	
	public static final class MockMatchStub extends MatchStub
	{
		@Override
		public void match(PactRecord value1, PactRecord value2, Collector<PactRecord> out) throws Exception {
			out.collect(value1);
		}
	}
}
