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

import junit.framework.Assert;

import org.junit.Test;

import eu.stratosphere.api.common.functions.GenericJoiner;
import eu.stratosphere.api.java.record.functions.JoinFunction;
import eu.stratosphere.pact.runtime.plugable.pactrecord.RecordComparator;
import eu.stratosphere.pact.runtime.plugable.pactrecord.RecordPairComparatorFactory;
import eu.stratosphere.pact.runtime.test.util.DriverTestBase;
import eu.stratosphere.pact.runtime.test.util.UniformRecordGenerator;
import eu.stratosphere.types.Key;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;

public class MatchTaskExternalITCase extends DriverTestBase<GenericJoiner<Record, Record, Record>>
{
	private static final long HASH_MEM = 4*1024*1024;
	
	private static final long SORT_MEM = 3*1024*1024;
	
	private static final long BNLJN_MEM = 10 * PAGE_SIZE;
	
	@SuppressWarnings("unchecked")
	private final RecordComparator comparator1 = new RecordComparator(
		new int[]{0}, (Class<? extends Key>[])new Class[]{ IntValue.class });
	
	@SuppressWarnings("unchecked")
	private final RecordComparator comparator2 = new RecordComparator(
		new int[]{0}, (Class<? extends Key>[])new Class[]{ IntValue.class });
	
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
		
		Assert.assertEquals("Wrong result set size.", expCnt, this.output.getNumberOfRecords());
	}
	
	@Test
	public void testExternalHash1MatchTask() {
		final int keyCnt1 = 32768;
		final int valCnt1 = 8;
		
		final int keyCnt2 = 65536;
		final int valCnt2 = 8;
		
		final int expCnt = valCnt1*valCnt2*Math.min(keyCnt1, keyCnt2);
		
		addInput(new UniformRecordGenerator(keyCnt1, valCnt1, false));
		addInput(new UniformRecordGenerator(keyCnt2, valCnt2, false));
		addInputComparator(this.comparator1);
		addInputComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
		setOutput(this.output);
		getTaskConfig().setDriverStrategy(DriverStrategy.HYBRIDHASH_BUILD_FIRST);
		getTaskConfig().setMemoryDriver(HASH_MEM);
		
		MatchDriver<Record, Record, Record> testTask = new MatchDriver<Record, Record, Record>();
		
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
		
		addInput(new UniformRecordGenerator(keyCnt1, valCnt1, false));
		addInput(new UniformRecordGenerator(keyCnt2, valCnt2, false));
		addInputComparator(this.comparator1);
		addInputComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
		setOutput(this.output);
		getTaskConfig().setDriverStrategy(DriverStrategy.HYBRIDHASH_BUILD_SECOND);
		getTaskConfig().setMemoryDriver(HASH_MEM);
		
		MatchDriver<Record, Record, Record> testTask = new MatchDriver<Record, Record, Record>();
		
		try {
			testDriver(testTask, MockMatchStub.class);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("Test caused an exception.");
		}
		
		Assert.assertEquals("Wrong result set size.", expCnt, this.output.getNumberOfRecords());
	}
	
	public static final class MockMatchStub extends JoinFunction
	{
		@Override
		public void match(Record value1, Record value2, Collector<Record> out) throws Exception {
			out.collect(value1);
		}
	}
}
