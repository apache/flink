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

import org.junit.Assert;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.typeutils.record.RecordComparator;
import org.apache.flink.api.common.typeutils.record.RecordPairComparatorFactory;
import org.apache.flink.api.java.record.functions.JoinFunction;
import org.apache.flink.runtime.operators.testutils.DriverTestBase;
import org.apache.flink.runtime.operators.testutils.UniformRecordGenerator;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.Key;
import org.apache.flink.types.Record;
import org.apache.flink.util.Collector;
import org.junit.Test;

@SuppressWarnings("deprecation")
public class MatchTaskExternalITCase extends DriverTestBase<FlatJoinFunction<Record, Record, Record>> {
	
	private static final long HASH_MEM = 4*1024*1024;
	
	private static final long SORT_MEM = 3*1024*1024;
	
	private static final long BNLJN_MEM = 10 * PAGE_SIZE;

	private final double bnljn_frac;

	private final double hash_frac;

	@SuppressWarnings("unchecked")
	private final RecordComparator comparator1 = new RecordComparator(
		new int[]{0}, (Class<? extends Key<?>>[])new Class[]{ IntValue.class });
	
	@SuppressWarnings("unchecked")
	private final RecordComparator comparator2 = new RecordComparator(
		new int[]{0}, (Class<? extends Key<?>>[])new Class[]{ IntValue.class });
	
	private final CountingOutputCollector output = new CountingOutputCollector();
	
	public MatchTaskExternalITCase() {
		super(HASH_MEM, 2, SORT_MEM);
		bnljn_frac = (double)BNLJN_MEM/this.getMemoryManager().getMemorySize();
		hash_frac = (double)HASH_MEM/this.getMemoryManager().getMemorySize();
	}
	
	@Test
	public void testExternalSort1MatchTask() {
		final int keyCnt1 = 16384*4;
		final int valCnt1 = 2;
		
		final int keyCnt2 = 8192;
		final int valCnt2 = 4*2;
		
		final int expCnt = valCnt1*valCnt2*Math.min(keyCnt1, keyCnt2);
		
		setOutput(this.output);
		addDriverComparator(this.comparator1);
		addDriverComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
		getTaskConfig().setDriverStrategy(DriverStrategy.MERGE);
		getTaskConfig().setRelativeMemoryDriver(bnljn_frac);
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
		addDriverComparator(this.comparator1);
		addDriverComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
		setOutput(this.output);
		getTaskConfig().setDriverStrategy(DriverStrategy.HYBRIDHASH_BUILD_FIRST);
		getTaskConfig().setRelativeMemoryDriver(hash_frac);
		
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
		addDriverComparator(this.comparator1);
		addDriverComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
		setOutput(this.output);
		getTaskConfig().setDriverStrategy(DriverStrategy.HYBRIDHASH_BUILD_SECOND);
		getTaskConfig().setRelativeMemoryDriver(hash_frac);
		
		MatchDriver<Record, Record, Record> testTask = new MatchDriver<Record, Record, Record>();
		
		try {
			testDriver(testTask, MockMatchStub.class);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("Test caused an exception.");
		}
		
		Assert.assertEquals("Wrong result set size.", expCnt, this.output.getNumberOfRecords());
	}
	
	public static final class MockMatchStub extends JoinFunction {
		private static final long serialVersionUID = 1L;
		
		@Override
		public void join(Record value1, Record value2, Collector<Record> out) throws Exception {
			out.collect(value1);
		}
	}
}
