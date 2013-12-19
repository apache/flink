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

import junit.framework.Assert;

import org.junit.Test;

import eu.stratosphere.api.functions.GenericCoGrouper;
import eu.stratosphere.api.record.functions.CoGroupFunction;
import eu.stratosphere.pact.runtime.plugable.pactrecord.RecordComparator;
import eu.stratosphere.pact.runtime.plugable.pactrecord.RecordPairComparatorFactory;
import eu.stratosphere.pact.runtime.test.util.DriverTestBase;
import eu.stratosphere.pact.runtime.test.util.UniformRecordGenerator;
import eu.stratosphere.types.Key;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;

public class CoGroupTaskExternalITCase extends DriverTestBase<GenericCoGrouper<Record, Record, Record>>
{
	private static final long SORT_MEM = 3*1024*1024;
	
	@SuppressWarnings("unchecked")
	private final RecordComparator comparator1 = new RecordComparator(
		new int[]{0}, (Class<? extends Key>[])new Class[]{ IntValue.class });
	
	@SuppressWarnings("unchecked")
	private final RecordComparator comparator2 = new RecordComparator(
		new int[]{0}, (Class<? extends Key>[])new Class[]{ IntValue.class });
	
	private final CountingOutputCollector output = new CountingOutputCollector();
	
	public CoGroupTaskExternalITCase() {
		super(0, 2, SORT_MEM);
	}

	@Test
	public void testExternalSortCoGroupTask() {

		int keyCnt1 = 16384*8;
		int valCnt1 = 32;
		
		int keyCnt2 = 65536*4;
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
	
	public static final class MockCoGroupStub extends CoGroupFunction
	{
		private final Record res = new Record();
		
		@Override
		public void coGroup(Iterator<Record> records1, Iterator<Record> records2, Collector<Record> out)
		{
			int val1Cnt = 0;
			int val2Cnt = 0;
			
			while (records1.hasNext()) {
				val1Cnt++;
				records1.next();
			}
			
			while (records2.hasNext()) {
				val2Cnt++;
				records2.next();
			}
			
			if (val1Cnt == 0) {
				for (int i = 0; i < val2Cnt; i++) {
					out.collect(this.res);
				}
			} else if (val2Cnt == 0) {
				for (int i = 0; i < val1Cnt; i++) {
					out.collect(this.res);
				}
			} else {
				for (int i = 0; i < val2Cnt * val1Cnt; i++) {
					out.collect(this.res);
				}
			}
		}
	}
}
