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

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.generic.contract.GenericReduceContract.Combinable;
import eu.stratosphere.pact.generic.stub.GenericReducer;
import eu.stratosphere.pact.runtime.plugable.PactRecordComparator;
import eu.stratosphere.pact.runtime.plugable.PactRecordSerializer;
import eu.stratosphere.pact.runtime.sort.CombiningUnilateralSortMerger;
import eu.stratosphere.pact.runtime.test.util.DriverTestBase;
import eu.stratosphere.pact.runtime.test.util.UniformPactRecordGenerator;

public class ReduceTaskExternalITCase extends DriverTestBase<GenericReducer<PactRecord, PactRecord>>
{
	private static final Log LOG = LogFactory.getLog(ReduceTaskExternalITCase.class);
	
	@SuppressWarnings("unchecked")
	private final PactRecordComparator comparator = new PactRecordComparator(
		new int[]{0}, (Class<? extends Key>[])new Class[]{ PactInteger.class });
	
	private final List<PactRecord> outList = new ArrayList<PactRecord>();
	
	
	public ReduceTaskExternalITCase() {
		super(0, 1, 3*1024*1024);
	}
	
	
	@Test
	public void testSingleLevelMergeReduceTask() {
		final int keyCnt = 8192;
		final int valCnt = 8;
		
		setNumFileHandlesForSort(2);
		
		addInputComparator(this.comparator);
		setOutput(this.outList);
		getTaskConfig().setDriverStrategy(DriverStrategy.GROUP);
		
		try {
			addInputSorted(new UniformPactRecordGenerator(keyCnt, valCnt, false), this.comparator.duplicate());
			
			ReduceDriver<PactRecord, PactRecord> testTask = new ReduceDriver<PactRecord, PactRecord>();
			
			testDriver(testTask, MockReduceStub.class);
		} catch (Exception e) {
			LOG.debug(e);
			Assert.fail("Exception in Test.");
		}
		
		Assert.assertTrue("Resultset size was "+this.outList.size()+". Expected was "+keyCnt, this.outList.size() == keyCnt);
		
		for(PactRecord record : this.outList) {
			Assert.assertTrue("Incorrect result", record.getField(1, PactInteger.class).getValue() == valCnt-record.getField(0, PactInteger.class).getValue());
		}
		
		this.outList.clear();
				
	}
	
	@Test
	public void testMultiLevelMergeReduceTask() {
		final int keyCnt = 32768;
		final int valCnt = 8;

		setNumFileHandlesForSort(2);
		
		addInputComparator(this.comparator);
		setOutput(this.outList);
		getTaskConfig().setDriverStrategy(DriverStrategy.GROUP);
		
		try {
			addInputSorted(new UniformPactRecordGenerator(keyCnt, valCnt, false), this.comparator.duplicate());
			
			ReduceDriver<PactRecord, PactRecord> testTask = new ReduceDriver<PactRecord, PactRecord>();
			
			testDriver(testTask, MockReduceStub.class);
		} catch (Exception e) {
			LOG.debug(e);
			Assert.fail("Exception in Test.");
		}
		
		Assert.assertTrue("Resultset size was "+this.outList.size()+". Expected was "+keyCnt, this.outList.size() == keyCnt);
		
		for(PactRecord record : this.outList) {
			Assert.assertTrue("Incorrect result", record.getField(1, PactInteger.class).getValue() == valCnt-record.getField(0, PactInteger.class).getValue());
		}
		
		this.outList.clear();
				
	}
	
	@Test
	public void testSingleLevelMergeCombiningReduceTask()
	{
		final int keyCnt = 8192;
		final int valCnt = 8;
		
		addInputComparator(this.comparator);
		setOutput(this.outList);
		getTaskConfig().setDriverStrategy(DriverStrategy.GROUP);
		
		CombiningUnilateralSortMerger<PactRecord> sorter = null;
		try {
			sorter = new CombiningUnilateralSortMerger<PactRecord>(new MockCombiningReduceStub(), 
				getMemoryManager(), getIOManager(), new UniformPactRecordGenerator(keyCnt, valCnt, false), 
				getOwningNepheleTask(), PactRecordSerializer.get(), this.comparator.duplicate(), this.perSortMem, 2, 0.8f, false);
			addInput(sorter.getIterator());
			
			ReduceDriver<PactRecord, PactRecord> testTask = new ReduceDriver<PactRecord, PactRecord>();
		
			testDriver(testTask, MockCombiningReduceStub.class);
		} catch (Exception e) {
			LOG.debug(e);
			Assert.fail("Invoke method caused exception.");
		} finally {
			if (sorter != null) {
				sorter.close();
			}
		}
		
		int expSum = 0;
		for (int i = 1; i < valCnt; i++) {
			expSum += i;
		}
		
		Assert.assertTrue("Resultset size was "+this.outList.size()+". Expected was "+keyCnt, this.outList.size() == keyCnt);
		
		for (PactRecord record : this.outList) {
			Assert.assertTrue("Incorrect result", record.getField(1, PactInteger.class).getValue() == expSum-record.getField(0, PactInteger.class).getValue());
		}
		
		this.outList.clear();
	}
	
	
	@Test
	public void testMultiLevelMergeCombiningReduceTask() {

		int keyCnt = 32768;
		int valCnt = 8;
		
		addInputComparator(this.comparator);
		setOutput(this.outList);
		getTaskConfig().setDriverStrategy(DriverStrategy.GROUP);
		
		CombiningUnilateralSortMerger<PactRecord> sorter = null;
		try {
			sorter = new CombiningUnilateralSortMerger<PactRecord>(new MockCombiningReduceStub(), 
				getMemoryManager(), getIOManager(), new UniformPactRecordGenerator(keyCnt, valCnt, false), 
				getOwningNepheleTask(), PactRecordSerializer.get(), this.comparator.duplicate(), this.perSortMem, 2, 0.8f, false);
			addInput(sorter.getIterator());
			
			ReduceDriver<PactRecord, PactRecord> testTask = new ReduceDriver<PactRecord, PactRecord>();
		
			testDriver(testTask, MockCombiningReduceStub.class);
		} catch (Exception e) {
			LOG.debug(e);
			Assert.fail("Invoke method caused exception.");
		} finally {
			if (sorter != null) {
				sorter.close();
			}
		}
		
		int expSum = 0;
		for (int i = 1; i < valCnt; i++) {
			expSum += i;
		}
		
		Assert.assertTrue("Resultset size was "+this.outList.size()+". Expected was "+keyCnt, this.outList.size() == keyCnt);
		
		for (PactRecord record : this.outList) {
			Assert.assertTrue("Incorrect result", record.getField(1, PactInteger.class).getValue() == expSum-record.getField(0, PactInteger.class).getValue());
		}
		
		this.outList.clear();
		
	}
	
	public static class MockReduceStub extends ReduceStub {

		private final PactInteger key = new PactInteger();
		private final PactInteger value = new PactInteger();

		@Override
		public void reduce(Iterator<PactRecord> records, Collector<PactRecord> out)
				throws Exception {
			PactRecord element = null;
			int cnt = 0;
			while (records.hasNext()) {
				element = records.next();
				cnt++;
			}
			element.getField(0, this.key);
			this.value.setValue(cnt - this.key.getValue());
			element.setField(1, this.value);
			out.collect(element);
		}
	}
	
	@Combinable
	public static class MockCombiningReduceStub extends ReduceStub {

		private final PactInteger key = new PactInteger();
		private final PactInteger value = new PactInteger();
		private final PactInteger combineValue = new PactInteger();

		@Override
		public void reduce(Iterator<PactRecord> records, Collector<PactRecord> out)
				throws Exception {
			PactRecord element = null;
			int sum = 0;
			while (records.hasNext()) {
				element = records.next();
				element.getField(1, this.value);
				
				sum += this.value.getValue();
			}
			element.getField(0, this.key);
			this.value.setValue(sum - this.key.getValue());
			element.setField(1, this.value);
			out.collect(element);
		}
		
		@Override
		public void combine(Iterator<PactRecord> records, Collector<PactRecord> out)
				throws Exception {
			PactRecord element = null;
			int sum = 0;
			while (records.hasNext()) {
				element = records.next();
				element.getField(1, this.combineValue);
				
				sum += this.combineValue.getValue();
			}
			
			this.combineValue.setValue(sum);
			element.setField(1, this.combineValue);
			out.collect(element);
		}
		
	}
	
}
