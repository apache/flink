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
import java.util.Iterator;
import java.util.List;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import eu.stratosphere.api.common.functions.GenericReducer;
import eu.stratosphere.api.common.operators.base.ReduceOperatorBase.Combinable;
import eu.stratosphere.api.java.record.functions.ReduceFunction;
import eu.stratosphere.pact.runtime.plugable.pactrecord.RecordComparator;
import eu.stratosphere.pact.runtime.plugable.pactrecord.RecordSerializer;
import eu.stratosphere.pact.runtime.sort.CombiningUnilateralSortMerger;
import eu.stratosphere.pact.runtime.test.util.DriverTestBase;
import eu.stratosphere.pact.runtime.test.util.UniformRecordGenerator;
import eu.stratosphere.types.Key;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;

public class ReduceTaskExternalITCase extends DriverTestBase<GenericReducer<Record, Record>>
{
	private static final Log LOG = LogFactory.getLog(ReduceTaskExternalITCase.class);
	
	@SuppressWarnings("unchecked")
	private final RecordComparator comparator = new RecordComparator(
		new int[]{0}, (Class<? extends Key>[])new Class[]{ IntValue.class });
	
	private final List<Record> outList = new ArrayList<Record>();
	
	
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
		getTaskConfig().setDriverStrategy(DriverStrategy.SORTED_GROUP);
		
		try {
			addInputSorted(new UniformRecordGenerator(keyCnt, valCnt, false), this.comparator.duplicate());
			
			ReduceDriver<Record, Record> testTask = new ReduceDriver<Record, Record>();
			
			testDriver(testTask, MockReduceStub.class);
		} catch (Exception e) {
			LOG.debug(e);
			Assert.fail("Exception in Test.");
		}
		
		Assert.assertTrue("Resultset size was "+this.outList.size()+". Expected was "+keyCnt, this.outList.size() == keyCnt);
		
		for(Record record : this.outList) {
			Assert.assertTrue("Incorrect result", record.getField(1, IntValue.class).getValue() == valCnt-record.getField(0, IntValue.class).getValue());
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
		getTaskConfig().setDriverStrategy(DriverStrategy.SORTED_GROUP);
		
		try {
			addInputSorted(new UniformRecordGenerator(keyCnt, valCnt, false), this.comparator.duplicate());
			
			ReduceDriver<Record, Record> testTask = new ReduceDriver<Record, Record>();
			
			testDriver(testTask, MockReduceStub.class);
		} catch (Exception e) {
			LOG.debug(e);
			Assert.fail("Exception in Test.");
		}
		
		Assert.assertTrue("Resultset size was "+this.outList.size()+". Expected was "+keyCnt, this.outList.size() == keyCnt);
		
		for(Record record : this.outList) {
			Assert.assertTrue("Incorrect result", record.getField(1, IntValue.class).getValue() == valCnt-record.getField(0, IntValue.class).getValue());
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
		getTaskConfig().setDriverStrategy(DriverStrategy.SORTED_GROUP);
		
		CombiningUnilateralSortMerger<Record> sorter = null;
		try {
			sorter = new CombiningUnilateralSortMerger<Record>(new MockCombiningReduceStub(), 
				getMemoryManager(), getIOManager(), new UniformRecordGenerator(keyCnt, valCnt, false), 
				getOwningNepheleTask(), RecordSerializer.get(), this.comparator.duplicate(), this.perSortMem, 2, 0.8f);
			addInput(sorter.getIterator());
			
			ReduceDriver<Record, Record> testTask = new ReduceDriver<Record, Record>();
		
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
		
		for (Record record : this.outList) {
			Assert.assertTrue("Incorrect result", record.getField(1, IntValue.class).getValue() == expSum-record.getField(0, IntValue.class).getValue());
		}
		
		this.outList.clear();
	}
	
	
	@Test
	public void testMultiLevelMergeCombiningReduceTask() {

		int keyCnt = 32768;
		int valCnt = 8;
		
		addInputComparator(this.comparator);
		setOutput(this.outList);
		getTaskConfig().setDriverStrategy(DriverStrategy.SORTED_GROUP);
		
		CombiningUnilateralSortMerger<Record> sorter = null;
		try {
			sorter = new CombiningUnilateralSortMerger<Record>(new MockCombiningReduceStub(), 
				getMemoryManager(), getIOManager(), new UniformRecordGenerator(keyCnt, valCnt, false), 
				getOwningNepheleTask(), RecordSerializer.get(), this.comparator.duplicate(), this.perSortMem, 2, 0.8f);
			addInput(sorter.getIterator());
			
			ReduceDriver<Record, Record> testTask = new ReduceDriver<Record, Record>();
		
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
		
		for (Record record : this.outList) {
			Assert.assertTrue("Incorrect result", record.getField(1, IntValue.class).getValue() == expSum-record.getField(0, IntValue.class).getValue());
		}
		
		this.outList.clear();
		
	}
	
	public static class MockReduceStub extends ReduceFunction {
		private static final long serialVersionUID = 1L;
		
		private final IntValue key = new IntValue();
		private final IntValue value = new IntValue();

		@Override
		public void reduce(Iterator<Record> records, Collector<Record> out)
				throws Exception {
			Record element = null;
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
	public static class MockCombiningReduceStub extends ReduceFunction {
		private static final long serialVersionUID = 1L;

		private final IntValue key = new IntValue();
		private final IntValue value = new IntValue();
		private final IntValue combineValue = new IntValue();

		@Override
		public void reduce(Iterator<Record> records, Collector<Record> out)
				throws Exception {
			Record element = null;
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
		public void combine(Iterator<Record> records, Collector<Record> out)
				throws Exception {
			Record element = null;
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
