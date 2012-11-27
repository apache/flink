/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import eu.stratosphere.pact.common.contract.ReduceContract.Combinable;
import eu.stratosphere.pact.common.generic.GenericReducer;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.runtime.plugable.PactRecordComparator;
import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;
import eu.stratosphere.pact.runtime.test.util.DriverTestBase;
import eu.stratosphere.pact.runtime.test.util.UniformPactRecordGenerator;


public class CombineTaskExternalITCase extends DriverTestBase<GenericReducer<PactRecord, ?>>
{
	private static final Log LOG = LogFactory.getLog(CombineTaskExternalITCase.class);
	
	final List<PactRecord> outList = new ArrayList<PactRecord>();
	
	
	public CombineTaskExternalITCase() {
		super(3*1024*1024);
	}

	
	@Test
	public void testSingleLevelMergeCombineTask() {

		int keyCnt = 40000;
		int valCnt = 8;
		
		addInput(new UniformPactRecordGenerator(keyCnt, valCnt, false));
		addOutput(this.outList);
		
		final CombineDriver<PactRecord> testTask = new CombineDriver<PactRecord>();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.COMBININGSORT);
		super.getTaskConfig().setMemorySize(3 * 1024 * 1024);
		super.getTaskConfig().setNumFilehandles(2);
		
		final int[] keyPos = new int[]{0};
		@SuppressWarnings("unchecked")
		final Class<? extends Key>[] keyClasses = (Class<? extends Key>[]) new Class[]{ PactInteger.class };
		addInputComparator(new PactRecordComparator(keyPos, keyClasses));
		
		try {
			testDriver(testTask, MockCombiningReduceStub.class);
		} catch (Exception e) {
			LOG.debug(e);
			Assert.fail("Invoke method caused exception.");
		}
		
		int expSum = 0;
		for(int i=1;i<valCnt;i++) {
			expSum+=i;
		}
		
		// wee need to do the final aggregation manually in the test, because the
		// combiner is not guaranteed to do that
		final HashMap<PactInteger, PactInteger> aggMap = new HashMap<PactInteger, PactInteger>();
		for (PactRecord record : this.outList) {
			PactInteger key = new PactInteger();
			PactInteger value = new PactInteger();
			key = record.getField(0, key);
			value = record.getField(1, value);
			PactInteger prevVal = aggMap.get(key);
			if (prevVal != null) {
				aggMap.put(key, new PactInteger(prevVal.getValue() + value.getValue()));
			}
			else {
				aggMap.put(key, value);
			}
		}
		
		Assert.assertTrue("Resultset size was "+aggMap.size()+". Expected was "+keyCnt, aggMap.size() == keyCnt);
		
		for (PactInteger integer : aggMap.values()) {
			Assert.assertTrue("Incorrect result", integer.getValue() == expSum);
		}
		
		this.outList.clear();
		
	}
	
	@Test
	public void testMultiLevelMergeCombineTask() {

		int keyCnt = 100000;
		int valCnt = 8;
		
		addInput(new UniformPactRecordGenerator(keyCnt, valCnt, false));
		addOutput(this.outList);
		
		CombineDriver<PactRecord> testTask = new CombineDriver<PactRecord>();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.COMBININGSORT);
		super.getTaskConfig().setMemorySize(3 * 1024 * 1024);
		super.getTaskConfig().setNumFilehandles(2);

		final int[] keyPos = new int[]{0};
		@SuppressWarnings("unchecked")
		final Class<? extends Key>[] keyClasses = (Class<? extends Key>[]) new Class[]{ PactInteger.class };
		addInputComparator(new PactRecordComparator(keyPos, keyClasses));
		
		try {
			testDriver(testTask, MockCombiningReduceStub.class);
		} catch (Exception e) {
			LOG.debug(e);
			Assert.fail("Invoke method caused exception.");
		}
		
		int expSum = 0;
		for(int i=1;i<valCnt;i++) {
			expSum+=i;
		}
		
		// wee need to do the final aggregation manually in the test, because the
		// combiner is not guaranteed to do that
		HashMap<PactInteger, PactInteger> aggMap = new HashMap<PactInteger, PactInteger>();
		for (PactRecord record : this.outList) {
			PactInteger key = new PactInteger();
			PactInteger value = new PactInteger();
			record.getField(0, key);
			record.getField(1, value);
			
			PactInteger prevVal = aggMap.get(key);
			if (prevVal != null) {
				aggMap.put(key, new PactInteger(prevVal.getValue() + value.getValue()));
			}
			else {
				aggMap.put(key, value);
			}
		}
		
		Assert.assertTrue("Resultset size was "+aggMap.size()+". Expected was "+keyCnt, aggMap.size() == keyCnt);
		
		for (PactInteger integer : aggMap.values()) {
			Assert.assertTrue("Incorrect result", integer.getValue() == expSum);
		}
		
		this.outList.clear();
		
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
