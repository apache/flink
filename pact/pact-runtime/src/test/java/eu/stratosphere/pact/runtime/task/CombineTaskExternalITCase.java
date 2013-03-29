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
import java.util.HashMap;

import junit.framework.Assert;

import org.junit.Test;

import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.generic.stub.GenericReducer;
import eu.stratosphere.pact.runtime.plugable.pactrecord.PactRecordComparator;
import eu.stratosphere.pact.runtime.task.CombineTaskTest.MockCombiningReduceStub;
import eu.stratosphere.pact.runtime.test.util.DriverTestBase;
import eu.stratosphere.pact.runtime.test.util.UniformPactRecordGenerator;


public class CombineTaskExternalITCase extends DriverTestBase<GenericReducer<PactRecord, ?>>
{
	private static final long COMBINE_MEM = 3 * 1024 * 1024;
	
	private final ArrayList<PactRecord> outList = new ArrayList<PactRecord>();
	
	@SuppressWarnings("unchecked")
	private final PactRecordComparator comparator = new PactRecordComparator(
		new int[]{0}, (Class<? extends Key>[])new Class[]{ PactInteger.class });

	public CombineTaskExternalITCase() {
		super(COMBINE_MEM, 0);
	}

	
	@Test
	public void testSingleLevelMergeCombineTask() {
		final int keyCnt = 40000;
		final int valCnt = 8;
		
		addInput(new UniformPactRecordGenerator(keyCnt, valCnt, false));
		addInputComparator(this.comparator);
		setOutput(this.outList);
		
		getTaskConfig().setDriverStrategy(DriverStrategy.SORTED_GROUP);
		getTaskConfig().setMemoryDriver(COMBINE_MEM);
		getTaskConfig().setFilehandlesDriver(2);
		
		final CombineDriver<PactRecord> testTask = new CombineDriver<PactRecord>();
		
		try {
			testDriver(testTask, MockCombiningReduceStub.class);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("Invoke method caused exception.");
		}
		
		int expSum = 0;
		for (int i = 1;i < valCnt; i++) {
			expSum += i;
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
		final int keyCnt = 100000;
		final int valCnt = 8;
		
		addInput(new UniformPactRecordGenerator(keyCnt, valCnt, false));
		addInputComparator(this.comparator);
		setOutput(this.outList);
		
		getTaskConfig().setDriverStrategy(DriverStrategy.SORTED_GROUP);
		getTaskConfig().setMemoryDriver(COMBINE_MEM);
		getTaskConfig().setFilehandlesDriver(2);
		
		final CombineDriver<PactRecord> testTask = new CombineDriver<PactRecord>();
		
		try {
			testDriver(testTask, MockCombiningReduceStub.class);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("Invoke method caused exception.");
		}
		
		int expSum = 0;
		for (int i = 1;i < valCnt; i++) {
			expSum += i;
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
}
