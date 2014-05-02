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
import java.util.HashMap;

import junit.framework.Assert;

import org.junit.Test;

import eu.stratosphere.api.common.functions.GenericGroupReduce;
import eu.stratosphere.pact.runtime.plugable.pactrecord.RecordComparator;
import eu.stratosphere.pact.runtime.task.CombineTaskTest.MockCombiningReduceStub;
import eu.stratosphere.pact.runtime.test.util.DriverTestBase;
import eu.stratosphere.pact.runtime.test.util.UniformRecordGenerator;
import eu.stratosphere.types.Key;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;


public class CombineTaskExternalITCase extends DriverTestBase<GenericGroupReduce<Record, ?>> {
	
	private static final long COMBINE_MEM = 3 * 1024 * 1024;
	
	private final ArrayList<Record> outList = new ArrayList<Record>();
	
	@SuppressWarnings("unchecked")
	private final RecordComparator comparator = new RecordComparator(
		new int[]{0}, (Class<? extends Key>[])new Class[]{ IntValue.class });

	public CombineTaskExternalITCase() {
		super(COMBINE_MEM, 0);
	}

	
	@Test
	public void testSingleLevelMergeCombineTask() {
		final int keyCnt = 40000;
		final int valCnt = 8;
		
		addInput(new UniformRecordGenerator(keyCnt, valCnt, false));
		addInputComparator(this.comparator);
		setOutput(this.outList);
		
		getTaskConfig().setDriverStrategy(DriverStrategy.PARTIAL_GROUP);
		getTaskConfig().setMemoryDriver(COMBINE_MEM);
		getTaskConfig().setFilehandlesDriver(2);
		
		final CombineDriver<Record> testTask = new CombineDriver<Record>();
		
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
		final HashMap<IntValue, IntValue> aggMap = new HashMap<IntValue, IntValue>();
		for (Record record : this.outList) {
			IntValue key = new IntValue();
			IntValue value = new IntValue();
			key = record.getField(0, key);
			value = record.getField(1, value);
			IntValue prevVal = aggMap.get(key);
			if (prevVal != null) {
				aggMap.put(key, new IntValue(prevVal.getValue() + value.getValue()));
			}
			else {
				aggMap.put(key, value);
			}
		}
		
		Assert.assertTrue("Resultset size was "+aggMap.size()+". Expected was "+keyCnt, aggMap.size() == keyCnt);
		
		for (IntValue integer : aggMap.values()) {
			Assert.assertTrue("Incorrect result", integer.getValue() == expSum);
		}
		
		this.outList.clear();
	}
	
	@Test
	public void testMultiLevelMergeCombineTask() throws Exception {
		final int keyCnt = 100000;
		final int valCnt = 8;
		
		addInput(new UniformRecordGenerator(keyCnt, valCnt, false));
		addInputComparator(this.comparator);
		setOutput(this.outList);
		
		getTaskConfig().setDriverStrategy(DriverStrategy.PARTIAL_GROUP);
		getTaskConfig().setMemoryDriver(COMBINE_MEM);
		getTaskConfig().setFilehandlesDriver(2);
		
		final CombineDriver<Record> testTask = new CombineDriver<Record>();

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
		final HashMap<IntValue, IntValue> aggMap = new HashMap<IntValue, IntValue>();
		for (Record record : this.outList) {
			IntValue key = new IntValue();
			IntValue value = new IntValue();
			key = record.getField(0, key);
			value = record.getField(1, value);
			IntValue prevVal = aggMap.get(key);
			if (prevVal != null) {
				aggMap.put(key, new IntValue(prevVal.getValue() + value.getValue()));
			}
			else {
				aggMap.put(key, value);
			}
		}
		
		Assert.assertTrue("Resultset size was "+aggMap.size()+". Expected was "+keyCnt, aggMap.size() == keyCnt);
		
		for (IntValue integer : aggMap.values()) {
			Assert.assertTrue("Incorrect result", integer.getValue() == expSum);
		}
		
		this.outList.clear();
	}
}
