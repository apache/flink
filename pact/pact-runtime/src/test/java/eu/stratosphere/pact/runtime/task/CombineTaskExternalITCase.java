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
import java.util.Iterator;
import java.util.List;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import eu.stratosphere.pact.common.contract.ReduceContract.Combinable;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;
import eu.stratosphere.pact.runtime.test.util.RegularlyGeneratedInputGenerator;
import eu.stratosphere.pact.runtime.test.util.TaskTestBase;

@SuppressWarnings("unchecked")
public class CombineTaskExternalITCase extends TaskTestBase {

	private static final Log LOG = LogFactory.getLog(CombineTaskExternalITCase.class);
	
	List<PactRecord> outList = new ArrayList<PactRecord>();

	@Test
	public void testSingleLevelMergeCombineTask() {

		int keyCnt = 8192;
		int valCnt = 8;
		
		super.initEnvironment(3*1024*1024);
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt, valCnt, false));
		super.addOutput(outList);
		
		CombineTask testTask = new CombineTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.COMBININGSORT);
		super.getTaskConfig().setMemorySize(3 * 1024 * 1024);
		super.getTaskConfig().setNumFilehandles(2);
		super.getTaskConfig().setLocalStrategyKeyTypes(0, new int[]{0});
		super.getTaskConfig().setLocalStrategyKeyTypes(new Class[]{ PactInteger.class });
		
		super.registerTask(testTask, MockCombiningReduceStub.class);
		
		try {
			testTask.invoke();
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
		for (PactRecord record : outList) {
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
		
		Assert.assertTrue("Resultset size was "+aggMap.size()+". Expected was "+keyCnt, outList.size() == keyCnt);
		
		for (PactInteger integer : aggMap.values()) {
			Assert.assertTrue("Incorrect result", integer.getValue() == expSum);
		}
		
		outList.clear();
		
	}
	
	@Test
	public void testMultiLevelMergeCombineTask() {

		int keyCnt = 32768;
		int valCnt = 8;
		
		super.initEnvironment(3*1024*1024);
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt, valCnt, false));
		super.addOutput(outList);
		
		CombineTask testTask = new CombineTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.COMBININGSORT);
		super.getTaskConfig().setMemorySize(3 * 1024 * 1024);
		super.getTaskConfig().setNumFilehandles(2);
		super.getTaskConfig().setLocalStrategyKeyTypes(0, new int[]{0});
		super.getTaskConfig().setLocalStrategyKeyTypes(new Class[]{ PactInteger.class });
		
		super.registerTask(testTask, MockCombiningReduceStub.class);
		
		try {
			testTask.invoke();
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
		for (PactRecord record : outList) {
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
		
		outList.clear();
		
	}
	
	@Combinable
	public static class MockCombiningReduceStub extends ReduceStub {

		private final PactInteger key = new PactInteger();
		private final PactInteger value = new PactInteger();
		private final PactInteger combineValue = new PactInteger();

		@Override
		public void reduce(Iterator<PactRecord> records, Collector out)
				throws Exception {
			PactRecord element = null;
			int sum = 0;
			while (records.hasNext()) {
				element = records.next();
				element.getField(1, this.value);
				
				sum += this.value.getValue();
			}
			element.getField(0, key);
			value.setValue(sum - key.getValue());
			element.setField(1, value);
			out.collect(element);
		}
		
		@Override
		public void combine(Iterator<PactRecord> records, Collector out)
				throws Exception {
			PactRecord element = null;
			int sum = 0;
			while (records.hasNext()) {
				element = records.next();
				element.getField(1, combineValue);
				
				sum += combineValue.getValue();
			}
			
			combineValue.setValue(sum);
			element.setField(1, combineValue);
			out.collect(element);
		}
	}
	
}
