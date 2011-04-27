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
import java.util.HashSet;
import java.util.List;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MatchStub;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;
import eu.stratosphere.pact.runtime.test.util.DelayingInfinitiveInputIterator;
import eu.stratosphere.pact.runtime.test.util.NirvanaOutputList;
import eu.stratosphere.pact.runtime.test.util.RegularlyGeneratedInputGenerator;
import eu.stratosphere.pact.runtime.test.util.TaskCancelThread;
import eu.stratosphere.pact.runtime.test.util.TaskTestBase;

public class SelfMatchTaskTest extends TaskTestBase {

	private static final Log LOG = LogFactory.getLog(SelfMatchTaskTest.class);
	
	List<KeyValuePair<PactInteger,PactInteger>> outList = new ArrayList<KeyValuePair<PactInteger,PactInteger>>();

	@Test
	public void testSortSelfMatchTask() {

		int keyCnt = 10;
		int valCnt = 35;
				
		super.initEnvironment(6 * 1024 * 1024);
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt, valCnt, false));
		super.addOutput(outList);
		
		SelfMatchTask testTask = new SelfMatchTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.SORT_SELF_NESTEDLOOP);
		
		super.getTaskConfig().setMemorySize(6 * 1024 * 1024);
		super.getTaskConfig().setNumFilehandles(4);
		
		super.registerTask(testTask, MockMatchStub.class);
		
		try {
			testTask.invoke();
		} catch (Exception e) {
			LOG.debug(e);
			Assert.fail("Invoke method caused exception.");
		}
		
		int expCnt = keyCnt*(valCnt*valCnt);
				
		Assert.assertTrue("Resultset size was "+outList.size()+". Expected was "+expCnt, outList.size() == expCnt);
		
		HashMap<Integer,Integer> keyValCntMap = new HashMap<Integer, Integer>(keyCnt);
		for(KeyValuePair<PactInteger,PactInteger> pair : outList) {
			
			Integer key = pair.getKey().getValue();
			if(!keyValCntMap.containsKey(key)) {
				keyValCntMap.put(key,1);
			} else {
				keyValCntMap.put(key, keyValCntMap.get(key)+1);
			}
		}
		
		for(Integer key : keyValCntMap.keySet()) {
			Assert.assertTrue("Invalid value count for key: "+key+". Value count was: "+keyValCntMap.get(key)+
				" Expected was: "+(valCnt*valCnt), keyValCntMap.get(key).intValue() == (valCnt*valCnt));
		}
		
		outList.clear();
		
	}
	
	
	@Test
	public void testNoneSelfMatchTask() {

		int keyCnt = 100;
		int valCnt = 5;
				
		super.initEnvironment(3 * 1024 * 1024);
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt, valCnt, true));
		super.addOutput(outList);
		
		SelfMatchTask testTask = new SelfMatchTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.SELF_NESTEDLOOP);
		
		super.getTaskConfig().setMemorySize(3 * 1024 * 1024);
		super.getTaskConfig().setNumFilehandles(4);
		
		super.registerTask(testTask, MockMatchStub.class);
		
		try {
			testTask.invoke();
		} catch (Exception e) {
			LOG.debug(e);
			Assert.fail("Invoke method caused exception.");
		}
		
		int expCnt = keyCnt*(valCnt*valCnt);
				
		Assert.assertTrue("Resultset size was "+outList.size()+". Expected was "+expCnt, outList.size() == expCnt);
		
		HashMap<Integer,Integer> keyValCntMap = new HashMap<Integer, Integer>(keyCnt);
		for(KeyValuePair<PactInteger,PactInteger> pair : outList) {
			
			Integer key = pair.getKey().getValue();
			if(!keyValCntMap.containsKey(key)) {
				keyValCntMap.put(key,1);
			} else {
				keyValCntMap.put(key, keyValCntMap.get(key)+1);
			}
		}
		for(Integer key : keyValCntMap.keySet()) {
			Assert.assertTrue("Invalid value count. Value count was: "+keyValCntMap.get(key)+
				" Expected was "+(valCnt*valCnt), keyValCntMap.get(key) == (valCnt*valCnt));
		}
		
		outList.clear();
		
	}
	
	@Test
	public void testFailingSortSelfMatchTask() {

		int keyCnt = 20;
		int valCnt = 20;
		
		super.initEnvironment(6 * 1024 * 1024);
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt, valCnt, false));
		super.addOutput(outList);
		
		SelfMatchTask testTask = new SelfMatchTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.SORT_SELF_NESTEDLOOP);
		super.getTaskConfig().setMemorySize(6 * 1024 * 1024);
		super.getTaskConfig().setNumFilehandles(4);
		
		super.registerTask(testTask, MockFailingMatchStub.class);
		
		boolean stubFailed = false;
		
		try {
			testTask.invoke();
		} catch (Exception e) {
			stubFailed = true;
		}
		
		Assert.assertTrue("Stub exception was not forwarded.", stubFailed);
		
		outList.clear();
		
	}
	
	@Test
	public void testCancelSelfMatchTaskWhileSorting() {
		
		super.initEnvironment(6 * 1024 * 1024);
		super.addInput(new DelayingInfinitiveInputIterator(100));
		super.addOutput(new NirvanaOutputList());
		
		final SelfMatchTask testTask = new SelfMatchTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.SORT_SELF_NESTEDLOOP);
		super.getTaskConfig().setMemorySize(6 * 1024 * 1024);
		super.getTaskConfig().setNumFilehandles(4);
		
		super.registerTask(testTask, MockMatchStub.class);
		
		Thread taskRunner = new Thread() {
			public void run() {
				try {
					testTask.invoke();
				} catch (Exception ie) {
					ie.printStackTrace();
					Assert.fail("Task threw exception although it was properly canceled");
				}
			}
		};
		taskRunner.start();
		
		TaskCancelThread tct = new TaskCancelThread(1, taskRunner, testTask);
		tct.start();
		
		try {
			tct.join();
			taskRunner.join();		
		} catch(InterruptedException ie) {
			Assert.fail("Joining threads failed");
		}
	}
	
	@Test
	public void testCancelSelfMatchTaskWhileMatching() {
		
		int keyCnt = 20;
		int valCnt = 20;
		
		super.initEnvironment(6 * 1024 * 1024);
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt, valCnt, false));
		super.addOutput(new NirvanaOutputList());
		
		final SelfMatchTask testTask = new SelfMatchTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.SORT_SELF_NESTEDLOOP);
		super.getTaskConfig().setMemorySize(6 * 1024 * 1024);
		super.getTaskConfig().setNumFilehandles(4);
		
		super.registerTask(testTask, MockDelayingMatchStub.class);
		
		Thread taskRunner = new Thread() {
			public void run() {
				try {
					testTask.invoke();
				} catch (Exception ie) {
					ie.printStackTrace();
					Assert.fail("Task threw exception although it was properly canceled");
				}
			}
		};
		taskRunner.start();
		
		TaskCancelThread tct = new TaskCancelThread(1, taskRunner, testTask);
		tct.start();
		
		try {
			tct.join();
			taskRunner.join();		
		} catch(InterruptedException ie) {
			Assert.fail("Joining threads failed");
		}
		
	}
	
	public static class MockMatchStub extends MatchStub<PactInteger, PactInteger, PactInteger, PactInteger, PactInteger> {

		HashSet<Integer> hashSet = new HashSet<Integer>(1000);
		
		@Override
		public void match(PactInteger key, PactInteger value1, PactInteger value2,
				Collector<PactInteger, PactInteger> out) {
			
			Assert.assertTrue("Key was given multiple times into user code",!hashSet.contains(System.identityHashCode(key)));
			Assert.assertTrue("Value was given multiple times into user code",!hashSet.contains(System.identityHashCode(value1)));
			Assert.assertTrue("Value was given multiple times into user code",!hashSet.contains(System.identityHashCode(value2)));
			
			hashSet.add(System.identityHashCode(key));
			hashSet.add(System.identityHashCode(value1));
			hashSet.add(System.identityHashCode(value2));
			
			out.collect(key, value1);
			
		}
		
	}
	
	public static class MockFailingMatchStub extends MatchStub<PactInteger, PactInteger, PactInteger, PactInteger, PactInteger> {

		int cnt = 0;
		
		@Override
		public void match(PactInteger key, PactInteger value1, PactInteger value2,
				Collector<PactInteger, PactInteger> out) {
			
			if(++cnt>=10) {
				throw new RuntimeException("Expected Test Exception");
			}
			
			out.collect(key, value1);
			
		}
		
	}
	
	
	public static class MockDelayingMatchStub extends MatchStub<PactInteger, PactInteger, PactInteger, PactInteger, PactInteger> {

		HashSet<Integer> hashSet = new HashSet<Integer>(1000);
		
		@Override
		public void match(PactInteger key, PactInteger value1, PactInteger value2,
				Collector<PactInteger, PactInteger> out) {
			
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) { }			
		}
		
	}
	
}
