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

//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//
//import junit.framework.Assert;
//
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
//import org.junit.Test;
//
//import eu.stratosphere.pact.common.stubs.Collector;
//import eu.stratosphere.pact.common.stubs.MatchStub;
//import eu.stratosphere.pact.common.type.PactRecord;
//import eu.stratosphere.pact.common.type.base.PactInteger;
//import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;
//import eu.stratosphere.pact.runtime.test.util.DelayingInfinitiveInputIterator;
//import eu.stratosphere.pact.runtime.test.util.NirvanaOutputList;
//import eu.stratosphere.pact.runtime.test.util.UniformPactRecordGenerator;
//import eu.stratosphere.pact.runtime.test.util.TaskCancelThread;
import eu.stratosphere.pact.runtime.test.util.TaskTestBase;


public class SelfMatchTaskTest extends TaskTestBase
{
//	private static final Log LOG = LogFactory.getLog(SelfMatchTaskTest.class);
//	
//	private final List<PactRecord> outList = new ArrayList<PactRecord>();
//
//	@Test
//	public void testSortFullSelfMatchTask() {
//
//		int keyCnt = 10;
//		int valCnt = 40;
//				
//		super.initEnvironment(6 * 1024 * 1024);
//		super.addInput(new UniformPactRecordGenerator(keyCnt, valCnt, false), 1);
//		super.addOutput(this.outList);
//		
//		SelfMatchTask testTask = new SelfMatchTask();
//		super.getTaskConfig().setLocalStrategy(LocalStrategy.SORT_SELF_NESTEDLOOP);
//		
//		super.getTaskConfig().setMemorySize(6 * 1024 * 1024);
//		super.getTaskConfig().setNumFilehandles(4);
//		super.getTaskConfig().setLocalStrategyKeyTypes(0, new int[]{0});
//		super.getTaskConfig().setLocalStrategyKeyTypes(new Class[]{ PactInteger.class });
//				
//		super.registerTask(testTask, MockMatchStub.class);
//		
//		try {
//			testTask.invoke();
//		} catch (Exception e) {
//			LOG.debug(e);
//			e.printStackTrace();
//			Assert.fail("Invoke method caused exception.");
//		}
//		
//		int expCnt = keyCnt*(valCnt*valCnt);
//				
//		Assert.assertTrue("Resultset size was "+this.outList.size()+". Expected was "+expCnt, this.outList.size() == expCnt);
//		
//		HashMap<Integer,Integer> keyValCntMap = new HashMap<Integer, Integer>(keyCnt);
//		for(PactRecord record : this.outList) {
//			Integer key = record.getField(0, PactInteger.class).getValue();
//			if(!keyValCntMap.containsKey(key)) {
//				keyValCntMap.put(key,1);
//			} else {
//				keyValCntMap.put(key, keyValCntMap.get(key)+1);
//			}
//		}
//		
//		for(Integer key : keyValCntMap.keySet()) {
//			Assert.assertTrue("Invalid value count for key: "+key+". Value count was: "+keyValCntMap.get(key)+
//				" Expected was: "+(valCnt*valCnt), keyValCntMap.get(key).intValue() == (valCnt*valCnt));
//		}
//		
//		this.outList.clear();
//	}
//	
//	@Test
//	public void testSortInclSelfMatchTask() {
//
//		int keyCnt = 10;
//		int valCnt = 40;
//				
//		super.initEnvironment(6 * 1024 * 1024);
//		super.addInput(new UniformPactRecordGenerator(keyCnt, valCnt, false), 0);
//		super.addOutput(this.outList);
//		
//		SelfMatchTask testTask = new SelfMatchTask();
//		super.getTaskConfig().setLocalStrategy(LocalStrategy.SORT_SELF_NESTEDLOOP);
//		
//		super.getTaskConfig().setMemorySize(6 * 1024 * 1024);
//		super.getTaskConfig().setNumFilehandles(4);
//		super.getTaskConfig().setLocalStrategyKeyTypes(0, new int[]{0});
//		super.getTaskConfig().setLocalStrategyKeyTypes(new Class[]{ PactInteger.class });
//		super.getTaskConfig().getStubParameters().setString(SelfMatchTask.SELFMATCH_CROSS_MODE_KEY, SelfMatchTask.CrossMode.TRIANGLE_CROSS_INCL_DIAG.toString());
//				
//		super.registerTask(testTask, MockMatchStub.class);
//		
//		try {
//			testTask.invoke();
//		} catch (Exception e) {
//			LOG.debug(e);
//			e.printStackTrace();
//			Assert.fail("Invoke method caused exception.");
//		}
//		
//		int expValCnt = (int)((valCnt+1) * (valCnt/2.0f));
//		int expTotCnt = (keyCnt*expValCnt);
//				
//		Assert.assertTrue("Resultset size was "+this.outList.size()+". Expected was "+expTotCnt, this.outList.size() == expTotCnt);
//		
//		HashMap<Integer,Integer> keyValCntMap = new HashMap<Integer, Integer>(keyCnt);
//		for(PactRecord record : this.outList) {
//			
//			Integer key = record.getField(0, PactInteger.class).getValue();
//			if(!keyValCntMap.containsKey(key)) {
//				keyValCntMap.put(key,1);
//			} else {
//				keyValCntMap.put(key, keyValCntMap.get(key)+1);
//			}
//		}
//		
//		for(Integer key : keyValCntMap.keySet()) {
//			Assert.assertTrue("Invalid value count for key: "+key+". Value count was: "+keyValCntMap.get(key)+
//				" Expected was: "+expValCnt, keyValCntMap.get(key).intValue() == expValCnt);
//		}
//		
//		this.outList.clear();
//	}
//	
//	@Test
//	public void testSortExclSelfMatchTask() {
//
//		int keyCnt = 10;
//		int valCnt = 40;
//				
//		super.initEnvironment(6 * 1024 * 1024);
//		super.addInput(new UniformPactRecordGenerator(keyCnt, valCnt, false), 0);
//		super.addOutput(this.outList);
//		
//		SelfMatchTask testTask = new SelfMatchTask();
//		super.getTaskConfig().setLocalStrategy(LocalStrategy.SORT_SELF_NESTEDLOOP);
//		
//		super.getTaskConfig().setMemorySize(6 * 1024 * 1024);
//		super.getTaskConfig().setNumFilehandles(4);
//		super.getTaskConfig().setLocalStrategyKeyTypes(0, new int[]{0});
//		super.getTaskConfig().setLocalStrategyKeyTypes(new Class[]{ PactInteger.class });
//		super.getTaskConfig().getStubParameters().setString(SelfMatchTask.SELFMATCH_CROSS_MODE_KEY, SelfMatchTask.CrossMode.TRIANGLE_CROSS_EXCL_DIAG.toString());
//				
//		super.registerTask(testTask, MockMatchStub.class);
//		
//		try {
//			testTask.invoke();
//		} catch (Exception e) {
//			LOG.debug(e);
//			e.printStackTrace();
//			Assert.fail("Invoke method caused exception.");
//		}
//		
//		int expValCnt = (int)((valCnt) * ((valCnt-1.0f)/2.0f));
//		int expTotCnt = (keyCnt*expValCnt);
//				
//		Assert.assertTrue("Resultset size was "+this.outList.size()+". Expected was "+expTotCnt, this.outList.size() == expTotCnt);
//		
//		HashMap<Integer,Integer> keyValCntMap = new HashMap<Integer, Integer>(keyCnt);
//		for(PactRecord record : this.outList) {
//			
//			Integer key = record.getField(0, PactInteger.class).getValue();
//			if(!keyValCntMap.containsKey(key)) {
//				keyValCntMap.put(key,1);
//			} else {
//				keyValCntMap.put(key, keyValCntMap.get(key)+1);
//			}
//		}
//		
//		for(Integer key : keyValCntMap.keySet()) {
//			Assert.assertTrue("Invalid value count for key: "+key+". Value count was: "+keyValCntMap.get(key)+
//				" Expected was: "+(expValCnt), keyValCntMap.get(key).intValue() == expValCnt);
//		}
//		
//		this.outList.clear();
//	}
//	
//	
//	@Test
//	public void testNoneSelfMatchTask() {
//
//		int keyCnt = 100;
//		int valCnt = 5;
//				
//		super.initEnvironment(3 * 1024 * 1024);
//		super.addInput(new UniformPactRecordGenerator(keyCnt, valCnt, true), 1);
//		super.addOutput(this.outList);
//		
//		SelfMatchTask testTask = new SelfMatchTask();
//		super.getTaskConfig().setLocalStrategy(LocalStrategy.SELF_NESTEDLOOP);
//		
//		super.getTaskConfig().setMemorySize(3 * 1024 * 1024);
//		super.getTaskConfig().setNumFilehandles(4);
//		super.getTaskConfig().setLocalStrategyKeyTypes(0, new int[]{0});
//		super.getTaskConfig().setLocalStrategyKeyTypes(new Class[]{ PactInteger.class });
//		
//		super.registerTask(testTask, MockMatchStub.class);
//		
//		try {
//			testTask.invoke();
//		} catch (Exception e) {
//			LOG.debug(e);
//			Assert.fail("Invoke method caused exception.");
//		}
//		
//		int expCnt = keyCnt*(valCnt*valCnt);
//				
//		Assert.assertTrue("Resultset size was "+this.outList.size()+". Expected was "+expCnt, this.outList.size() == expCnt);
//		
//		HashMap<Integer,Integer> keyValCntMap = new HashMap<Integer, Integer>(keyCnt);
//		for(PactRecord record : this.outList) {
//			
//			Integer key = record.getField(0, PactInteger.class).getValue();
//			
//			if(!keyValCntMap.containsKey(key)) {
//				keyValCntMap.put(key,1);
//			} else {
//				keyValCntMap.put(key, keyValCntMap.get(key)+1);
//			}
//		}
//		for(Integer key : keyValCntMap.keySet()) {
//			Assert.assertTrue("Invalid value count. Value count was: "+keyValCntMap.get(key)+
//				" Expected was "+(valCnt*valCnt), keyValCntMap.get(key) == (valCnt*valCnt));
//		}
//		
//		this.outList.clear();
//		
//	}
//	
//	@Test
//	public void testFailingSortSelfMatchTask() {
//
//		int keyCnt = 20;
//		int valCnt = 20;
//		
//		super.initEnvironment(6 * 1024 * 1024);
//		super.addInput(new UniformPactRecordGenerator(keyCnt, valCnt, false), 1);
//		super.addOutput(this.outList);
//		
//		SelfMatchTask testTask = new SelfMatchTask();
//		super.getTaskConfig().setLocalStrategy(LocalStrategy.SORT_SELF_NESTEDLOOP);
//		super.getTaskConfig().setMemorySize(6 * 1024 * 1024);
//		super.getTaskConfig().setNumFilehandles(4);
//		super.getTaskConfig().setLocalStrategyKeyTypes(0, new int[]{0});
//		super.getTaskConfig().setLocalStrategyKeyTypes(new Class[]{ PactInteger.class });
//		
//		super.registerTask(testTask, MockFailingMatchStub.class);
//		
//		boolean stubFailed = false;
//		
//		try {
//			testTask.invoke();
//		} catch (Exception e) {
//			stubFailed = true;
//		}
//		
//		Assert.assertTrue("Stub exception was not forwarded.", stubFailed);
//		
//		this.outList.clear();
//		
//	}
//	
//	@Test
//	public void testCancelSelfMatchTaskWhileSorting() {
//		
//		super.initEnvironment(6 * 1024 * 1024);
//		super.addInput(new DelayingInfinitiveInputIterator(100), 1);
//		super.addOutput(new NirvanaOutputList());
//		
//		final SelfMatchTask testTask = new SelfMatchTask();
//		super.getTaskConfig().setLocalStrategy(LocalStrategy.SORT_SELF_NESTEDLOOP);
//		super.getTaskConfig().setMemorySize(6 * 1024 * 1024);
//		super.getTaskConfig().setNumFilehandles(4);
//		super.getTaskConfig().setLocalStrategyKeyTypes(0, new int[]{0});
//		super.getTaskConfig().setLocalStrategyKeyTypes(new Class[]{ PactInteger.class });
//		
//		super.registerTask(testTask, MockMatchStub.class);
//		
//		Thread taskRunner = new Thread() {
//			@Override
//			public void run() {
//				try {
//					testTask.invoke();
//				} catch (Exception ie) {
//					ie.printStackTrace();
//					Assert.fail("Task threw exception although it was properly canceled");
//				}
//			}
//		};
//		taskRunner.start();
//		
//		TaskCancelThread tct = new TaskCancelThread(1, taskRunner, testTask);
//		tct.start();
//		
//		try {
//			tct.join();
//			taskRunner.join();		
//		} catch(InterruptedException ie) {
//			Assert.fail("Joining threads failed");
//		}
//	}
//	
//	@Test
//	public void testCancelSelfMatchTaskWhileMatching() {
//		
//		int keyCnt = 20;
//		int valCnt = 20;
//		
//		super.initEnvironment(6 * 1024 * 1024);
//		super.addInput(new UniformPactRecordGenerator(keyCnt, valCnt, false), 1);
//		super.addOutput(new NirvanaOutputList());
//		
//		final SelfMatchTask testTask = new SelfMatchTask();
//		super.getTaskConfig().setLocalStrategy(LocalStrategy.SORT_SELF_NESTEDLOOP);
//		super.getTaskConfig().setMemorySize(6 * 1024 * 1024);
//		super.getTaskConfig().setNumFilehandles(4);
//		super.getTaskConfig().setLocalStrategyKeyTypes(0, new int[]{0});
//		super.getTaskConfig().setLocalStrategyKeyTypes(new Class[]{ PactInteger.class });
//		
//		super.registerTask(testTask, MockDelayingMatchStub.class);
//		
//		Thread taskRunner = new Thread() {
//			@Override
//			public void run() {
//				try {
//					testTask.invoke();
//				} catch (Exception ie) {
//					ie.printStackTrace();
//					Assert.fail("Task threw exception although it was properly canceled");
//				}
//			}
//		};
//		taskRunner.start();
//		
//		TaskCancelThread tct = new TaskCancelThread(1, taskRunner, testTask);
//		tct.start();
//		
//		try {
//			tct.join();
//			taskRunner.join();		
//		} catch(InterruptedException ie) {
//			Assert.fail("Joining threads failed");
//		}
//		
//	}
//	
//	public static class MockMatchStub extends MatchStub {
//
//		@Override
//		public void match(PactRecord value1, PactRecord value2, Collector out) throws Exception { 
//			out.collect(value1);
//		}
//		
//	}
//	
//	public static class MockFailingMatchStub extends MatchStub {
//
//		int cnt = 0;
//		
//		@Override
//		public void match(PactRecord value1, PactRecord value2, Collector out) throws Exception {
//			if(++this.cnt>=10) {
//				throw new RuntimeException("Expected Test Exception");
//			}
//			
//			out.collect(value1);
//			
//		}
//		
//	}
//	
//	
//	public static class MockDelayingMatchStub extends MatchStub {
//
//		@Override
//		public void match(PactRecord value1, PactRecord value2, Collector out)
//				throws Exception {
//			
//			try {
//				Thread.sleep(100);
//			} catch (InterruptedException e) { }
//			
//		}
//		
//	}
	
}
