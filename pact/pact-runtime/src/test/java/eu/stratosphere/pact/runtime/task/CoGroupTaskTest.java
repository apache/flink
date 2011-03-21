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

import eu.stratosphere.pact.common.stub.CoGroupStub;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;
import eu.stratosphere.pact.runtime.test.util.DelayingInfinitiveInputIterator;
import eu.stratosphere.pact.runtime.test.util.NirvanaOutputList;
import eu.stratosphere.pact.runtime.test.util.RegularlyGeneratedInputGenerator;
import eu.stratosphere.pact.runtime.test.util.TaskCancelThread;
import eu.stratosphere.pact.runtime.test.util.TaskTestBase;

public class CoGroupTaskTest extends TaskTestBase {

	private static final Log LOG = LogFactory.getLog(CoGroupTaskTest.class);
	
	List<KeyValuePair<PactInteger,PactInteger>> outList = new ArrayList<KeyValuePair<PactInteger,PactInteger>>();

	@Test
	public void testSortBoth1CoGroupTask() {

		int keyCnt1 = 100;
		int valCnt1 = 2;
		
		int keyCnt2 = 200;
		int valCnt2 = 1;
		
		super.initEnvironment(5*1024*1024);
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt1, valCnt1, false));
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt2, valCnt2, false));
		super.addOutput(outList);
		
		CoGroupTask testTask = new CoGroupTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.SORT_BOTH_MERGE);
		super.getTaskConfig().setMemorySize(5 * 1024 * 1024);
		super.getTaskConfig().setNumFilehandles(4);
		
		super.registerTask(testTask, MockCoGroupStub.class);
		
		try {
			testTask.invoke();
		} catch (Exception e) {
			LOG.debug(e);
		}
		
		int expCnt = valCnt1*valCnt2*Math.min(keyCnt1, keyCnt2) + Math.max(keyCnt1, keyCnt2) - Math.min(keyCnt1, keyCnt2);
		
		Assert.assertTrue("Resultset size was "+outList.size()+". Expected was "+expCnt, outList.size() == expCnt);
		
		outList.clear();
				
	}
	
	@Test
	public void testSortBoth2CoGroupTask() {

		int keyCnt1 = 200;
		int valCnt1 = 2;
		
		int keyCnt2 = 200;
		int valCnt2 = 4;
		
		super.initEnvironment(5*1024*1024);
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt1, valCnt1, false));
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt2, valCnt2, false));
		super.addOutput(outList);
		
		CoGroupTask testTask = new CoGroupTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.SORT_BOTH_MERGE);
		super.getTaskConfig().setMemorySize(5 * 1024 * 1024);
		super.getTaskConfig().setNumFilehandles(4);
		
		super.registerTask(testTask, MockCoGroupStub.class);
		
		try {
			testTask.invoke();
		} catch (Exception e) {
			LOG.debug(e);
		}
		
		int expCnt = valCnt1*valCnt2*Math.min(keyCnt1, keyCnt2) + Math.max(keyCnt1, keyCnt2) - Math.min(keyCnt1, keyCnt2);
		
		Assert.assertTrue("Resultset size was "+outList.size()+". Expected was "+expCnt, outList.size() == expCnt);
		
		outList.clear();
				
	}
	
	@Test
	public void testSortFirstCoGroupTask() {

		int keyCnt1 = 200;
		int valCnt1 = 2;
		
		int keyCnt2 = 200;
		int valCnt2 = 4;
		
		super.initEnvironment(5*1024*1024);
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt1, valCnt1, false));
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt2, valCnt2, true));
		super.addOutput(outList);
		
		CoGroupTask testTask = new CoGroupTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.SORT_FIRST_MERGE);
		super.getTaskConfig().setMemorySize(5 * 1024 * 1024);
		super.getTaskConfig().setNumFilehandles(4);
		
		super.registerTask(testTask, MockCoGroupStub.class);
		
		try {
			testTask.invoke();
		} catch (Exception e) {
			LOG.debug(e);
		}
		
		int expCnt = valCnt1*valCnt2*Math.min(keyCnt1, keyCnt2) + Math.max(keyCnt1, keyCnt2) - Math.min(keyCnt1, keyCnt2);
		
		Assert.assertTrue("Resultset size was "+outList.size()+". Expected was "+expCnt, outList.size() == expCnt);
		
		outList.clear();
				
	}
	
	@Test
	public void testSortSecondCoGroupTask() {

		int keyCnt1 = 200;
		int valCnt1 = 2;
		
		int keyCnt2 = 200;
		int valCnt2 = 4;
		
		super.initEnvironment(5*1024*1024);
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt1, valCnt1, true));
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt2, valCnt2, false));
		super.addOutput(outList);
		
		CoGroupTask testTask = new CoGroupTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.SORT_SECOND_MERGE);
		super.getTaskConfig().setMemorySize(5 * 1024 * 1024);
		super.getTaskConfig().setNumFilehandles(4);
		
		super.registerTask(testTask, MockCoGroupStub.class);
		
		try {
			testTask.invoke();
		} catch (Exception e) {
			LOG.debug(e);
		}
		
		int expCnt = valCnt1*valCnt2*Math.min(keyCnt1, keyCnt2) + Math.max(keyCnt1, keyCnt2) - Math.min(keyCnt1, keyCnt2);
		
		Assert.assertTrue("Resultset size was "+outList.size()+". Expected was "+expCnt, outList.size() == expCnt);
		
		outList.clear();
				
	}
	
	@Test
	public void testMergeCoGroupTask() {

		int keyCnt1 = 200;
		int valCnt1 = 2;
		
		int keyCnt2 = 200;
		int valCnt2 = 4;
		
		super.initEnvironment(5*1024*1024);
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt1, valCnt1, true));
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt2, valCnt2, true));
		super.addOutput(outList);
		
		CoGroupTask testTask = new CoGroupTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.MERGE);
		super.getTaskConfig().setMemorySize(5 * 1024 * 1024);
		super.getTaskConfig().setNumFilehandles(4);
		
		super.registerTask(testTask, MockCoGroupStub.class);
		
		try {
			testTask.invoke();
		} catch (Exception e) {
			LOG.debug(e);
		}
		
		int expCnt = valCnt1*valCnt2*Math.min(keyCnt1, keyCnt2) + Math.max(keyCnt1, keyCnt2) - Math.min(keyCnt1, keyCnt2);
		
		Assert.assertTrue("Resultset size was "+outList.size()+". Expected was "+expCnt, outList.size() == expCnt);
		
		outList.clear();
				
	}
	
	@Test
	public void testFailingSortCoGroupTask() {

		int keyCnt1 = 100;
		int valCnt1 = 2;
		
		int keyCnt2 = 200;
		int valCnt2 = 1;
		
		super.initEnvironment(5*1024*1024);
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt1, valCnt1, false));
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt2, valCnt2, false));
		super.addOutput(outList);
		
		CoGroupTask testTask = new CoGroupTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.SORT_BOTH_MERGE);
		super.getTaskConfig().setMemorySize(5 * 1024 * 1024);
		super.getTaskConfig().setNumFilehandles(4);
		
		super.registerTask(testTask, MockFailingCoGroupStub.class);
		
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
	public void testCancelCoGroupTaskWhileSorting1() {
		
		int keyCnt = 10;
		int valCnt = 2;
		
		super.initEnvironment(5*1024*1024);
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt, valCnt, false));
		super.addInput(new DelayingInfinitiveInputIterator(1000));
		super.addOutput(new NirvanaOutputList());
		
		final CoGroupTask testTask = new CoGroupTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.SORT_BOTH_MERGE);
		super.getTaskConfig().setMemorySize(5 * 1024 * 1024);
		super.getTaskConfig().setNumFilehandles(4);
		
		super.registerTask(testTask, MockCoGroupStub.class);
		
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
	public void testCancelCoGroupTaskWhileSorting2() {
		
		int keyCnt = 10;
		int valCnt = 2;
		
		super.initEnvironment(5*1024*1024);
		super.addInput(new DelayingInfinitiveInputIterator(1000));
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt, valCnt, false));
		super.addOutput(new NirvanaOutputList());
		
		final CoGroupTask testTask = new CoGroupTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.SORT_BOTH_MERGE);
		super.getTaskConfig().setMemorySize(5 * 1024 * 1024);
		super.getTaskConfig().setNumFilehandles(4);
		
		super.registerTask(testTask, MockCoGroupStub.class);
		
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
	public void testCancelCoGroupTaskWhileCoGrouping() {
		int keyCnt = 100;
		int valCnt = 5;
		
		super.initEnvironment(5*1024*1024);
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt, valCnt, false));
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt, valCnt, false));
		super.addOutput(new NirvanaOutputList());
		
		final CoGroupTask testTask = new CoGroupTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.SORT_BOTH_MERGE);
		super.getTaskConfig().setMemorySize(5 * 1024 * 1024);
		super.getTaskConfig().setNumFilehandles(4);
		
		super.registerTask(testTask, MockDelayingCoGroupStub.class);
		
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
		
		TaskCancelThread tct = new TaskCancelThread(2, taskRunner, testTask);
		tct.start();
		
		try {
			tct.join();
			taskRunner.join();		
		} catch(InterruptedException ie) {
			Assert.fail("Joining threads failed");
		}
	}
	
	public static class MockCoGroupStub extends CoGroupStub<PactInteger, PactInteger, PactInteger, PactInteger, PactInteger> {

		@Override
		public void coGroup(PactInteger key, Iterator<PactInteger> values1, Iterator<PactInteger> values2,
				Collector<PactInteger, PactInteger> out) {

			int val1Cnt = 0;
			
			while(values1.hasNext()) {
				val1Cnt++;
				values1.next();
			}
			
			while(values2.hasNext()) {
				PactInteger val2 =  values2.next();
				if(val1Cnt == 0) {
					out.collect(key,val2);
				} else {
					for(int i=0; i<val1Cnt; i++) {
						out.collect(key,val2);
					}
				}
			}
		}
	
	}
	
	public static class MockFailingCoGroupStub extends CoGroupStub<PactInteger, PactInteger, PactInteger, PactInteger, PactInteger> {
		
		int cnt = 0;
		
		@Override
		public void coGroup(PactInteger key, Iterator<PactInteger> values1, Iterator<PactInteger> values2,
				Collector<PactInteger, PactInteger> out) {

			int val1Cnt = 0;
			
			while(values1.hasNext()) {
				val1Cnt++;
				values1.next();
			}
			
			while(values2.hasNext()) {
				PactInteger val2 =  values2.next();
				if(val1Cnt == 0) {
					
					if(++cnt>=10) {
						throw new RuntimeException("Expected Test Exception");
					}
					
					out.collect(key,val2);
				} else {
					for(int i=0; i<val1Cnt; i++) {
						
						if(++cnt>=10) {
							throw new RuntimeException("Expected Test Exception");
						}
						
						out.collect(key,val2);
					}
				}
			}
		}
	
	}
	
	public static class MockDelayingCoGroupStub extends CoGroupStub<PactInteger, PactInteger, PactInteger, PactInteger, PactInteger> {

		@Override
		public void coGroup(PactInteger key, Iterator<PactInteger> values1, Iterator<PactInteger> values2,
				Collector<PactInteger, PactInteger> out) {

			while(values1.hasNext()) {
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) { }
				values1.next();
			}
			
			while(values2.hasNext()) {
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) { }
				values2.next();
			}
		}
	
	}
	
	
		
}
