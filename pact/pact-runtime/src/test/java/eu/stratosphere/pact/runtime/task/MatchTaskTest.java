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
import java.util.List;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;
import eu.stratosphere.pact.runtime.test.util.DelayingInfinitiveInputIterator;
import eu.stratosphere.pact.runtime.test.util.NirvanaOutputList;
import eu.stratosphere.pact.runtime.test.util.RegularlyGeneratedInputGenerator;
import eu.stratosphere.pact.runtime.test.util.TaskCancelThread;
import eu.stratosphere.pact.runtime.test.util.TaskTestBase;

@SuppressWarnings("unchecked")
public class MatchTaskTest extends TaskTestBase {

	private static final Log LOG = LogFactory.getLog(MatchTaskTest.class);
	
	List<PactRecord> outList = new ArrayList<PactRecord>();

	@Test
	public void testSortBoth1MatchTask() {

		int keyCnt1 = 20;
		int valCnt1 = 1;
		
		int keyCnt2 = 10;
		int valCnt2 = 2;
				
		super.initEnvironment(6 * 1024 * 1024);
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt1, valCnt1, false));
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt2, valCnt2, false));
		super.addOutput(outList);
		
		MatchTask testTask = new MatchTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.SORT_BOTH_MERGE);
		
		super.getTaskConfig().setMemorySize(6 * 1024 * 1024);
		super.getTaskConfig().setNumFilehandles(4);
		super.getTaskConfig().setLocalStrategyKeyTypes(0, new int[]{0});
		super.getTaskConfig().setLocalStrategyKeyTypes(1, new int[]{0});
		super.getTaskConfig().setLocalStrategyKeyTypes(new Class[]{ PactInteger.class });
		
		super.registerTask(testTask, MockMatchStub.class);
		
		try {
			testTask.invoke();
		} catch (Exception e) {
			LOG.debug(e);
			Assert.fail("Invoke method caused exception.");
		}
		
		int expCnt = valCnt1*valCnt2*Math.min(keyCnt1, keyCnt2);
				
		Assert.assertTrue("Resultset size was "+outList.size()+". Expected was "+expCnt, outList.size() == expCnt);
		
		outList.clear();
		
	}
	
	@Test
	public void testSortBoth2MatchTask() {

		int keyCnt1 = 20;
		int valCnt1 = 1;
		
		int keyCnt2 = 20;
		int valCnt2 = 1;
		
		super.initEnvironment(6 * 1024 * 1024);
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt1, valCnt1, false));
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt2, valCnt2, false));
		super.addOutput(outList);
		
		MatchTask testTask = new MatchTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.SORT_BOTH_MERGE);
		super.getTaskConfig().setMemorySize(6 * 1024 * 1024);
		super.getTaskConfig().setNumFilehandles(4);
		super.getTaskConfig().setLocalStrategyKeyTypes(0, new int[]{0});
		super.getTaskConfig().setLocalStrategyKeyTypes(1, new int[]{0});
		super.getTaskConfig().setLocalStrategyKeyTypes(new Class[]{ PactInteger.class });

		
		super.registerTask(testTask, MockMatchStub.class);
		
		try {
			testTask.invoke();
		} catch (Exception e) {
			LOG.debug(e);
			Assert.fail("Invoke method caused exception.");
		}
		
		int expCnt = valCnt1*valCnt2*Math.min(keyCnt1, keyCnt2);
		
		Assert.assertTrue("Resultset size was "+outList.size()+". Expected was "+expCnt, outList.size() == expCnt);
		
		outList.clear();
		
	}
	
	@Test
	public void testSortBoth3MatchTask() {

		int keyCnt1 = 20;
		int valCnt1 = 1;
		
		int keyCnt2 = 20;
		int valCnt2 = 20;
		
		super.initEnvironment(6 * 1024 * 1024);
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt1, valCnt1, false));
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt2, valCnt2, false));
		super.addOutput(outList);
		
		MatchTask testTask = new MatchTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.SORT_BOTH_MERGE);
		super.getTaskConfig().setMemorySize(6 * 1024 * 1024);
		super.getTaskConfig().setNumFilehandles(4);
		super.getTaskConfig().setLocalStrategyKeyTypes(0, new int[]{0});
		super.getTaskConfig().setLocalStrategyKeyTypes(1, new int[]{0});
		super.getTaskConfig().setLocalStrategyKeyTypes(new Class[]{ PactInteger.class });

		
		super.registerTask(testTask, MockMatchStub.class);
		
		try {
			testTask.invoke();
		} catch (Exception e) {
			LOG.debug(e);
			Assert.fail("Invoke method caused exception.");
		}
		
		int expCnt = valCnt1*valCnt2*Math.min(keyCnt1, keyCnt2);
		
		Assert.assertTrue("Resultset size was "+outList.size()+". Expected was "+expCnt, outList.size() == expCnt);
		
		outList.clear();
		
	}
	
	@Test
	public void testSortBoth4MatchTask() {

		int keyCnt1 = 20;
		int valCnt1 = 20;
		
		int keyCnt2 = 20;
		int valCnt2 = 1;
		
		super.initEnvironment(6 * 1024 * 1024);
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt1, valCnt1, false));
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt2, valCnt2, false));
		super.addOutput(outList);
		
		MatchTask testTask = new MatchTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.SORT_BOTH_MERGE);
		super.getTaskConfig().setMemorySize(6 * 1024 * 1024);
		super.getTaskConfig().setNumFilehandles(4);
		super.getTaskConfig().setLocalStrategyKeyTypes(0, new int[]{0});
		super.getTaskConfig().setLocalStrategyKeyTypes(1, new int[]{0});
		super.getTaskConfig().setLocalStrategyKeyTypes(new Class[]{ PactInteger.class });

		
		super.registerTask(testTask, MockMatchStub.class);
		
		try {
			testTask.invoke();
		} catch (Exception e) {
			LOG.debug(e);
			Assert.fail("Invoke method caused exception.");
		}
		
		int expCnt = valCnt1*valCnt2*Math.min(keyCnt1, keyCnt2);
		
		Assert.assertTrue("Resultset size was "+outList.size()+". Expected was "+expCnt, outList.size() == expCnt);
		
		outList.clear();
		
	}
	
	@Test
	public void testSortBoth5MatchTask() {

		int keyCnt1 = 20;
		int valCnt1 = 20;
		
		int keyCnt2 = 20;
		int valCnt2 = 20;
		
		super.initEnvironment(6 * 1024 * 1024);
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt1, valCnt1, false));
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt2, valCnt2, false));
		super.addOutput(outList);
		
		MatchTask testTask = new MatchTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.SORT_BOTH_MERGE);
		super.getTaskConfig().setMemorySize(6 * 1024 * 1024);
		super.getTaskConfig().setNumFilehandles(4);
		super.getTaskConfig().setLocalStrategyKeyTypes(0, new int[]{0});
		super.getTaskConfig().setLocalStrategyKeyTypes(1, new int[]{0});
		super.getTaskConfig().setLocalStrategyKeyTypes(new Class[]{ PactInteger.class });

		
		super.registerTask(testTask, MockMatchStub.class);
		
		try {
			testTask.invoke();
		} catch (Exception e) {
			LOG.debug(e);
			Assert.fail("Invoke method caused exception.");
		}
		
		int expCnt = valCnt1*valCnt2*Math.min(keyCnt1, keyCnt2);
		
		Assert.assertTrue("Resultset size was "+outList.size()+". Expected was "+expCnt, outList.size() == expCnt);
		
		outList.clear();
		
	}
	
	@Test
	public void testSortFirstMatchTask() {

		int keyCnt1 = 20;
		int valCnt1 = 20;
		
		int keyCnt2 = 20;
		int valCnt2 = 20;
		
		super.initEnvironment(5 * 1024 * 1024);
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt1, valCnt1, false));
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt2, valCnt2, true));
		super.addOutput(outList);
		
		MatchTask testTask = new MatchTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.SORT_FIRST_MERGE);
		super.getTaskConfig().setMemorySize(5 * 1024 * 1024);
		super.getTaskConfig().setNumFilehandles(4);
		super.getTaskConfig().setLocalStrategyKeyTypes(0, new int[]{0});
		super.getTaskConfig().setLocalStrategyKeyTypes(1, new int[]{0});
		super.getTaskConfig().setLocalStrategyKeyTypes(new Class[]{ PactInteger.class });

		
		super.registerTask(testTask, MockMatchStub.class);
		
		try {
			testTask.invoke();
		} catch (Exception e) {
			LOG.debug(e);
			e.printStackTrace();
			Assert.fail("Invoke method caused exception.");
		}
		
		int expCnt = valCnt1*valCnt2*Math.min(keyCnt1, keyCnt2);
		
		Assert.assertTrue("Resultset size was "+outList.size()+". Expected was "+expCnt, outList.size() == expCnt);
		
		outList.clear();
		
	}
	
	@Test
	public void testSortSecondMatchTask() {

		int keyCnt1 = 20;
		int valCnt1 = 20;
		
		int keyCnt2 = 20;
		int valCnt2 = 20;
		
		super.initEnvironment(5 * 1024 * 1024);
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt1, valCnt1, true));
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt2, valCnt2, false));
		super.addOutput(outList);
		
		MatchTask testTask = new MatchTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.SORT_SECOND_MERGE);
		super.getTaskConfig().setMemorySize(5 * 1024 * 1024);
		super.getTaskConfig().setNumFilehandles(4);
		super.getTaskConfig().setLocalStrategyKeyTypes(0, new int[]{0});
		super.getTaskConfig().setLocalStrategyKeyTypes(1, new int[]{0});
		super.getTaskConfig().setLocalStrategyKeyTypes(new Class[]{ PactInteger.class });

		
		super.registerTask(testTask, MockMatchStub.class);
		
		try {
			testTask.invoke();
		} catch (Exception e) {
			LOG.debug(e);
			e.printStackTrace();
			Assert.fail("Invoke method caused exception.");
		}
		
		int expCnt = valCnt1*valCnt2*Math.min(keyCnt1, keyCnt2);
		
		Assert.assertTrue("Resultset size was "+outList.size()+". Expected was "+expCnt, outList.size() == expCnt);
		
		outList.clear();
		
	}
	
	@Test
	public void testMergeMatchTask() {

		int keyCnt1 = 20;
		int valCnt1 = 20;
		
		int keyCnt2 = 20;
		int valCnt2 = 20;
		
		super.initEnvironment(3 * 1024 * 1024);
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt1, valCnt1, true));
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt2, valCnt2, true));
		super.addOutput(outList);
		
		MatchTask testTask = new MatchTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.MERGE);
		super.getTaskConfig().setMemorySize(3 * 1024 * 1024);
		super.getTaskConfig().setNumFilehandles(4);
		super.getTaskConfig().setLocalStrategyKeyTypes(0, new int[]{0});
		super.getTaskConfig().setLocalStrategyKeyTypes(1, new int[]{0});
		super.getTaskConfig().setLocalStrategyKeyTypes(new Class[]{ PactInteger.class });

		
		super.registerTask(testTask, MockMatchStub.class);
		
		try {
			testTask.invoke();
		} catch (Exception e) {
			LOG.debug(e);
			Assert.fail("Invoke method caused exception.");
		}
		
		int expCnt = valCnt1*valCnt2*Math.min(keyCnt1, keyCnt2);
		
		Assert.assertTrue("Resultset size was "+outList.size()+". Expected was "+expCnt, outList.size() == expCnt);
		
		outList.clear();
		
	}
	
	@Test
	public void testFailingSortMatchTask() {

		int keyCnt1 = 20;
		int valCnt1 = 20;
		
		int keyCnt2 = 20;
		int valCnt2 = 20;
		
		super.initEnvironment(6 * 1024 * 1024);
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt1, valCnt1, false));
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt2, valCnt2, false));
		super.addOutput(outList);
		
		MatchTask testTask = new MatchTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.SORT_BOTH_MERGE);
		super.getTaskConfig().setMemorySize(6 * 1024 * 1024);
		super.getTaskConfig().setNumFilehandles(4);
		super.getTaskConfig().setLocalStrategyKeyTypes(0, new int[]{0});
		super.getTaskConfig().setLocalStrategyKeyTypes(1, new int[]{0});
		super.getTaskConfig().setLocalStrategyKeyTypes(new Class[]{ PactInteger.class });

		
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
	public void testCancelMatchTaskWhileSort1() {
		
		int keyCnt = 20;
		int valCnt = 20;
		
		super.initEnvironment(6 * 1024 * 1024);
		super.addInput(new DelayingInfinitiveInputIterator(100));
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt, valCnt, false));
		super.addOutput(new NirvanaOutputList());
		
		final MatchTask testTask = new MatchTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.SORT_BOTH_MERGE);
		super.getTaskConfig().setMemorySize(6 * 1024 * 1024);
		super.getTaskConfig().setNumFilehandles(4);
		super.getTaskConfig().setLocalStrategyKeyTypes(0, new int[]{0});
		super.getTaskConfig().setLocalStrategyKeyTypes(1, new int[]{0});
		super.getTaskConfig().setLocalStrategyKeyTypes(new Class[]{ PactInteger.class });

		
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
	public void testCancelMatchTaskWhileSort2() {
		
		int keyCnt = 20;
		int valCnt = 20;
		
		super.initEnvironment(6 * 1024 * 1024);
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt, valCnt, false));
		super.addInput(new DelayingInfinitiveInputIterator(100));
		super.addOutput(new NirvanaOutputList());
		
		final MatchTask testTask = new MatchTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.SORT_BOTH_MERGE);
		super.getTaskConfig().setMemorySize(6 * 1024 * 1024);
		super.getTaskConfig().setNumFilehandles(4);
		super.getTaskConfig().setLocalStrategyKeyTypes(0, new int[]{0});
		super.getTaskConfig().setLocalStrategyKeyTypes(1, new int[]{0});
		super.getTaskConfig().setLocalStrategyKeyTypes(new Class[]{ PactInteger.class });

		
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
	public void testCancelMatchTaskWhileMatching() {
		
		int keyCnt = 20;
		int valCnt = 20;
		
		super.initEnvironment(6 * 1024 * 1024);
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt, valCnt, false));
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt, valCnt, false));
		super.addOutput(new NirvanaOutputList());
		
		final MatchTask testTask = new MatchTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.SORT_BOTH_MERGE);
		super.getTaskConfig().setMemorySize(6 * 1024 * 1024);
		super.getTaskConfig().setNumFilehandles(4);
		super.getTaskConfig().setLocalStrategyKeyTypes(0, new int[]{0});
		super.getTaskConfig().setLocalStrategyKeyTypes(1, new int[]{0});
		super.getTaskConfig().setLocalStrategyKeyTypes(new Class[]{ PactInteger.class });

		
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
	
	
	@Test
	public void testHash1MatchTask() {

		int keyCnt1 = 20;
		int valCnt1 = 1;
		
		int keyCnt2 = 10;
		int valCnt2 = 2;
				
		super.initEnvironment(6 * 1024 * 1024);
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt1, valCnt1, false));
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt2, valCnt2, false));
		super.addOutput(outList);
		
		MatchTask testTask = new MatchTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.HYBRIDHASH_FIRST);
		
		super.getTaskConfig().setMemorySize(6 * 1024 * 1024);
		super.getTaskConfig().setNumFilehandles(4);
		super.getTaskConfig().setLocalStrategyKeyTypes(0, new int[]{0});
		super.getTaskConfig().setLocalStrategyKeyTypes(1, new int[]{0});
		super.getTaskConfig().setLocalStrategyKeyTypes(new Class[]{ PactInteger.class });

		
		super.registerTask(testTask, MockMatchStub.class);
		
		try {
			testTask.invoke();
		} catch (Exception e) {
			LOG.debug(e);
			Assert.fail("Invoke method caused exception.");
		}
		
		int expCnt = valCnt1*valCnt2*Math.min(keyCnt1, keyCnt2);
				
		Assert.assertTrue("Resultset size was "+outList.size()+". Expected was "+expCnt, outList.size() == expCnt);
		
		outList.clear();
		
	}
	
	@Test
	public void testHash2MatchTask() {

		int keyCnt1 = 20;
		int valCnt1 = 1;
		
		int keyCnt2 = 20;
		int valCnt2 = 1;
		
		super.initEnvironment(6 * 1024 * 1024);
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt1, valCnt1, false));
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt2, valCnt2, false));
		super.addOutput(outList);
		
		MatchTask testTask = new MatchTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.HYBRIDHASH_SECOND);
		super.getTaskConfig().setMemorySize(6 * 1024 * 1024);
		super.getTaskConfig().setNumFilehandles(4);
		super.getTaskConfig().setLocalStrategyKeyTypes(0, new int[]{0});
		super.getTaskConfig().setLocalStrategyKeyTypes(1, new int[]{0});
		super.getTaskConfig().setLocalStrategyKeyTypes(new Class[]{ PactInteger.class });

		
		super.registerTask(testTask, MockMatchStub.class);
		
		try {
			testTask.invoke();
		} catch (Exception e) {
			LOG.debug(e);
			Assert.fail("Invoke method caused exception.");
		}
		
		int expCnt = valCnt1*valCnt2*Math.min(keyCnt1, keyCnt2);
		
		Assert.assertTrue("Resultset size was "+outList.size()+". Expected was "+expCnt, outList.size() == expCnt);
		
		outList.clear();
		
	}
	
	@Test
	public void testHash3MatchTask() {

		int keyCnt1 = 20;
		int valCnt1 = 1;
		
		int keyCnt2 = 20;
		int valCnt2 = 20;
		
		super.initEnvironment(6 * 1024 * 1024);
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt1, valCnt1, false));
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt2, valCnt2, false));
		super.addOutput(outList);
		
		MatchTask testTask = new MatchTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.HYBRIDHASH_FIRST);
		super.getTaskConfig().setMemorySize(6 * 1024 * 1024);
		super.getTaskConfig().setNumFilehandles(4);
		super.getTaskConfig().setLocalStrategyKeyTypes(0, new int[]{0});
		super.getTaskConfig().setLocalStrategyKeyTypes(1, new int[]{0});
		super.getTaskConfig().setLocalStrategyKeyTypes(new Class[]{ PactInteger.class });

		
		super.registerTask(testTask, MockMatchStub.class);
		
		try {
			testTask.invoke();
		} catch (Exception e) {
			LOG.debug(e);
			Assert.fail("Invoke method caused exception.");
		}
		
		int expCnt = valCnt1*valCnt2*Math.min(keyCnt1, keyCnt2);
		
		Assert.assertTrue("Resultset size was "+outList.size()+". Expected was "+expCnt, outList.size() == expCnt);
		
		outList.clear();
		
	}
	
	@Test
	public void testHash4MatchTask() {

		int keyCnt1 = 20;
		int valCnt1 = 20;
		
		int keyCnt2 = 20;
		int valCnt2 = 1;
		
		super.initEnvironment(6 * 1024 * 1024);
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt1, valCnt1, false));
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt2, valCnt2, false));
		super.addOutput(outList);
		
		MatchTask testTask = new MatchTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.HYBRIDHASH_SECOND);
		super.getTaskConfig().setMemorySize(6 * 1024 * 1024);
		super.getTaskConfig().setNumFilehandles(4);
		super.getTaskConfig().setLocalStrategyKeyTypes(0, new int[]{0});
		super.getTaskConfig().setLocalStrategyKeyTypes(1, new int[]{0});
		super.getTaskConfig().setLocalStrategyKeyTypes(new Class[]{ PactInteger.class });

		
		super.registerTask(testTask, MockMatchStub.class);
		
		try {
			testTask.invoke();
		} catch (Exception e) {
			LOG.debug(e);
			Assert.fail("Invoke method caused exception.");
		}
		
		int expCnt = valCnt1*valCnt2*Math.min(keyCnt1, keyCnt2);
		
		Assert.assertTrue("Resultset size was "+outList.size()+". Expected was "+expCnt, outList.size() == expCnt);
		
		outList.clear();
		
	}
	
	@Test
	public void testHash5MatchTask() {

		int keyCnt1 = 20;
		int valCnt1 = 20;
		
		int keyCnt2 = 20;
		int valCnt2 = 20;
		
		super.initEnvironment(6 * 1024 * 1024);
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt1, valCnt1, false));
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt2, valCnt2, false));
		super.addOutput(outList);
		
		MatchTask testTask = new MatchTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.HYBRIDHASH_FIRST);
		super.getTaskConfig().setMemorySize(6 * 1024 * 1024);
		super.getTaskConfig().setNumFilehandles(4);
		super.getTaskConfig().setLocalStrategyKeyTypes(0, new int[]{0});
		super.getTaskConfig().setLocalStrategyKeyTypes(1, new int[]{0});
		super.getTaskConfig().setLocalStrategyKeyTypes(new Class[]{ PactInteger.class });

		
		super.registerTask(testTask, MockMatchStub.class);
		
		try {
			testTask.invoke();
		} catch (Exception e) {
			LOG.debug(e);
			Assert.fail("Invoke method caused exception.");
		}
		
		int expCnt = valCnt1*valCnt2*Math.min(keyCnt1, keyCnt2);
		
		Assert.assertTrue("Resultset size was "+outList.size()+". Expected was "+expCnt, outList.size() == expCnt);
		
		outList.clear();
		
	}
	
	@Test
	public void testFailingHashFirstMatchTask() {

		int keyCnt1 = 20;
		int valCnt1 = 20;
		
		int keyCnt2 = 20;
		int valCnt2 = 20;
		
		super.initEnvironment(6 * 1024 * 1024);
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt1, valCnt1, false));
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt2, valCnt2, false));
		super.addOutput(outList);
		
		MatchTask testTask = new MatchTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.HYBRIDHASH_FIRST);
		super.getTaskConfig().setMemorySize(6 * 1024 * 1024);
		super.getTaskConfig().setNumFilehandles(4);
		super.getTaskConfig().setLocalStrategyKeyTypes(0, new int[]{0});
		super.getTaskConfig().setLocalStrategyKeyTypes(1, new int[]{0});
		super.getTaskConfig().setLocalStrategyKeyTypes(new Class[]{ PactInteger.class });

		
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
	public void testFailingHashSecondMatchTask() {

		int keyCnt1 = 20;
		int valCnt1 = 20;
		
		int keyCnt2 = 20;
		int valCnt2 = 20;
		
		super.initEnvironment(6 * 1024 * 1024);
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt1, valCnt1, false));
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt2, valCnt2, false));
		super.addOutput(outList);
		
		MatchTask testTask = new MatchTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.HYBRIDHASH_SECOND);
		super.getTaskConfig().setMemorySize(6 * 1024 * 1024);
		super.getTaskConfig().setNumFilehandles(4);
		super.getTaskConfig().setLocalStrategyKeyTypes(0, new int[]{0});
		super.getTaskConfig().setLocalStrategyKeyTypes(1, new int[]{0});
		super.getTaskConfig().setLocalStrategyKeyTypes(new Class[]{ PactInteger.class });

		
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
	public void testCancelHashMatchTaskWhileBuildFirst() {
		
		int keyCnt = 20;
		int valCnt = 20;
		
		super.initEnvironment(6 * 1024 * 1024);
		super.addInput(new DelayingInfinitiveInputIterator(100));
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt, valCnt, false));
		super.addOutput(new NirvanaOutputList());
		
		final MatchTask testTask = new MatchTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.HYBRIDHASH_FIRST);
		super.getTaskConfig().setMemorySize(6 * 1024 * 1024);
		super.getTaskConfig().setNumFilehandles(4);
		super.getTaskConfig().setLocalStrategyKeyTypes(0, new int[]{0});
		super.getTaskConfig().setLocalStrategyKeyTypes(1, new int[]{0});
		super.getTaskConfig().setLocalStrategyKeyTypes(new Class[]{ PactInteger.class });

		
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
	public void testHashCancelMatchTaskWhileBuildSecond() {
		
		int keyCnt = 20;
		int valCnt = 20;
		
		super.initEnvironment(6 * 1024 * 1024);
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt, valCnt, false));
		super.addInput(new DelayingInfinitiveInputIterator(100));
		super.addOutput(new NirvanaOutputList());
		
		final MatchTask testTask = new MatchTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.HYBRIDHASH_SECOND);
		super.getTaskConfig().setMemorySize(6 * 1024 * 1024);
		super.getTaskConfig().setNumFilehandles(4);
		super.getTaskConfig().setLocalStrategyKeyTypes(0, new int[]{0});
		super.getTaskConfig().setLocalStrategyKeyTypes(1, new int[]{0});
		super.getTaskConfig().setLocalStrategyKeyTypes(new Class[]{ PactInteger.class });

		
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
	public void testHashFirstCancelMatchTaskWhileMatching() {
		
		int keyCnt = 20;
		int valCnt = 20;
		
		super.initEnvironment(6 * 1024 * 1024);
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt, valCnt, false));
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt, valCnt, false));
		super.addOutput(new NirvanaOutputList());
		
		final MatchTask testTask = new MatchTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.HYBRIDHASH_FIRST);
		super.getTaskConfig().setMemorySize(6 * 1024 * 1024);
		super.getTaskConfig().setNumFilehandles(4);
		super.getTaskConfig().setLocalStrategyKeyTypes(0, new int[]{0});
		super.getTaskConfig().setLocalStrategyKeyTypes(1, new int[]{0});
		super.getTaskConfig().setLocalStrategyKeyTypes(new Class[]{ PactInteger.class });

		
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
	
	@Test
	public void testHashSecondCancelMatchTaskWhileMatching() {
		
		int keyCnt = 20;
		int valCnt = 20;
		
		super.initEnvironment(6 * 1024 * 1024);
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt, valCnt, false));
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt, valCnt, false));
		super.addOutput(new NirvanaOutputList());
		
		final MatchTask testTask = new MatchTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.HYBRIDHASH_SECOND);
		super.getTaskConfig().setMemorySize(6 * 1024 * 1024);
		super.getTaskConfig().setNumFilehandles(4);
		super.getTaskConfig().setLocalStrategyKeyTypes(0, new int[]{0});
		super.getTaskConfig().setLocalStrategyKeyTypes(1, new int[]{0});
		super.getTaskConfig().setLocalStrategyKeyTypes(new Class[]{ PactInteger.class });

		
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
	
	// =================================================================================================
	
	public static class MockMatchStub extends MatchStub {

		@Override
		public void match(PactRecord record1, PactRecord record2, Collector out)
				throws Exception {
			
			out.collect(record1);
		}
		
	}
	
	public static class MockFailingMatchStub extends MatchStub {

		int cnt = 0;
		
		@Override
		public void match(PactRecord record1, PactRecord record2, Collector out) {
			
			if(++cnt>=10) {
				throw new RuntimeException("Expected Test Exception");
			}
			
			out.collect(record1);
			
		}
		
	}
	
	
	public static class MockDelayingMatchStub extends MatchStub {

		@Override
		public void match(PactRecord record1, PactRecord record2, Collector out) {
			
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) { }			
		}
		
	}
	
}
