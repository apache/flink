package eu.stratosphere.pact.runtime.task;

import java.util.ArrayList;
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

public class MatchTaskTest extends TaskTestBase {

	private static final Log LOG = LogFactory.getLog(MatchTaskTest.class);
	
	List<KeyValuePair<PactInteger,PactInteger>> outList = new ArrayList<KeyValuePair<PactInteger,PactInteger>>();

	@Test
	public void testSort1MatchTask() {

		int keyCnt1 = 20;
		int valCnt1 = 1;
		
		int keyCnt2 = 10;
		int valCnt2 = 2;
				
		super.initEnvironment(5*1024*1024);
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt1, valCnt1));
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt2, valCnt2));
		super.addOutput(outList);
		
		MatchTask testTask = new MatchTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.SORTMERGE);
		super.getTaskConfig().setNumSortBuffer(4);
		super.getTaskConfig().setSortBufferSize(1);
		super.getTaskConfig().setMergeFactor(4);
		super.getTaskConfig().setIOBufferSize(1);
		
		super.registerTask(testTask, MockMatchStub.class);
		
		try {
			testTask.invoke();
		} catch (Exception e) {
			LOG.debug(e);
		}
		
		int expCnt = valCnt1*valCnt2*Math.min(keyCnt1, keyCnt2);
				
		Assert.assertTrue("Resultset size was "+outList.size()+". Expected was "+expCnt, outList.size() == expCnt);
		
		outList.clear();
		
	}
	
	@Test
	public void testSort2MatchTask() {

		int keyCnt1 = 20;
		int valCnt1 = 1;
		
		int keyCnt2 = 20;
		int valCnt2 = 1;
		
		super.initEnvironment(5*1024*1024);
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt1, valCnt1));
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt2, valCnt2));
		super.addOutput(outList);
		
		MatchTask testTask = new MatchTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.SORTMERGE);
		super.getTaskConfig().setNumSortBuffer(4);
		super.getTaskConfig().setSortBufferSize(1);
		super.getTaskConfig().setMergeFactor(4);
		super.getTaskConfig().setIOBufferSize(1);
		
		super.registerTask(testTask, MockMatchStub.class);
		
		try {
			testTask.invoke();
		} catch (Exception e) {
			LOG.debug(e);
		}
		
		int expCnt = valCnt1*valCnt2*Math.min(keyCnt1, keyCnt2);
		
		Assert.assertTrue("Resultset size was "+outList.size()+". Expected was "+expCnt, outList.size() == expCnt);
		
		outList.clear();
		
	}
	
	@Test
	public void testSort3MatchTask() {

		int keyCnt1 = 20;
		int valCnt1 = 1;
		
		int keyCnt2 = 20;
		int valCnt2 = 20;
		
		super.initEnvironment(5*1024*1024);
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt1, valCnt1));
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt2, valCnt2));
		super.addOutput(outList);
		
		MatchTask testTask = new MatchTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.SORTMERGE);
		super.getTaskConfig().setNumSortBuffer(4);
		super.getTaskConfig().setSortBufferSize(1);
		super.getTaskConfig().setMergeFactor(4);
		super.getTaskConfig().setIOBufferSize(1);
		
		super.registerTask(testTask, MockMatchStub.class);
		
		try {
			testTask.invoke();
		} catch (Exception e) {
			LOG.debug(e);
		}
		
		int expCnt = valCnt1*valCnt2*Math.min(keyCnt1, keyCnt2);
		
		Assert.assertTrue("Resultset size was "+outList.size()+". Expected was "+expCnt, outList.size() == expCnt);
		
		outList.clear();
		
	}
	
	@Test
	public void testSort4MatchTask() {

		int keyCnt1 = 20;
		int valCnt1 = 20;
		
		int keyCnt2 = 20;
		int valCnt2 = 1;
		
		super.initEnvironment(5*1024*1024);
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt1, valCnt1));
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt2, valCnt2));
		super.addOutput(outList);
		
		MatchTask testTask = new MatchTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.SORTMERGE);
		super.getTaskConfig().setNumSortBuffer(4);
		super.getTaskConfig().setSortBufferSize(1);
		super.getTaskConfig().setMergeFactor(4);
		super.getTaskConfig().setIOBufferSize(1);
		
		super.registerTask(testTask, MockMatchStub.class);
		
		try {
			testTask.invoke();
		} catch (Exception e) {
			LOG.debug(e);
		}
		
		int expCnt = valCnt1*valCnt2*Math.min(keyCnt1, keyCnt2);
		
		Assert.assertTrue("Resultset size was "+outList.size()+". Expected was "+expCnt, outList.size() == expCnt);
		
		outList.clear();
		
	}
	
	@Test
	public void testSort5MatchTask() {

		int keyCnt1 = 20;
		int valCnt1 = 20;
		
		int keyCnt2 = 20;
		int valCnt2 = 20;
		
		super.initEnvironment(5*1024*1024);
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt1, valCnt1));
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt2, valCnt2));
		super.addOutput(outList);
		
		MatchTask testTask = new MatchTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.SORTMERGE);
		super.getTaskConfig().setNumSortBuffer(4);
		super.getTaskConfig().setSortBufferSize(1);
		super.getTaskConfig().setMergeFactor(4);
		super.getTaskConfig().setIOBufferSize(1);
		
		super.registerTask(testTask, MockMatchStub.class);
		
		try {
			testTask.invoke();
		} catch (Exception e) {
			LOG.debug(e);
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
		
		super.initEnvironment(5*1024*1024);
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt1, valCnt1));
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt2, valCnt2));
		super.addOutput(outList);
		
		MatchTask testTask = new MatchTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.SORTMERGE);
		super.getTaskConfig().setNumSortBuffer(4);
		super.getTaskConfig().setSortBufferSize(1);
		super.getTaskConfig().setMergeFactor(4);
		super.getTaskConfig().setIOBufferSize(1);
		
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
		
		super.initEnvironment(5*1024*1024);
		super.addInput(new DelayingInfinitiveInputIterator(100));
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt, valCnt));
		super.addOutput(new NirvanaOutputList());
		
		final MatchTask testTask = new MatchTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.SORTMERGE);
		super.getTaskConfig().setNumSortBuffer(4);
		super.getTaskConfig().setSortBufferSize(1);
		super.getTaskConfig().setMergeFactor(4);
		super.getTaskConfig().setIOBufferSize(1);
		
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
		
		super.initEnvironment(5*1024*1024);
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt, valCnt));
		super.addInput(new DelayingInfinitiveInputIterator(100));
		super.addOutput(new NirvanaOutputList());
		
		final MatchTask testTask = new MatchTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.SORTMERGE);
		super.getTaskConfig().setNumSortBuffer(4);
		super.getTaskConfig().setSortBufferSize(1);
		super.getTaskConfig().setMergeFactor(4);
		super.getTaskConfig().setIOBufferSize(1);
		
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
		
		super.initEnvironment(5*1024*1024);
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt, valCnt));
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt, valCnt));
		super.addOutput(new NirvanaOutputList());
		
		final MatchTask testTask = new MatchTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.SORTMERGE);
		super.getTaskConfig().setNumSortBuffer(4);
		super.getTaskConfig().setSortBufferSize(1);
		super.getTaskConfig().setMergeFactor(4);
		super.getTaskConfig().setIOBufferSize(1);
		
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
	
//	@Test
//	public void testHash1MatchTask() {
//
//		int keyCnt1 = 20;
//		int valCnt1 = 20;
//		
//		int keyCnt2 = 20;
//		int valCnt2 = 20;
//		
//		super.initEnvironment(1*1024*1024);
//		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt1, valCnt1));
//		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt2, valCnt2));
//		super.addOutput(outList);
//		
//		MatchTask testTask = new MatchTask();
//		super.getTaskConfig().setLocalStrategy(LocalStrategy.HYBRIDHASH_FIRST);
//		super.getTaskConfig().setIOBufferSize(1);
//		
//		super.registerTask(testTask, MockMatchStub.class);
//		
//		try {
//			testTask.invoke();
//		} catch (Exception e) {
//			LOG.debug(e);
//		}
//		
//		int expCnt = valCnt1*valCnt2*Math.min(keyCnt1, keyCnt2);
//		
//		Assert.assertTrue("Resultset size was "+outList.size()+". Expected was "+expCnt, outList.size() == expCnt);
//		
//		outList.clear();
//		
//	}
	
//	@Test
//	public void testHash2MatchTask() {
//
//		int keyCnt1 = 20;
//		int valCnt1 = 20;
//		
//		int keyCnt2 = 20;
//		int valCnt2 = 20;
//		
//		super.initEnvironment(1*1024*1024);
//		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt1, valCnt1));
//		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt2, valCnt2));
//		super.addOutput(outList);
//		
//		MatchTask testTask = new MatchTask();
//		super.getTaskConfig().setLocalStrategy(LocalStrategy.HYBRIDHASH_SECOND);
//		super.getTaskConfig().setIOBufferSize(1);
//		
//		super.registerTask(testTask, MockMatchStub.class);
//		
//		try {
//			testTask.invoke();
//		} catch (Exception e) {
//			LOG.debug(e);
//		}
//		
//		int expCnt = valCnt1*valCnt2*Math.min(keyCnt1, keyCnt2);
//		
//		Assert.assertTrue("Resultset size was "+outList.size()+". Expected was "+expCnt, outList.size() == expCnt);
//		
//		outList.clear();
//		
//	}
	
//	@Test
//	public void testFailingHashMatchTask() {
//
//		int keyCnt1 = 20;
//		int valCnt1 = 20;
//		
//		int keyCnt2 = 20;
//		int valCnt2 = 20;
//		
//		super.initEnvironment(1*1024*1024);
//		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt1, valCnt1));
//		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt2, valCnt2));
//		super.addOutput(outList);
//		
//		MatchTask testTask = new MatchTask();
//		super.getTaskConfig().setLocalStrategy(LocalStrategy.HYBRIDHASH_FIRST);
//		super.getTaskConfig().setIOBufferSize(1);
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
//		outList.clear();
//		
//	}
	
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
