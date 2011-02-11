package eu.stratosphere.pact.runtime.task;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.CrossStub;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;
import eu.stratosphere.pact.runtime.test.util.RegularlyGeneratedInputGenerator;
import eu.stratosphere.pact.runtime.test.util.TaskTestBase;

public class CrossTaskTest extends TaskTestBase {

	List<KeyValuePair<PactInteger,PactInteger>> outList = new ArrayList<KeyValuePair<PactInteger,PactInteger>>();

	@Test
	public void testBlock1CrossTask() {

		int keyCnt1 = 10;
		int valCnt1 = 1;
		
		int keyCnt2 = 100;
		int valCnt2 = 4;
		
		super.initEnvironment(1*1024*1024);
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt1, valCnt1));
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt2, valCnt2));
		super.addOutput(outList);
		
		CrossTask testTask = new CrossTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.NESTEDLOOP_BLOCKED_OUTER_FIRST);
		super.getTaskConfig().setIOBufferSize(1);
		
		super.registerTask(testTask, MockCrossStub.class);
		
		try {
			testTask.invoke();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		int expCnt = keyCnt1*valCnt1*keyCnt2*valCnt2;
		
		Assert.assertTrue("Resultset size was "+outList.size()+". Expected was "+expCnt, outList.size() == expCnt);
		
		outList.clear();
				
	}
	
	@Test
	public void testBlock2CrossTask() {

		int keyCnt1 = 10;
		int valCnt1 = 1;
		
		int keyCnt2 = 100;
		int valCnt2 = 4;
		
		super.initEnvironment(1*1024*1024);
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt1, valCnt1));
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt2, valCnt2));
		super.addOutput(outList);
		
		CrossTask testTask = new CrossTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.NESTEDLOOP_BLOCKED_OUTER_SECOND);
		super.getTaskConfig().setIOBufferSize(1);
		
		super.registerTask(testTask, MockCrossStub.class);
		
		try {
			testTask.invoke();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		int expCnt = keyCnt1*valCnt1*keyCnt2*valCnt2;
		
		Assert.assertTrue("Resultset size was "+outList.size()+". Expected was "+expCnt, outList.size() == expCnt);
		
		outList.clear();
		
	}
	
	@Test
	public void testStream1CrossTask() {

		int keyCnt1 = 10;
		int valCnt1 = 1;
		
		int keyCnt2 = 100;
		int valCnt2 = 4;
		
		super.initEnvironment(1*1024*1024);
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt1, valCnt1));
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt2, valCnt2));
		super.addOutput(outList);
		
		CrossTask testTask = new CrossTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.NESTEDLOOP_STREAMED_OUTER_FIRST);
		super.getTaskConfig().setIOBufferSize(1);
		
		super.registerTask(testTask, MockCrossStub.class);
		
		try {
			testTask.invoke();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		int expCnt = keyCnt1*valCnt1*keyCnt2*valCnt2;
		
		Assert.assertTrue("Resultset size was "+outList.size()+". Expected was "+expCnt, outList.size() == expCnt);
		
		outList.clear();
		
	}
	
	@Test
	public void testStream2CrossTask() {

		int keyCnt1 = 10;
		int valCnt1 = 1;
		
		int keyCnt2 = 100;
		int valCnt2 = 4;
		
		super.initEnvironment(1*1024*1024);
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt1, valCnt1));
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt2, valCnt2));
		super.addOutput(outList);
		
		CrossTask testTask = new CrossTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.NESTEDLOOP_STREAMED_OUTER_SECOND);
		super.getTaskConfig().setIOBufferSize(1);
		
		super.registerTask(testTask, MockCrossStub.class);
		
		try {
			testTask.invoke();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		int expCnt = keyCnt1*valCnt1*keyCnt2*valCnt2;
		
		Assert.assertTrue("Resultset size was "+outList.size()+". Expected was "+expCnt, outList.size() == expCnt);
		
		outList.clear();
		
	}
	
	public static class MockCrossStub extends CrossStub<PactInteger, PactInteger, PactInteger, PactInteger, PactInteger, PactInteger> {

		HashSet<Integer> hashSet = new HashSet<Integer>(1000);
		
		@Override
		public void cross(PactInteger key1, PactInteger value1, PactInteger key2, PactInteger value2,
				Collector<PactInteger, PactInteger> out) {
			
			Assert.assertTrue("Key was given multiple times into user code",!hashSet.contains(System.identityHashCode(key1)));
			Assert.assertTrue("Key was given multiple times into user code",!hashSet.contains(System.identityHashCode(key2)));
			Assert.assertTrue("Value was given multiple times into user code",!hashSet.contains(System.identityHashCode(value1)));
			Assert.assertTrue("Value was given multiple times into user code",!hashSet.contains(System.identityHashCode(value2)));
			
			hashSet.add(System.identityHashCode(key1));
			hashSet.add(System.identityHashCode(key2));
			hashSet.add(System.identityHashCode(value1));
			hashSet.add(System.identityHashCode(value2));
			
			out.collect(key1, value1);
		}
	}
	
}
