package eu.stratosphere.pact.runtime.task;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

import eu.stratosphere.nephele.services.memorymanager.MemoryAllocationException;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.CrossStub;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;

public class CrossTaskTest extends TaskTestBase {

	List<KeyValuePair<PactInteger,PactInteger>> outList = new ArrayList<KeyValuePair<PactInteger,PactInteger>>();

	@Test
	public void testBlock1CrossTask() {

		InputIterator inIt1 = new InputIterator(200, 1);
		InputIterator inIt2 = new InputIterator(100, 2);
		
		super.initEnvironment(1*1024*1024);
		super.addInput(inIt1);
		super.addInput(inIt2);
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
		
		Assert.assertTrue("Resultset size was "+outList.size()+". Expected was 40000.", outList.size() == 40000);
		
		outList.clear();
		
		try {
			super.getMemoryManager().allocate(5*1024*1024);
		} catch (MemoryAllocationException e) {
			Assert.fail("MemoryManager not reset");
		}
	}
	
	@Test
	public void testBlock2CrossTask() {

		InputIterator inIt1 = new InputIterator(200, 1);
		InputIterator inIt2 = new InputIterator(100, 2);
		
		super.initEnvironment(1*1024*1024);
		super.addInput(inIt1);
		super.addInput(inIt2);
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
		
		Assert.assertTrue("Resultset size was "+outList.size()+". Expected was 40000.", outList.size() == 40000);
		
		outList.clear();
		
		try {
			super.getMemoryManager().allocate(5*1024*1024);
		} catch (MemoryAllocationException e) {
			Assert.fail("MemoryManager not reset");
		}
	}
	
	@Test
	public void testStream1CrossTask() {

		InputIterator inIt1 = new InputIterator(200, 1);
		InputIterator inIt2 = new InputIterator(100, 2);
		
		super.initEnvironment(1*1024*1024);
		super.addInput(inIt1);
		super.addInput(inIt2);
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
		
		Assert.assertTrue("Resultset size was "+outList.size()+". Expected was 40000.", outList.size() == 40000);
		
		outList.clear();
		
		try {
			super.getMemoryManager().allocate(5*1024*1024);
		} catch (MemoryAllocationException e) {
			Assert.fail("MemoryManager not reset");
		}
	}
	
	@Test
	public void testStream2CrossTask() {

		InputIterator inIt1 = new InputIterator(200, 1);
		InputIterator inIt2 = new InputIterator(100, 2);
		
		super.initEnvironment(1*1024*1024);
		super.addInput(inIt1);
		super.addInput(inIt2);
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
		
		Assert.assertTrue("Resultset size was "+outList.size()+". Expected was 40000.", outList.size() == 40000);
		
		outList.clear();
		
		try {
			super.getMemoryManager().allocate(5*1024*1024);
		} catch (MemoryAllocationException e) {
			Assert.fail("MemoryManager not reset");
		}
	}
	
	public static class MockCrossStub extends CrossStub<PactInteger, PactInteger, PactInteger, PactInteger, PactInteger, PactInteger> {

		@Override
		public void cross(PactInteger key1, PactInteger value1, PactInteger key2, PactInteger value2,
				Collector<PactInteger, PactInteger> out) {
			
			out.collect(key1, value1);
		}
	
	}
	
	private static class InputIterator implements Iterator<KeyValuePair<PactInteger, PactInteger>> {

		int numKeys;
		int numVals;
		
		int keyCnt = 0;
		int valCnt = 0;
		
		public InputIterator(int numKeys, int numVals) {
			this.numKeys = numKeys;
			this.numVals = numVals;
		}
		
		@Override
		public boolean hasNext() {
			if(valCnt < numVals) {
				return true;
			} else {
				return false;
			}
		}

		@Override
		public KeyValuePair<PactInteger, PactInteger> next() {
			PactInteger key = new PactInteger(keyCnt++);
			PactInteger val = new PactInteger(valCnt);
			
			if(keyCnt == numKeys) {
				keyCnt = 0;
				valCnt++;
			}
			
			return new KeyValuePair<PactInteger, PactInteger>(key,val);
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
		
	};
}
