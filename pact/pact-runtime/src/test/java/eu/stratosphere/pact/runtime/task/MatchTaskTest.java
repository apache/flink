package eu.stratosphere.pact.runtime.task;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

import eu.stratosphere.nephele.services.memorymanager.MemoryAllocationException;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MatchStub;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;

public class MatchTaskTest extends TaskTestBase {

	List<KeyValuePair<PactInteger,PactInteger>> outList = new ArrayList<KeyValuePair<PactInteger,PactInteger>>();

	@Test
	public void testSort1MatchTask() {

		InputIterator inIt1 = new InputIterator(20, 1);
		InputIterator inIt2 = new InputIterator(10, 2);
		
		super.initEnvironment(5*1024*1024);
		super.addInput(inIt1);
		super.addInput(inIt2);
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
			e.printStackTrace();
		}
		
		Assert.assertTrue("Resultset size was "+outList.size()+". Expected was 20.", outList.size() == 20);
		
		outList.clear();
		
		try {
			super.getMemoryManager().allocate(5*1024*1024);
		} catch (MemoryAllocationException e) {
			Assert.fail("MemoryManager not reset");
		}
	}
	
	@Test
	public void testSort2MatchTask() {

		InputIterator inIt1 = new InputIterator(20, 1);
		InputIterator inIt2 = new InputIterator(20, 1);
		
		super.initEnvironment(5*1024*1024);
		super.addInput(inIt1);
		super.addInput(inIt2);
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
			e.printStackTrace();
		}
		
		Assert.assertTrue("Resultset size was "+outList.size()+". Expected was 20.", outList.size() == 20);
		
		outList.clear();
		
		try {
			super.getMemoryManager().allocate(5*1024*1024);
		} catch (MemoryAllocationException e) {
			Assert.fail("MemoryManager not reset");
		}
	}
	
	@Test
	public void testSort3MatchTask() {

		InputIterator inIt1 = new InputIterator(20, 1);
		InputIterator inIt2 = new InputIterator(20, 20);
		
		super.initEnvironment(5*1024*1024);
		super.addInput(inIt1);
		super.addInput(inIt2);
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
			e.printStackTrace();
		}
		
		Assert.assertTrue("Resultset size was "+outList.size()+". Expected was 400.", outList.size() == 400);
		
		outList.clear();
		
		try {
			super.getMemoryManager().allocate(5*1024*1024);
		} catch (MemoryAllocationException e) {
			Assert.fail("MemoryManager not reset");
		}
	}
	
	@Test
	public void testSort4MatchTask() {

		InputIterator inIt1 = new InputIterator(20, 20);
		InputIterator inIt2 = new InputIterator(20, 1);
		
		super.initEnvironment(5*1024*1024);
		super.addInput(inIt1);
		super.addInput(inIt2);
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
			e.printStackTrace();
		}
		
		Assert.assertTrue("Resultset size was "+outList.size()+". Expected was 400.", outList.size() == 400);
		
		outList.clear();
		
		try {
			super.getMemoryManager().allocate(5*1024*1024);
		} catch (MemoryAllocationException e) {
			Assert.fail("MemoryManager not reset");
		}
	}
	
	@Test
	public void testSort5MatchTask() {

		InputIterator inIt1 = new InputIterator(20, 20);
		InputIterator inIt2 = new InputIterator(20, 20);
		
		super.initEnvironment(5*1024*1024);
		super.addInput(inIt1);
		super.addInput(inIt2);
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
			e.printStackTrace();
		}
		
		Assert.assertTrue("Resultset size was "+outList.size()+". Expected was 8000.", outList.size() == 8000);
		
		outList.clear();
		
		try {
			super.getMemoryManager().allocate(5*1024*1024);
		} catch (MemoryAllocationException e) {
			Assert.fail("MemoryManager not reset");
		}
	}
	
	@Test
	public void testExternalSort1MatchTask() {

		InputIterator inIt1 = new InputIterator(16384, 4);
		InputIterator inIt2 = new InputIterator(65536, 1);
		
		super.initEnvironment(5*1024*1024);
		super.addInput(inIt1);
		super.addInput(inIt2);
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
			e.printStackTrace();
		}
		
		Assert.assertTrue("Resultset size was "+outList.size()+". Expected was 65536.", outList.size() == 65536);
		
		outList.clear();
		
		try {
			super.getMemoryManager().allocate(5*1024*1024);
		} catch (MemoryAllocationException e) {
			Assert.fail("MemoryManager not reset");
		}
	}
	
	@Test
	public void testHash1MatchTask() {

		InputIterator inIt1 = new InputIterator(20, 20);
		InputIterator inIt2 = new InputIterator(20, 20);
		
		super.initEnvironment(1*1024*1024);
		super.addInput(inIt1);
		super.addInput(inIt2);
		super.addOutput(outList);
		
		MatchTask testTask = new MatchTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.HYBRIDHASH_FIRST);
		super.getTaskConfig().setIOBufferSize(1);
		
		super.registerTask(testTask, MockMatchStub.class);
		
		try {
			testTask.invoke();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		Assert.assertTrue("Resultset size was "+outList.size()+". Expected was 8000.", outList.size() == 8000);
		
		outList.clear();
		
		try {
			super.getMemoryManager().allocate(1*1024*1024);
		} catch (MemoryAllocationException e) {
			Assert.fail("MemoryManager not reset");
		}
	}
	
	@Test
	public void testHash2MatchTask() {

		InputIterator inIt1 = new InputIterator(20, 20);
		InputIterator inIt2 = new InputIterator(20, 20);
		
		super.initEnvironment(1*1024*1024);
		super.addInput(inIt1);
		super.addInput(inIt2);
		super.addOutput(outList);
		
		MatchTask testTask = new MatchTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.HYBRIDHASH_SECOND);
		super.getTaskConfig().setIOBufferSize(1);
		
		super.registerTask(testTask, MockMatchStub.class);
		
		try {
			testTask.invoke();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		Assert.assertTrue("Resultset size was "+outList.size()+". Expected was 8000.", outList.size() == 8000);
		
		outList.clear();
		
		try {
			super.getMemoryManager().allocate(1*1024*1024);
		} catch (MemoryAllocationException e) {
			Assert.fail("MemoryManager not reset");
		}
	}
	
	@Test
	public void testExternalHash1MatchTask() {

		InputIterator inIt1 = new InputIterator(32768, 4);
		InputIterator inIt2 = new InputIterator(65536, 1);
		
		super.initEnvironment(1*1024*1024);
		super.addInput(inIt1);
		super.addInput(inIt2);
		super.addOutput(outList);
		
		MatchTask testTask = new MatchTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.HYBRIDHASH_FIRST);
		super.getTaskConfig().setIOBufferSize(1);
		
		super.registerTask(testTask, MockMatchStub.class);
		
		try {
			testTask.invoke();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		Assert.assertTrue("Resultset size was "+outList.size()+". Expected was 131072.", outList.size() == 131072);
		
		outList.clear();
		
		try {
			super.getMemoryManager().allocate(1*1024*1024);
		} catch (MemoryAllocationException e) {
			Assert.fail("MemoryManager not reset");
		}
	}
	
	@Test
	public void testExternalHash2MatchTask() {

		InputIterator inIt1 = new InputIterator(32768, 4);
		InputIterator inIt2 = new InputIterator(65536, 1);
		
		super.initEnvironment(1*1024*1024);
		super.addInput(inIt1);
		super.addInput(inIt2);
		super.addOutput(outList);
		
		MatchTask testTask = new MatchTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.HYBRIDHASH_SECOND);
		super.getTaskConfig().setIOBufferSize(1);
		
		super.registerTask(testTask, MockMatchStub.class);
		
		try {
			testTask.invoke();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		Assert.assertTrue("Resultset size was "+outList.size()+". Expected was 131072.", outList.size() == 131072);
		
		outList.clear();
		
		try {
			super.getMemoryManager().allocate(1*1024*1024);
		} catch (MemoryAllocationException e) {
			Assert.fail("MemoryManager not reset");
		}
	}
	
	public static class MockMatchStub extends MatchStub<PactInteger, PactInteger, PactInteger, PactInteger, PactInteger> {

		@Override
		public void match(PactInteger key, PactInteger value1, PactInteger value2,
				Collector<PactInteger, PactInteger> out) {
			out.collect(key, value2);
			
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
