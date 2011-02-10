package eu.stratosphere.pact.runtime.task;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

import eu.stratosphere.nephele.services.memorymanager.MemoryAllocationException;
import eu.stratosphere.pact.common.contract.ReduceContract.Combinable;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;

public class ReduceTaskTest extends TaskTestBase {

	List<KeyValuePair<PactInteger,PactInteger>> outList = new ArrayList<KeyValuePair<PactInteger,PactInteger>>();

	@Test
	public void testReduceTask() {

		int[] inKeys = {5,4,5,5,2,3,3,2,5,4,1,4,5,3,4};
		int[] inVals = {4,1,2,5,1,2,1,2,3,3,1,2,1,3,4};
		
		super.initEnvironment(3*1024*1024);
		super.addInput(super.createInputIterator(inKeys, inVals));
		super.addOutput(outList);
		
		ReduceTask testTask = new ReduceTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.SORT);
		super.getTaskConfig().setNumSortBuffer(2);
		super.getTaskConfig().setSortBufferSize(1);
		super.getTaskConfig().setMergeFactor(4);
		super.getTaskConfig().setIOBufferSize(1);
		
		super.registerTask(testTask, MockReduceStub.class);
		
		try {
			testTask.invoke();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		Assert.assertTrue("Resultset size was "+outList.size()+". Expected was 5.", outList.size() == 5);
		
		for(KeyValuePair<PactInteger,PactInteger> pair : outList) {
			Assert.assertTrue("Incorrect result", computeSum(pair.getKey().getValue()) == pair.getValue().getValue());
		}
		
		outList.clear();
		
		try {
			super.getMemoryManager().allocate(3*1024*1024);
		} catch (MemoryAllocationException e) {
			Assert.fail("MemoryManager not reset");
		}
		
	}
	
	@Test
	public void testExternalReduceTask() {

		final int NUMKEYS = 16384;
		final int NUMVALS = 4;
		
		Iterator<KeyValuePair<PactInteger, PactInteger>> inIt = new Iterator<KeyValuePair<PactInteger, PactInteger>>() {

			int keyCnt = 0;
			int valCnt = 0;
			
			@Override
			public boolean hasNext() {
				if(valCnt < NUMVALS) {
					return true;
				} else {
					return false;
				}
			}

			@Override
			public KeyValuePair<PactInteger, PactInteger> next() {
				PactInteger key = new PactInteger(keyCnt++);
				PactInteger val = new PactInteger(valCnt);
				
				if(keyCnt == NUMKEYS) {
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
		
		super.initEnvironment(3*1024*1024);
		super.addInput(inIt);
		super.addOutput(outList);
		
		ReduceTask testTask = new ReduceTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.SORT);
		super.getTaskConfig().setNumSortBuffer(2);
		super.getTaskConfig().setSortBufferSize(1);
		super.getTaskConfig().setMergeFactor(4);
		super.getTaskConfig().setIOBufferSize(1);
		
		super.registerTask(testTask, MockReduceStub.class);
		
		try {
			testTask.invoke();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		Assert.assertTrue("Resultset size was "+outList.size()+". Expected was "+NUMKEYS, outList.size() == NUMKEYS);
		
		final int expVal = computeSum(NUMVALS-1);
		
		for(KeyValuePair<PactInteger,PactInteger> pair : outList) {
			Assert.assertTrue("Incorrect result", pair.getValue().getValue() == expVal);
		}
		
		outList.clear();
		
		try {
			super.getMemoryManager().allocate(3*1024*1024);
		} catch (MemoryAllocationException e) {
			Assert.fail("MemoryManager not reset");
		}
		
	}
	
	@Test
	public void testCombiningReduceTask() {

		int[] inKeys = {5,4,5,5,2,3,3,2,5,4,1,4,5,3,4};
		int[] inVals = {4,1,2,5,1,2,1,2,3,3,1,2,1,3,4};
		
		super.initEnvironment(3*1024*1024);
		super.addInput(super.createInputIterator(inKeys, inVals));
		super.addOutput(outList);
		
		ReduceTask testTask = new ReduceTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.COMBININGSORT);
		super.getTaskConfig().setNumSortBuffer(2);
		super.getTaskConfig().setSortBufferSize(1);
		super.getTaskConfig().setMergeFactor(4);
		super.getTaskConfig().setIOBufferSize(1);
		
		super.registerTask(testTask, MockCombiningReduceStub.class);
		
		try {
			testTask.invoke();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		Assert.assertTrue("Resultset size was "+outList.size()+". Expected was 5.", outList.size() == 5);
		
		for(KeyValuePair<PactInteger,PactInteger> pair : outList) {
			Assert.assertTrue("Incorrect result " + (computeSum(pair.getKey().getValue())%pair.getKey().getValue()) + " != " + pair.getValue().getValue(), (computeSum(pair.getKey().getValue())%pair.getKey().getValue()) == pair.getValue().getValue());
		}
		
		outList.clear();
		
		try {
			super.getMemoryManager().allocate(3*1024*1024);
		} catch (MemoryAllocationException e) {
			Assert.fail("MemoryManager not reset");
		}
		
	}
	
	@Test
	public void testExternalCombiningReduceTask() {

		final int NUMKEYS = 12288;
		final int NUMVALS = 4;
		
		Iterator<KeyValuePair<PactInteger, PactInteger>> inIt = new Iterator<KeyValuePair<PactInteger, PactInteger>>() {

			int keyCnt = 1;
			int valCnt = 1;
			
			@Override
			public boolean hasNext() {
				if(valCnt <= NUMVALS) {
					return true;
				} else {
					return false;
				}
			}

			@Override
			public KeyValuePair<PactInteger, PactInteger> next() {
				PactInteger key = new PactInteger(keyCnt++);
				PactInteger val = new PactInteger(valCnt);
				
				if(keyCnt == NUMKEYS+1) {
					keyCnt = 1;
					valCnt++;
				}
				
				return new KeyValuePair<PactInteger, PactInteger>(key,val);
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
			
		};
		
		super.initEnvironment(3*1024*1024);
		super.addInput(inIt);
		super.addOutput(outList);
		
		ReduceTask testTask = new ReduceTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.COMBININGSORT);
		super.getTaskConfig().setNumSortBuffer(2);
		super.getTaskConfig().setSortBufferSize(1);
		super.getTaskConfig().setMergeFactor(4);
		super.getTaskConfig().setIOBufferSize(1);
		
		super.registerTask(testTask, MockCombiningReduceStub.class);
		
		try {
			testTask.invoke();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		Assert.assertTrue("Resultset size was "+outList.size()+". Expected was "+NUMKEYS, outList.size() == NUMKEYS);
		
		final int expVal = computeSum(NUMVALS);
		
		for(KeyValuePair<PactInteger,PactInteger> pair : outList) {
			Assert.assertTrue("Incorrect result", (expVal%pair.getKey().getValue()) == pair.getValue().getValue());
		}
		
		outList.clear();
		
		try {
			super.getMemoryManager().allocate(3*1024*1024);
		} catch (MemoryAllocationException e) {
			Assert.fail("MemoryManager not reset");
		}
		
	}
	
	private int computeSum(int x) {
		
		int sum = 0;
		for(int i=1;i<=x;i++) {
			sum+=i;
		}
		return sum;
	}
	
	public static class MockReduceStub extends ReduceStub<PactInteger, PactInteger, PactInteger, PactInteger> {

		@Override
		public void reduce(PactInteger key, Iterator<PactInteger> values, Collector<PactInteger, PactInteger> out) {
			int sum = 0;
			while(values.hasNext()) {
				sum+=values.next().getValue();
			}
			out.collect(key, new PactInteger(sum));
		}
	}
	
	@Combinable
	public static class MockCombiningReduceStub extends ReduceStub<PactInteger, PactInteger, PactInteger, PactInteger> {

		@Override
		public void reduce(PactInteger key, Iterator<PactInteger> values, Collector<PactInteger, PactInteger> out) {
			int sum = 0;
			while(values.hasNext()) {
				sum+=values.next().getValue();
			}
			out.collect(key, new PactInteger(sum%key.getValue()));			
		}
		
		@Override
		public void combine(PactInteger key, Iterator<PactInteger> values, Collector<PactInteger, PactInteger> out) {
			int sum = 0;
			while(values.hasNext()) {
				sum+=values.next().getValue();
			}
			out.collect(key, new PactInteger(sum));
		}
		
	}
	
}
