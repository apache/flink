package eu.stratosphere.pact.runtime.task;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

import eu.stratosphere.nephele.services.memorymanager.MemoryAllocationException;
import eu.stratosphere.pact.common.stub.CoGroupStub;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;

public class CoGroupTaskTest extends TaskTestBase {

	List<KeyValuePair<PactInteger,PactInteger>> outList = new ArrayList<KeyValuePair<PactInteger,PactInteger>>();

	@Test
	public void testSort1CoGroupTask() {

		InputIterator inIt1 = new InputIterator(100, 2);
		InputIterator inIt2 = new InputIterator(200, 1);
		
		super.initEnvironment(5*1024*1024);
		super.addInput(inIt1);
		super.addInput(inIt2);
		super.addOutput(outList);
		
		CoGroupTask testTask = new CoGroupTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.SORTMERGE);
		super.getTaskConfig().setNumSortBuffer(4);
		super.getTaskConfig().setSortBufferSize(1);
		super.getTaskConfig().setMergeFactor(4);
		super.getTaskConfig().setIOBufferSize(1);
		
		super.registerTask(testTask, MockCoGroupStub.class);
		
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
	
	
	
	public static class MockCoGroupStub extends CoGroupStub<PactInteger, PactInteger, PactInteger, PactInteger, PactInteger> {

		@Override
		public void coGroup(PactInteger key, Iterator<PactInteger> values1, Iterator<PactInteger> values2,
				Collector<PactInteger, PactInteger> out) {

			int val1Cnt = 1;
			
			while(values1.hasNext()) {
				val1Cnt++;
				values1.next();
			}
			
			while(values2.hasNext()) {
				PactInteger val2 =  values2.next();
				for(int i=0; i<val1Cnt; i++) {
					out.collect(key,val2);
				}
			}
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
