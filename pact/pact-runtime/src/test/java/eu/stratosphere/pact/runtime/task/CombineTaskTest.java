package eu.stratosphere.pact.runtime.task;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

import eu.stratosphere.pact.common.contract.ReduceContract.Combinable;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;
import eu.stratosphere.pact.runtime.test.util.RegularlyGeneratedInputGenerator;
import eu.stratosphere.pact.runtime.test.util.TaskTestBase;

public class CombineTaskTest extends TaskTestBase {

	List<KeyValuePair<PactInteger,PactInteger>> outList = new ArrayList<KeyValuePair<PactInteger,PactInteger>>();

	@Test
	public void testCombineTask() {

		int keyCnt = 100;
		int valCnt = 20;
		
		super.initEnvironment(3*1024*1024);
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt, valCnt));
		super.addOutput(outList);
		
		CombineTask testTask = new CombineTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.COMBININGSORT);
		super.getTaskConfig().setNumSortBuffer(2);
		super.getTaskConfig().setSortBufferSize(1);
		super.getTaskConfig().setMergeFactor(2);
		super.getTaskConfig().setIOBufferSize(1);
		
		super.registerTask(testTask, MockCombiningReduceStub.class);
		
		try {
			testTask.invoke();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		int expSum = 0;
		for(int i=1;i<valCnt;i++) {
			expSum+=i;
		}
		
		Assert.assertTrue("Resultset size was "+outList.size()+". Expected was "+keyCnt, outList.size() == keyCnt);
		
		for(KeyValuePair<PactInteger,PactInteger> pair : outList) {
			Assert.assertTrue("Incorrect result", pair.getValue().getValue() == expSum);
		}
		
		outList.clear();
		
	}
	
	
	@Combinable
	public static class MockCombiningReduceStub extends ReduceStub<PactInteger, PactInteger, PactInteger, PactInteger> {

		@Override
		public void reduce(PactInteger key, Iterator<PactInteger> values, Collector<PactInteger, PactInteger> out) {
			int sum = 0;
			while(values.hasNext()) {
				sum+=values.next().getValue();
			}
			out.collect(key, new PactInteger(sum-key.getValue()));			
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
