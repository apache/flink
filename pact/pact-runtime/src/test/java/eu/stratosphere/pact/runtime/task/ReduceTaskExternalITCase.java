package eu.stratosphere.pact.runtime.task;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import eu.stratosphere.pact.common.contract.ReduceContract.Combinable;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;
import eu.stratosphere.pact.runtime.test.util.RegularlyGeneratedInputGenerator;
import eu.stratosphere.pact.runtime.test.util.TaskTestBase;

public class ReduceTaskExternalITCase extends TaskTestBase {

	private static final Log LOG = LogFactory.getLog(ReduceTaskExternalITCase.class);
	
	List<KeyValuePair<PactInteger,PactInteger>> outList = new ArrayList<KeyValuePair<PactInteger,PactInteger>>();

	@Test
	public void testSingleLevelMergeReduceTask() {

		int keyCnt = 8192;
		int valCnt = 8;
		
		super.initEnvironment(3*1024*1024);
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt, valCnt));
		super.addOutput(outList);
		
		ReduceTask testTask = new ReduceTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.SORT);
		super.getTaskConfig().setMemorySize(3 * 1024 * 1024);
		super.getTaskConfig().setNumFilehandles(2);
		
		super.registerTask(testTask, MockReduceStub.class);
		
		try {
			testTask.invoke();
		} catch (Exception e) {
			LOG.debug(e);
		}
		
		Assert.assertTrue("Resultset size was "+outList.size()+". Expected was "+keyCnt, outList.size() == keyCnt);
		
		for(KeyValuePair<PactInteger,PactInteger> pair : outList) {
			Assert.assertTrue("Incorrect result", pair.getValue().getValue() == valCnt-pair.getKey().getValue());
		}
		
		outList.clear();
				
	}
	
	@Test
	public void testMultiLevelMergeReduceTask() {

		int keyCnt = 32768;
		int valCnt = 8;
		
		super.initEnvironment(3*1024*1024);
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt, valCnt));
		super.addOutput(outList);
		
		ReduceTask testTask = new ReduceTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.SORT);
		super.getTaskConfig().setMemorySize(3 * 1024 * 1024);
		super.getTaskConfig().setNumFilehandles(2);
		
		super.registerTask(testTask, MockReduceStub.class);
		
		try {
			testTask.invoke();
		} catch (Exception e) {
			LOG.debug(e);
		}
		
		Assert.assertTrue("Resultset size was "+outList.size()+". Expected was "+keyCnt, outList.size() == keyCnt);
		
		for(KeyValuePair<PactInteger,PactInteger> pair : outList) {
			Assert.assertTrue("Incorrect result", pair.getValue().getValue() == valCnt-pair.getKey().getValue());
		}
		
		outList.clear();
				
	}
	
	@Test
	public void testSingleLevelMergeCombiningReduceTask() {

		int keyCnt = 8192;
		int valCnt = 8;
		
		super.initEnvironment(3*1024*1024);
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt, valCnt));
		super.addOutput(outList);
		
		ReduceTask testTask = new ReduceTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.COMBININGSORT);
		super.getTaskConfig().setMemorySize(3 * 1024 * 1024);
		super.getTaskConfig().setNumFilehandles(2);
		
		super.registerTask(testTask, MockCombiningReduceStub.class);
		
		try {
			testTask.invoke();
		} catch (Exception e) {
			LOG.debug(e);
		}
		
		int expSum = 0;
		for(int i=1;i<valCnt;i++) {
			expSum+=i;
		}
		
		Assert.assertTrue("Resultset size was "+outList.size()+". Expected was "+keyCnt, outList.size() == keyCnt);
		
		for(KeyValuePair<PactInteger,PactInteger> pair : outList) {
			Assert.assertTrue("Incorrect result", pair.getValue().getValue() == expSum-pair.getKey().getValue());
		}
		
		outList.clear();
		
	}
	
	
	@Test
	public void testMultiLevelMergeCombiningReduceTask() {

		int keyCnt = 32768;
		int valCnt = 8;
		
		super.initEnvironment(3*1024*1024);
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt, valCnt));
		super.addOutput(outList);
		
		ReduceTask testTask = new ReduceTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.COMBININGSORT);
		super.getTaskConfig().setMemorySize(3 * 1024 * 1024);
		super.getTaskConfig().setNumFilehandles(2);
		
		super.registerTask(testTask, MockCombiningReduceStub.class);
		
		try {
			testTask.invoke();
		} catch (Exception e) {
			LOG.debug(e);
		}
		
		int expSum = 0;
		for(int i=1;i<valCnt;i++) {
			expSum+=i;
		}
		
		Assert.assertTrue("Resultset size was "+outList.size()+". Expected was "+keyCnt, outList.size() == keyCnt);
		
		for(KeyValuePair<PactInteger,PactInteger> pair : outList) {
			Assert.assertTrue("Incorrect result", pair.getValue().getValue() == expSum-pair.getKey().getValue());
		}
		
		outList.clear();
		
	}
	
	private static final class MockReduceStub extends ReduceStub<PactInteger, PactInteger, PactInteger, PactInteger> {

		@Override
		public void reduce(PactInteger key, Iterator<PactInteger> values, Collector<PactInteger, PactInteger> out) {
			int cnt = 0;
			while(values.hasNext()) {
				values.next();
				cnt++;
			}
			out.collect(key, new PactInteger(cnt-key.getValue()));
		}
	}
	
	@Combinable
	private static final class MockCombiningReduceStub extends ReduceStub<PactInteger, PactInteger, PactInteger, PactInteger> {

		@Override
		public void reduce(PactInteger key, Iterator<PactInteger> values, Collector<PactInteger, PactInteger> out) {
			PactInteger pi = null;
			int sum = 0;
			
			while(values.hasNext()) {
				pi = values.next();
				sum += pi.getValue();
			}
			
			pi.setValue(sum-key.getValue());
			out.collect(key, pi);
		}
		
		@Override
		public void combine(PactInteger key, Iterator<PactInteger> values, Collector<PactInteger, PactInteger> out) {
			PactInteger pi = null;
			int sum = 0;
			
			while(values.hasNext()) {
				pi = values.next();
				sum += pi.getValue();
			}
			
			pi.setValue(sum);
			out.collect(key, pi);
		}
		
	}
	
}
