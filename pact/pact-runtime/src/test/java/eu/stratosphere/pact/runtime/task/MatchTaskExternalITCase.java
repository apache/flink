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
import eu.stratosphere.pact.runtime.test.util.RegularlyGeneratedInputGenerator;
import eu.stratosphere.pact.runtime.test.util.TaskTestBase;

public class MatchTaskExternalITCase extends TaskTestBase {

	private static final Log LOG = LogFactory.getLog(MatchTaskExternalITCase.class);
	
	List<KeyValuePair<PactInteger,PactInteger>> outList = new ArrayList<KeyValuePair<PactInteger,PactInteger>>();

	@Test
	public void testExternalSort1MatchTask() {

		int keyCnt1 = 16384;
		int valCnt1 = 2;
		
		int keyCnt2 = 8192;
		int valCnt2 = 4;
		
		super.initEnvironment(5*1024*1024);
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt1, valCnt1));
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt2, valCnt2));
		super.addOutput(outList);
		
		MatchTask testTask = new MatchTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.SORTMERGE);
		super.getTaskConfig().setNumSortBuffer(4);
		super.getTaskConfig().setSortBufferSize(1);
		super.getTaskConfig().setMergeFactor(2);
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
	public void testExternalHash1MatchTask() {

		int keyCnt1 = 32768;
		int valCnt1 = 4;
		
		int keyCnt2 = 65536;
		int valCnt2 = 1;
		
		super.initEnvironment(1*1024*1024);
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt1, valCnt1));
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt2, valCnt2));
		super.addOutput(outList);
		
		MatchTask testTask = new MatchTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.HYBRIDHASH_FIRST);
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
	public void testExternalHash2MatchTask() {

		int keyCnt1 = 32768;
		int valCnt1 = 4;
		
		int keyCnt2 = 65536;
		int valCnt2 = 1;
		
		super.initEnvironment(1*1024*1024);
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt1, valCnt1));
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt2, valCnt2));
		super.addOutput(outList);
		
		MatchTask testTask = new MatchTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.HYBRIDHASH_SECOND);
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
	
}
