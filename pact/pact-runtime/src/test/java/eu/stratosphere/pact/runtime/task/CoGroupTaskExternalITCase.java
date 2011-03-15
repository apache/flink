package eu.stratosphere.pact.runtime.task;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import eu.stratosphere.pact.common.stub.CoGroupStub;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;
import eu.stratosphere.pact.runtime.test.util.RegularlyGeneratedInputGenerator;
import eu.stratosphere.pact.runtime.test.util.TaskTestBase;

public class CoGroupTaskExternalITCase extends TaskTestBase {

	private static final Log LOG = LogFactory.getLog(CoGroupTaskExternalITCase.class);
	
	List<KeyValuePair<PactInteger,PactInteger>> outList = new ArrayList<KeyValuePair<PactInteger,PactInteger>>();

	@Test
	public void testExternalSortCoGroupTask() {

		int keyCnt1 = 16384;
		int valCnt1 = 4;
		
		int keyCnt2 = 65536;
		int valCnt2 = 1;
		
		super.initEnvironment(5*1024*1024);
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt1, valCnt1));
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt2, valCnt2));
		super.addOutput(outList);
		
		CoGroupTask testTask = new CoGroupTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.SORTMERGE);
		super.getTaskConfig().setMemorySize(5 * 1024 * 1024);
		super.getTaskConfig().setNumFilehandles(4);
		
		super.registerTask(testTask, MockCoGroupStub.class);
		
		try {
			testTask.invoke();
		} catch (Exception e) {
			LOG.debug(e);
		}
		
		int expCnt = valCnt1*valCnt2*Math.min(keyCnt1, keyCnt2) + Math.max(keyCnt1, keyCnt2) - Math.min(keyCnt1, keyCnt2);
		
		Assert.assertTrue("Resultset size was "+outList.size()+". Expected was "+expCnt, outList.size() == expCnt);
		
		outList.clear();
				
	}
	
	public static class MockCoGroupStub extends CoGroupStub<PactInteger, PactInteger, PactInteger, PactInteger, PactInteger> {

		@Override
		public void coGroup(PactInteger key, Iterator<PactInteger> values1, Iterator<PactInteger> values2,
				Collector<PactInteger, PactInteger> out) {

			int val1Cnt = 0;
			
			while(values1.hasNext()) {
				val1Cnt++;
				values1.next();
			}
			
			while(values2.hasNext()) {
				PactInteger val2 =  values2.next();
				if(val1Cnt == 0) {
					out.collect(key,val2);
				} else {
					for(int i=0; i<val1Cnt; i++) {
						out.collect(key,val2);
					}
				}
			}
		}
	
	}
		
}
