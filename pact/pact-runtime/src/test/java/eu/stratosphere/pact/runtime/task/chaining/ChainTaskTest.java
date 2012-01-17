package eu.stratosphere.pact.runtime.task.chaining;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.runtime.task.MapTask;
import eu.stratosphere.pact.runtime.task.MapTaskTest.MockMapStub;
import eu.stratosphere.pact.runtime.task.ReduceTaskTest.MockReduceStub;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;
import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;
import eu.stratosphere.pact.runtime.test.util.RegularlyGeneratedInputGenerator;
import eu.stratosphere.pact.runtime.test.util.TaskTestBase;

/**
 * 
 * @author enijkamp
 *
 */
@SuppressWarnings("javadoc")
public class ChainTaskTest extends TaskTestBase {

	private static final Log LOG = LogFactory.getLog(ChainTaskTest.class);
	
	List<PactRecord> outList = new ArrayList<PactRecord>();
		
	@SuppressWarnings("unchecked")
	@Test
	public void testMapTask() {

		int keyCnt = 100;
		int valCnt = 20;
		
		// environment
		{
			super.initEnvironment(3*1024*1024);
			super.addInput(new RegularlyGeneratedInputGenerator(keyCnt, valCnt, false), 1);
			super.addOutput(this.outList);
		}
		
		// chained combine config
		{
			Configuration config = new Configuration();
			config.addAll(super.getConfiguration(), "");
			TaskConfig combineConfig = new TaskConfig(config);
			
			combineConfig.setStubClass(MockReduceStub.class);
			combineConfig.setLocalStrategy(LocalStrategy.COMBININGSORT);
			combineConfig.setMemorySize(3 * 1024 * 1024);
			combineConfig.setNumFilehandles(2);
			combineConfig.setLocalStrategyKeyTypes(0, new int[]{0});
			combineConfig.setLocalStrategyKeyTypes(new Class[]{ PactInteger.class });
			
			super.getTaskConfig().addChainedTask(ChainedCombineTask.class, combineConfig, "combine");
		}
		
		// chained map+combine
		{
			MapTask testTask = new MapTask();
			
			super.registerTask(testTask, MockMapStub.class);
			
			try {
				testTask.invoke();
			} catch (Exception e) {
				LOG.debug(e);
				Assert.fail("Invoke method caused exception.");
			}
		}
		
		Assert.assertEquals(keyCnt, this.outList.size());
		
	}
	
	/**
	 * TODO: bug (en)
	 * 1. ChainedCombineTask.collect gets called
	 * 2. ChainedCombineTask.collect calls done ... combing takes a while ...
	 * 3. ChainedCombineTask.CombinerThread sets ChainedCombineTask.exception
	 * 4. Additional call to ChainedCombineTask.collect (which triggers exception throwing) is missing
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testFailingMapTask() {
		
		int keyCnt = 100;
		int valCnt = 20;
		
		// environment
		{
			super.initEnvironment(3*1024*1024);
			super.addInput(new RegularlyGeneratedInputGenerator(keyCnt, valCnt, false), 1);
			super.addOutput(this.outList);
		}

		// chained combine config
		{
			Configuration config = new Configuration();
			config.addAll(super.getConfiguration(), "");
			TaskConfig combineConfig = new TaskConfig(config);
			
			combineConfig.setStubClass(MockFailingCombineStub.class);
			combineConfig.setLocalStrategy(LocalStrategy.COMBININGSORT);
			combineConfig.setMemorySize(3 * 1024 * 1024);
			combineConfig.setNumFilehandles(2);
			combineConfig.setLocalStrategyKeyTypes(0, new int[]{0});
			combineConfig.setLocalStrategyKeyTypes(new Class[]{ PactInteger.class });
			
			super.getTaskConfig().addChainedTask(ChainedCombineTask.class, combineConfig, "combine");
		}
		
		// chained map+combine
		{
			MapTask testTask = new MapTask();
			
			super.registerTask(testTask, MockMapStub.class);

			boolean stubFailed = false;
			
			try {
				testTask.invoke();
			} catch (Exception e) {
				stubFailed = true;
			}
			
			Assert.assertTrue("Stub exception was not forwarded.", stubFailed);
		}		
	}
	
	public static class MockFailingCombineStub extends ReduceStub {

		int cnt = 0;

		@Override
		public void reduce(Iterator<PactRecord> records, Collector out)
				throws Exception {
			if(++this.cnt>=5) {
				throw new RuntimeException("Expected Test Exception");
			}
			while(records.hasNext())
				out.collect(records.next());			
		}
		
	}
	
}
