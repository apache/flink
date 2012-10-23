/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.task.chaining;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.generic.GenericMapper;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.runtime.plugable.PactRecordComparatorFactory;
import eu.stratosphere.pact.runtime.task.MapDriver;
import eu.stratosphere.pact.runtime.task.MapTaskTest.MockMapStub;
import eu.stratosphere.pact.runtime.task.ReduceTaskTest.MockReduceStub;
import eu.stratosphere.pact.runtime.task.RegularPactTask;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;
import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;
import eu.stratosphere.pact.runtime.test.util.UniformPactRecordGenerator;
import eu.stratosphere.pact.runtime.test.util.TaskTestBase;

/**
 * @author enijkamp
 */
public class ChainTaskTest extends TaskTestBase
{

	private static final Log LOG = LogFactory.getLog(ChainTaskTest.class);
	
	private final List<PactRecord> outList = new ArrayList<PactRecord>();
		
	@SuppressWarnings("unchecked")
	@Test
	public void testMapTask() {

		int keyCnt = 100;
		int valCnt = 20;
		
		// environment
		{
			super.initEnvironment(3*1024*1024);
			super.addInput(new UniformPactRecordGenerator(keyCnt, valCnt, false), 1);
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
			
			PactRecordComparatorFactory.writeComparatorSetupToConfig(combineConfig.getConfigForInputParameters(0), new int[]{0}, new Class[]{PactInteger.class}, new boolean[] {true});
			
			super.getTaskConfig().addChainedTask(ChainedCombineDriver.class, combineConfig, "combine");
		}
		
		// chained map+combine
		{
			final RegularPactTask<GenericMapper<PactRecord, PactRecord>, PactRecord> testTask = 
										new RegularPactTask<GenericMapper<PactRecord, PactRecord>, PactRecord>();
			
			super.registerTask(testTask, MapDriver.class, MockMapStub.class);
			
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
	 * TODO: enable and fix bug
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
			super.addInput(new UniformPactRecordGenerator(keyCnt, valCnt, false), 1);
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
			PactRecordComparatorFactory.writeComparatorSetupToConfig(combineConfig.getConfigForInputParameters(0),
					new int[]{0}, new Class[]{PactInteger.class}, new boolean[]{true});
			
			super.getTaskConfig().addChainedTask(ChainedCombineDriver.class, combineConfig, "combine");
		}
		
		// chained map+combine
		{
			final RegularPactTask<GenericMapper<PactRecord, PactRecord>, PactRecord> testTask = 
										new RegularPactTask<GenericMapper<PactRecord, PactRecord>, PactRecord>();
			
			super.registerTask(testTask, MapDriver.class, MockMapStub.class);

			boolean stubFailed = false;
			
			try {
				testTask.invoke();
			} catch (Exception e) {
				stubFailed = true;
			}
			
			Assert.assertTrue("Stub exception was not forwarded.", stubFailed);
		}		
	}
	
	public static final class MockFailingCombineStub extends ReduceStub
	{
		private int cnt = 0;

		@Override
		public void reduce(Iterator<PactRecord> records, Collector<PactRecord> out) throws Exception
		{
			if (++this.cnt >= 5) {
				throw new RuntimeException("Expected Test Exception");
			}
			while(records.hasNext())
				out.collect(records.next());
		}
	}
}
