/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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

import org.junit.Assert;
import org.junit.Test;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.generic.stub.GenericMapper;
import eu.stratosphere.pact.runtime.plugable.pactrecord.PactRecordComparatorFactory;
import eu.stratosphere.pact.runtime.plugable.pactrecord.PactRecordSerializerFactory;
import eu.stratosphere.pact.runtime.shipping.ShipStrategyType;
import eu.stratosphere.pact.runtime.task.DriverStrategy;
import eu.stratosphere.pact.runtime.task.MapDriver;
import eu.stratosphere.pact.runtime.task.MapTaskTest.MockMapStub;
import eu.stratosphere.pact.runtime.task.ReduceTaskTest.MockReduceStub;
import eu.stratosphere.pact.runtime.task.RegularPactTask;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;
import eu.stratosphere.pact.runtime.test.util.UniformPactRecordGenerator;
import eu.stratosphere.pact.runtime.test.util.TaskTestBase;


public class ChainTaskTest extends TaskTestBase {
	
	private final List<PactRecord> outList = new ArrayList<PactRecord>();
	
	@SuppressWarnings("unchecked")
	private final PactRecordComparatorFactory compFact = new PactRecordComparatorFactory(new int[]{0}, new Class[]{PactInteger.class}, new boolean[] {true});
	private final PactRecordSerializerFactory serFact = PactRecordSerializerFactory.get();
	
	@Test
	public void testMapTask() {
		final int keyCnt = 100;
		final int valCnt = 20;
		
		try {
		
			// environment
			initEnvironment(3*1024*1024);
			addInput(new UniformPactRecordGenerator(keyCnt, valCnt, false), 0);
			addOutput(this.outList);
			
			// chained combine config
			{
				final TaskConfig combineConfig = new TaskConfig(new Configuration());
	
				// input
				combineConfig.addInputToGroup(0);
				combineConfig.setInputSerializer(serFact, 0);
				
				// output
				combineConfig.addOutputShipStrategy(ShipStrategyType.FORWARD);
				combineConfig.setOutputSerializer(serFact);
				
				// driver
				combineConfig.setDriverStrategy(DriverStrategy.PARTIAL_GROUP);
				combineConfig.setDriverComparator(compFact, 0);
				combineConfig.setMemoryDriver(3 * 1024 * 1024);
				
				// udf
				combineConfig.setStubClass(MockReduceStub.class);
				
				getTaskConfig().addChainedTask(ChainedCombineDriver.class, combineConfig, "combine");
			}
			
			// chained map+combine
			{
				RegularPactTask<GenericMapper<PactRecord, PactRecord>, PactRecord> testTask = 
											new RegularPactTask<GenericMapper<PactRecord, PactRecord>, PactRecord>();
				registerTask(testTask, MapDriver.class, MockMapStub.class);
				
				try {
					testTask.invoke();
				} catch (Exception e) {
					e.printStackTrace();
					Assert.fail("Invoke method caused exception.");
				}
			}
			
			Assert.assertEquals(keyCnt, this.outList.size());
		}
		catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}
	
	@Test
	public void testFailingMapTask() {
		int keyCnt = 100;
		int valCnt = 20;
		
		try {
			// environment
			initEnvironment(3*1024*1024);
			addInput(new UniformPactRecordGenerator(keyCnt, valCnt, false), 0);
			addOutput(this.outList);
	
			// chained combine config
			{
				final TaskConfig combineConfig = new TaskConfig(new Configuration());
	
				// input
				combineConfig.addInputToGroup(0);
				combineConfig.setInputSerializer(serFact, 0);
				
				// output
				combineConfig.addOutputShipStrategy(ShipStrategyType.FORWARD);
				combineConfig.setOutputSerializer(serFact);
				
				// driver
				combineConfig.setDriverStrategy(DriverStrategy.PARTIAL_GROUP);
				combineConfig.setDriverComparator(compFact, 0);
				combineConfig.setMemoryDriver(3 * 1024 * 1024);
				
				// udf
				combineConfig.setStubClass(MockFailingCombineStub.class);
				
				getTaskConfig().addChainedTask(ChainedCombineDriver.class, combineConfig, "combine");
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
		catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}
	
	public static final class MockFailingCombineStub extends ReduceStub {
		
		private int cnt = 0;

		@Override
		public void reduce(Iterator<PactRecord> records, Collector<PactRecord> out) throws Exception {
			if (++this.cnt >= 5) {
				throw new RuntimeException("Expected Test Exception");
			}
			while(records.hasNext())
				out.collect(records.next());
		}
	}
}
