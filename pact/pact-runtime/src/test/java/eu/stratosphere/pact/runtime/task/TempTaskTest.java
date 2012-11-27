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

package eu.stratosphere.pact.runtime.task;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.Stub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.runtime.test.util.DelayingInfinitiveInputIterator;
import eu.stratosphere.pact.runtime.test.util.DriverTestBase;
import eu.stratosphere.pact.runtime.test.util.NirvanaOutputList;
import eu.stratosphere.pact.runtime.test.util.UniformPactRecordGenerator;
import eu.stratosphere.pact.runtime.test.util.TaskCancelThread;

public class TempTaskTest extends DriverTestBase<Stub>
{
	private static final Log LOG = LogFactory.getLog(TempTaskTest.class);
	
	private final List<PactRecord> outList = new ArrayList<PactRecord>();
	
	
	public TempTaskTest() {
		super(1*1024*1024);
	}
	
	
	@Test
	public void testTempTask()
	{
		int keyCnt = 1024;
		int valCnt = 4;
		
		super.addInput(new UniformPactRecordGenerator(keyCnt, valCnt, false));
		super.addOutput(this.outList);
		
		TempDriver<PactRecord> testTask = new TempDriver<PactRecord>();
		super.getTaskConfig().setMemorySize(1 * 1024 * 1024);
		
		try {
			testDriver(testTask, PrevStub.class);
		} catch (Exception e) {
			LOG.debug(e);
			Assert.fail("Invoke method caused exception.");
		}
		
		Assert.assertTrue(this.outList.size() == keyCnt*valCnt);
		
	}
	
	@Test
	public void testCancelTempTask()
	{
		super.addInput(new DelayingInfinitiveInputIterator(100));
		super.addOutput(new NirvanaOutputList());
		
		final TempDriver<PactRecord> testTask = new TempDriver<PactRecord>();
		super.getTaskConfig().setMemorySize(1 * 1024 * 1024);
		
		Thread taskRunner = new Thread() {
			@Override
			public void run() {
				try {
					testDriver(testTask, PrevStub.class);
				} catch (Exception ie) {
					ie.printStackTrace();
					Assert.fail("Task threw exception although it was properly canceled");
				}
			}
		};
		taskRunner.start();
		
		TaskCancelThread tct = new TaskCancelThread(1, taskRunner, this);
		tct.start();
		
		try {
			tct.join();
			taskRunner.join();		
		} catch(InterruptedException ie) {
			Assert.fail("Joining threads failed");
		}
		
	}
	
	public static class PrevStub implements Stub
	{
		@Override
		public void open(Configuration parameters) throws Exception {}

		@Override
		public void close() throws Exception {}
	}	
}
