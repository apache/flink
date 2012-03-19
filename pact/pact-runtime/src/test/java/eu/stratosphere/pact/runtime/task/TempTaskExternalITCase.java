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

package eu.stratosphere.pact.runtime.task;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import eu.stratosphere.pact.common.stubs.Stub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.runtime.test.util.UniformPactRecordGenerator;
import eu.stratosphere.pact.runtime.test.util.TaskTestBase;

@SuppressWarnings("javadoc")
public class TempTaskExternalITCase extends TaskTestBase {

	private static final Log LOG = LogFactory.getLog(TempTaskExternalITCase.class);
	
	List<PactRecord> outList = new ArrayList<PactRecord>();
		
	@Test
	public void testTempTask() {

		int keyCnt = 16384;
		int valCnt = 32;
		
		super.initEnvironment(1024*1024*1);
		super.addInput(new UniformPactRecordGenerator(keyCnt, valCnt, false), 1);
		super.addOutput(this.outList);
		
		TempTask testTask = new TempTask();
		super.getTaskConfig().setMemorySize(1 * 1024 * 1024);
		
		super.registerTask(testTask, PrevStub.class);
		
		try {
			testTask.invoke();
		} catch (Exception e) {
			LOG.debug(e);
			Assert.fail("Invoke method caused exception.");
		}
		
		Assert.assertTrue(this.outList.size() == keyCnt*valCnt);
		
	}
	
	public static class PrevStub extends Stub {}
		
}
