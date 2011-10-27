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

@SuppressWarnings("javadoc")
public class MatchTaskExternalITCase extends TaskTestBase {

	private static final Log LOG = LogFactory.getLog(MatchTaskExternalITCase.class);
	
	List<KeyValuePair<PactInteger,PactInteger>> outList = new ArrayList<KeyValuePair<PactInteger,PactInteger>>();

	@Test
	public void testExternalSort1MatchTask() {

		int keyCnt1 = 16384*2;
		int valCnt1 = 2;
		
		int keyCnt2 = 8192;
		int valCnt2 = 4*2;
		
		super.initEnvironment(6*1024*1024);
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt1, valCnt1, false), 1);
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt2, valCnt2, false), 2);
		super.addOutput(this.outList);
		
		MatchTask testTask = new MatchTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.SORT_BOTH_MERGE);
		super.getTaskConfig().setMemorySize(6 * 1024 * 1024);
		super.getTaskConfig().setNumFilehandles(4);
		
		super.registerTask(testTask, MockMatchStub.class);
		
		try {
			testTask.invoke();
		} catch (Exception e) {
			LOG.debug(e);
			Assert.fail("Invoke method caused exception.");
		}
		
		int expCnt = valCnt1*valCnt2*Math.min(keyCnt1, keyCnt2);
		
		Assert.assertTrue("Resultset size was "+this.outList.size()+". Expected was "+expCnt, this.outList.size() == expCnt);
		
		this.outList.clear();

	}
	
//	@Test
//	public void testExternalHash1MatchTask() {
//
//		int keyCnt1 = 32768;
//		int valCnt1 = 4;
//		
//		int keyCnt2 = 65536;
//		int valCnt2 = 1;
//		
//		super.initEnvironment(1*1024*1024);
//		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt1, valCnt1));
//		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt2, valCnt2));
//		super.addOutput(outList);
//		
//		MatchTask testTask = new MatchTask();
//		super.getTaskConfig().setLocalStrategy(LocalStrategy.HYBRIDHASH_FIRST);
//		super.getTaskConfig().setIOBufferSize(1);
//		
//		super.registerTask(testTask, MockMatchStub.class);
//		
//		try {
//			testTask.invoke();
//		} catch (Exception e) {
//			LOG.debug(e);
//		}
//		
//		int expCnt = valCnt1*valCnt2*Math.min(keyCnt1, keyCnt2);
//		
//		Assert.assertTrue("Resultset size was "+outList.size()+". Expected was "+expCnt, outList.size() == expCnt);
//		
//		outList.clear();
//		
//	}
//	
//	@Test
//	public void testExternalHash2MatchTask() {
//
//		int keyCnt1 = 32768;
//		int valCnt1 = 4;
//		
//		int keyCnt2 = 65536;
//		int valCnt2 = 1;
//		
//		super.initEnvironment(1*1024*1024);
//		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt1, valCnt1));
//		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt2, valCnt2));
//		super.addOutput(outList);
//		
//		MatchTask testTask = new MatchTask();
//		super.getTaskConfig().setLocalStrategy(LocalStrategy.HYBRIDHASH_SECOND);
//		super.getTaskConfig().setIOBufferSize(1);
//		
//		super.registerTask(testTask, MockMatchStub.class);
//		
//		try {
//			testTask.invoke();
//		} catch (Exception e) {
//			LOG.debug(e);
//		}
//		
//		int expCnt = valCnt1*valCnt2*Math.min(keyCnt1, keyCnt2);
//		
//		Assert.assertTrue("Resultset size was "+outList.size()+". Expected was "+expCnt, outList.size() == expCnt);
//		
//		outList.clear();
//		
//	}
	
	public static class MockMatchStub extends MatchStub<PactInteger, PactInteger, PactInteger, PactInteger, PactInteger> {

		HashSet<Integer> hashSet = new HashSet<Integer>(1000);
		
		@Override
		public void match(PactInteger key, PactInteger value1, PactInteger value2,
				Collector<PactInteger, PactInteger> out) {
			
			Assert.assertTrue("Key was given multiple times into user code",!this.hashSet.contains(System.identityHashCode(key)));
			Assert.assertTrue("Value was given multiple times into user code",!this.hashSet.contains(System.identityHashCode(value1)));
			Assert.assertTrue("Value was given multiple times into user code",!this.hashSet.contains(System.identityHashCode(value2)));
			
			this.hashSet.add(System.identityHashCode(key));
			this.hashSet.add(System.identityHashCode(value1));
			this.hashSet.add(System.identityHashCode(value2));
			
			out.collect(key, value1);
			
		}
		
	}
	
}
