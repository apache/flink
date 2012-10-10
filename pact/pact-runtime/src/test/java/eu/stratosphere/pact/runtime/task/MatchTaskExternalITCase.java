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

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.generic.stub.GenericMatcher;
import eu.stratosphere.pact.runtime.plugable.PactRecordComparator;
import eu.stratosphere.pact.runtime.task.util.LocalStrategy;
import eu.stratosphere.pact.runtime.test.util.DriverTestBase;
import eu.stratosphere.pact.runtime.test.util.UniformPactRecordGenerator;

public class MatchTaskExternalITCase extends DriverTestBase<GenericMatcher<PactRecord, PactRecord, PactRecord>>
{
	private static final Log LOG = LogFactory.getLog(MatchTaskExternalITCase.class);
	
	private final ArrayList<PactRecord> outList = new ArrayList<PactRecord>();

	
	public MatchTaskExternalITCase() {
		super(6*1024*1024);
	}
	
	@Test
	public void testExternalSort1MatchTask() {

		int keyCnt1 = 16384*2;
		int valCnt1 = 2;
		
		int keyCnt2 = 8192;
		int valCnt2 = 4*2;
		
		addInput(new UniformPactRecordGenerator(keyCnt1, valCnt1, false));
		addInput(new UniformPactRecordGenerator(keyCnt2, valCnt2, false));
		addOutput(this.outList);
		
		final MatchDriver<PactRecord, PactRecord, PactRecord> testTask = new MatchDriver<PactRecord, PactRecord, PactRecord>();
		getTaskConfig().setLocalStrategy(LocalStrategy.SORT_BOTH_MERGE);
		getTaskConfig().setMemorySize(6 * 1024 * 1024);
		getTaskConfig().setNumFilehandles(4);

		final int[] keyPos1 = new int[]{0};
		final int[] keyPos2 = new int[]{0};
		@SuppressWarnings("unchecked")
		final Class<? extends Key>[] keyClasses = (Class<? extends Key>[]) new Class[]{ PactInteger.class };
		
		addInputComparator(new PactRecordComparator(keyPos1, keyClasses));
		addInputComparator(new PactRecordComparator(keyPos2, keyClasses));
		
		try {
			testDriver(testTask, MockMatchStub.class);
		} catch (Exception e) {
			LOG.debug(e);
			Assert.fail("Invoke method caused exception.");
		}
		
		int expCnt = valCnt1*valCnt2*Math.min(keyCnt1, keyCnt2);
		
		Assert.assertTrue("Resultset size was "+this.outList.size()+". Expected was "+expCnt, this.outList.size() == expCnt);
		
		this.outList.clear();

	}
	
	@Test
	public void testExternalHash1MatchTask() {

		int keyCnt1 = 32768;
		int valCnt1 = 8;
		
		int keyCnt2 = 65536;
		int valCnt2 = 8;
		
		int expCnt = valCnt1*valCnt2*Math.min(keyCnt1, keyCnt2);
		this.outList.ensureCapacity(expCnt);
		
		addInput(new UniformPactRecordGenerator(keyCnt1, valCnt1, false));
		addInput(new UniformPactRecordGenerator(keyCnt2, valCnt2, false));
		addOutput(outList);
		
		final MatchDriver<PactRecord, PactRecord, PactRecord> testTask = new MatchDriver<PactRecord, PactRecord, PactRecord>();
		super.getTaskConfig().setMemorySize(4*1024*1024);
		super.getTaskConfig().setLocalStrategy(LocalStrategy.HYBRIDHASH_FIRST);

		final int[] keyPos1 = new int[]{0};
		final int[] keyPos2 = new int[]{0};
		@SuppressWarnings("unchecked")
		final Class<? extends Key>[] keyClasses = (Class<? extends Key>[]) new Class[]{ PactInteger.class };
		
		addInputComparator(new PactRecordComparator(keyPos1, keyClasses));
		addInputComparator(new PactRecordComparator(keyPos2, keyClasses));
		
		try {
			testDriver(testTask, MockMatchStub.class);
		} catch (Exception e) {
			LOG.debug(e);
			e.printStackTrace();
			Assert.fail("Invoke method caused exception.");
		}
		
		Assert.assertTrue("Resultset size was "+outList.size()+". Expected was "+expCnt, outList.size() == expCnt);
		
		outList.clear();
		
	}
	
	@Test
	public void testExternalHash2MatchTask() {

		int keyCnt1 = 32768;
		int valCnt1 = 8;
		
		int keyCnt2 = 65536;
		int valCnt2 = 8;
		
		addInput(new UniformPactRecordGenerator(keyCnt1, valCnt1, false));
		addInput(new UniformPactRecordGenerator(keyCnt2, valCnt2, false));
		super.addOutput(outList);
		
		final MatchDriver<PactRecord, PactRecord, PactRecord> testTask = new MatchDriver<PactRecord, PactRecord, PactRecord>();
		super.getTaskConfig().setMemorySize(4*1024*1024);
		super.getTaskConfig().setLocalStrategy(LocalStrategy.HYBRIDHASH_SECOND);
		
		final int[] keyPos1 = new int[]{0};
		final int[] keyPos2 = new int[]{0};
		@SuppressWarnings("unchecked")
		final Class<? extends Key>[] keyClasses = (Class<? extends Key>[]) new Class[]{ PactInteger.class };
		
		addInputComparator(new PactRecordComparator(keyPos1, keyClasses));
		addInputComparator(new PactRecordComparator(keyPos2, keyClasses));
		
		try {
			testDriver(testTask, MockMatchStub.class);
		} catch (Exception e) {
			LOG.debug(e);
		}
		
		int expCnt = valCnt1*valCnt2*Math.min(keyCnt1, keyCnt2);
		
		Assert.assertTrue("Resultset size was "+outList.size()+". Expected was "+expCnt, outList.size() == expCnt);
		
		outList.clear();
		
	}
	
	public static class MockMatchStub extends MatchStub
	{
		@Override
		public void match(PactRecord value1, PactRecord value2, Collector<PactRecord> out) throws Exception {
			out.collect(value1);
		}
	}
}
