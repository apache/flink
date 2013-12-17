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

import java.util.concurrent.atomic.AtomicBoolean;

import junit.framework.Assert;

import org.junit.Test;

import eu.stratosphere.api.functions.GenericCrosser;
import eu.stratosphere.api.record.functions.CrossFunction;
import eu.stratosphere.pact.runtime.test.util.DelayingInfinitiveInputIterator;
import eu.stratosphere.pact.runtime.test.util.DriverTestBase;
import eu.stratosphere.pact.runtime.test.util.ExpectedTestException;
import eu.stratosphere.pact.runtime.test.util.TaskCancelThread;
import eu.stratosphere.pact.runtime.test.util.UniformPactRecordGenerator;
import eu.stratosphere.types.PactRecord;
import eu.stratosphere.util.Collector;

public class CrossTaskTest extends DriverTestBase<GenericCrosser<PactRecord, PactRecord, PactRecord>>
{
	private static final long CROSS_MEM = 1024 * 1024;
	
	private final CountingOutputCollector output = new CountingOutputCollector();

	public CrossTaskTest() {
		super(CROSS_MEM, 0);
	}
	
	@Test
	public void testBlock1CrossTask()
	{
		int keyCnt1 = 10;
		int valCnt1 = 1;
		
		int keyCnt2 = 100;
		int valCnt2 = 4;
		
		final int expCnt = keyCnt1*valCnt1*keyCnt2*valCnt2;
		
		setOutput(this.output);
		
		addInput(new UniformPactRecordGenerator(keyCnt1, valCnt1, false));
		addInput(new UniformPactRecordGenerator(keyCnt2, valCnt2, false));
				
		getTaskConfig().setDriverStrategy(DriverStrategy.NESTEDLOOP_BLOCKED_OUTER_FIRST);
		getTaskConfig().setMemoryDriver(CROSS_MEM);
		
		final CrossDriver<PactRecord, PactRecord, PactRecord> testTask = new CrossDriver<PactRecord, PactRecord, PactRecord>();
		
		try {
			testDriver(testTask, MockCrossStub.class);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("Test failed due to an exception.");
		}
		
		Assert.assertEquals("Wrong result size.", expCnt, this.output.getNumberOfRecords());
	}
	
	@Test
	public void testBlock2CrossTask() {
		int keyCnt1 = 10;
		int valCnt1 = 1;
		
		int keyCnt2 = 100;
		int valCnt2 = 4;
		
		final int expCnt = keyCnt1*valCnt1*keyCnt2*valCnt2;
		
		setOutput(this.output);
		
		addInput(new UniformPactRecordGenerator(keyCnt1, valCnt1, false));
		addInput(new UniformPactRecordGenerator(keyCnt2, valCnt2, false));
				
		getTaskConfig().setDriverStrategy(DriverStrategy.NESTEDLOOP_BLOCKED_OUTER_SECOND);
		getTaskConfig().setMemoryDriver(CROSS_MEM);
		
		final CrossDriver<PactRecord, PactRecord, PactRecord> testTask = new CrossDriver<PactRecord, PactRecord, PactRecord>();
		
		try {
			testDriver(testTask, MockCrossStub.class);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("Test failed due to an exception.");
		}
		
		Assert.assertEquals("Wrong result size.", expCnt, this.output.getNumberOfRecords());	}
	
	@Test
	public void testFailingBlockCrossTask() {

		int keyCnt1 = 10;
		int valCnt1 = 1;
		
		int keyCnt2 = 100;
		int valCnt2 = 4;
		
		setOutput(this.output);
		
		addInput(new UniformPactRecordGenerator(keyCnt1, valCnt1, false));
		addInput(new UniformPactRecordGenerator(keyCnt2, valCnt2, false));
				
		getTaskConfig().setDriverStrategy(DriverStrategy.NESTEDLOOP_BLOCKED_OUTER_FIRST);
		getTaskConfig().setMemoryDriver(CROSS_MEM);
		
		final CrossDriver<PactRecord, PactRecord, PactRecord> testTask = new CrossDriver<PactRecord, PactRecord, PactRecord>();
		
		try {
			testDriver(testTask, MockFailingCrossStub.class);
			Assert.fail("Exception not forwarded.");
		} catch (ExpectedTestException etex) {
			// good!
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("Test failed due to an exception.");
		}
	}
	
	@Test
	public void testFailingBlockCrossTask2() {

		int keyCnt1 = 10;
		int valCnt1 = 1;
		
		int keyCnt2 = 100;
		int valCnt2 = 4;
		
		setOutput(this.output);
		
		addInput(new UniformPactRecordGenerator(keyCnt1, valCnt1, false));
		addInput(new UniformPactRecordGenerator(keyCnt2, valCnt2, false));
				
		getTaskConfig().setDriverStrategy(DriverStrategy.NESTEDLOOP_BLOCKED_OUTER_SECOND);
		getTaskConfig().setMemoryDriver(CROSS_MEM);
		
		final CrossDriver<PactRecord, PactRecord, PactRecord> testTask = new CrossDriver<PactRecord, PactRecord, PactRecord>();
		
		try {
			testDriver(testTask, MockFailingCrossStub.class);
			Assert.fail("Exception not forwarded.");
		} catch (ExpectedTestException etex) {
			// good!
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("Test failed due to an exception.");
		}
	}
	
	@Test
	public void testStream1CrossTask() {
		int keyCnt1 = 10;
		int valCnt1 = 1;
		
		int keyCnt2 = 100;
		int valCnt2 = 4;
		
		final int expCnt = keyCnt1*valCnt1*keyCnt2*valCnt2;
		
		setOutput(this.output);
		
		addInput(new UniformPactRecordGenerator(keyCnt1, valCnt1, false));
		addInput(new UniformPactRecordGenerator(keyCnt2, valCnt2, false));
				
		getTaskConfig().setDriverStrategy(DriverStrategy.NESTEDLOOP_STREAMED_OUTER_FIRST);
		getTaskConfig().setMemoryDriver(CROSS_MEM);
		
		final CrossDriver<PactRecord, PactRecord, PactRecord> testTask = new CrossDriver<PactRecord, PactRecord, PactRecord>();
		
		try {
			testDriver(testTask, MockCrossStub.class);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("Test failed due to an exception.");
		}
		
		Assert.assertEquals("Wrong result size.", expCnt, this.output.getNumberOfRecords());
		
	}
	
	@Test
	public void testStream2CrossTask() {
		int keyCnt1 = 10;
		int valCnt1 = 1;
		
		int keyCnt2 = 100;
		int valCnt2 = 4;
		
		final int expCnt = keyCnt1*valCnt1*keyCnt2*valCnt2;
		
		setOutput(this.output);
		
		addInput(new UniformPactRecordGenerator(keyCnt1, valCnt1, false));
		addInput(new UniformPactRecordGenerator(keyCnt2, valCnt2, false));
				
		getTaskConfig().setDriverStrategy(DriverStrategy.NESTEDLOOP_STREAMED_OUTER_SECOND);
		getTaskConfig().setMemoryDriver(CROSS_MEM);
		
		final CrossDriver<PactRecord, PactRecord, PactRecord> testTask = new CrossDriver<PactRecord, PactRecord, PactRecord>();
		
		try {
			testDriver(testTask, MockCrossStub.class);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("Test failed due to an exception.");
		}
		
		Assert.assertEquals("Wrong result size.", expCnt, this.output.getNumberOfRecords());
	}
	
	@Test
	public void testFailingStreamCrossTask() {
		int keyCnt1 = 10;
		int valCnt1 = 1;
		
		int keyCnt2 = 100;
		int valCnt2 = 4;
	
		setOutput(this.output);
		
		addInput(new UniformPactRecordGenerator(keyCnt1, valCnt1, false));
		addInput(new UniformPactRecordGenerator(keyCnt2, valCnt2, false));
				
		getTaskConfig().setDriverStrategy(DriverStrategy.NESTEDLOOP_STREAMED_OUTER_FIRST);
		getTaskConfig().setMemoryDriver(CROSS_MEM);
		
		final CrossDriver<PactRecord, PactRecord, PactRecord> testTask = new CrossDriver<PactRecord, PactRecord, PactRecord>();
		
		try {
			testDriver(testTask, MockFailingCrossStub.class);
			Assert.fail("Exception not forwarded.");
		} catch (ExpectedTestException etex) {
			// good!
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("Test failed due to an exception.");
		}
	}
	
	@Test
	public void testFailingStreamCrossTask2() {
		int keyCnt1 = 10;
		int valCnt1 = 1;
		
		int keyCnt2 = 100;
		int valCnt2 = 4;
	
		setOutput(this.output);
		
		addInput(new UniformPactRecordGenerator(keyCnt1, valCnt1, false));
		addInput(new UniformPactRecordGenerator(keyCnt2, valCnt2, false));
				
		getTaskConfig().setDriverStrategy(DriverStrategy.NESTEDLOOP_STREAMED_OUTER_SECOND);
		getTaskConfig().setMemoryDriver(CROSS_MEM);
		
		final CrossDriver<PactRecord, PactRecord, PactRecord> testTask = new CrossDriver<PactRecord, PactRecord, PactRecord>();
		
		try {
			testDriver(testTask, MockFailingCrossStub.class);
			Assert.fail("Exception not forwarded.");
		} catch (ExpectedTestException etex) {
			// good!
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("Test failed due to an exception.");
		}
	}

	@Test
	public void testStreamEmptyInnerCrossTask() {
		int keyCnt1 = 10;
		int valCnt1 = 1;
		
		int keyCnt2 = 0;
		int valCnt2 = 0;

		final int expCnt = keyCnt1*valCnt1*keyCnt2*valCnt2;
		
		setOutput(this.output);
		
		addInput(new UniformPactRecordGenerator(keyCnt1, valCnt1, false));
		addInput(new UniformPactRecordGenerator(keyCnt2, valCnt2, false));
				
		getTaskConfig().setDriverStrategy(DriverStrategy.NESTEDLOOP_STREAMED_OUTER_FIRST);
		getTaskConfig().setMemoryDriver(CROSS_MEM);
		
		final CrossDriver<PactRecord, PactRecord, PactRecord> testTask = new CrossDriver<PactRecord, PactRecord, PactRecord>();
		
		try {
			testDriver(testTask, MockCrossStub.class);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("Test failed due to an exception.");
		}
		
		Assert.assertEquals("Wrong result size.", expCnt, this.output.getNumberOfRecords());
	}
	
	@Test
	public void testStreamEmptyOuterCrossTask() {
		int keyCnt1 = 10;
		int valCnt1 = 1;
		
		int keyCnt2 = 0;
		int valCnt2 = 0;
		
		final int expCnt = keyCnt1*valCnt1*keyCnt2*valCnt2;
		
		setOutput(this.output);
		
		addInput(new UniformPactRecordGenerator(keyCnt1, valCnt1, false));
		addInput(new UniformPactRecordGenerator(keyCnt2, valCnt2, false));
				
		getTaskConfig().setDriverStrategy(DriverStrategy.NESTEDLOOP_STREAMED_OUTER_SECOND);
		getTaskConfig().setMemoryDriver(CROSS_MEM);
		
		final CrossDriver<PactRecord, PactRecord, PactRecord> testTask = new CrossDriver<PactRecord, PactRecord, PactRecord>();
		
		try {
			testDriver(testTask, MockCrossStub.class);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("Test failed due to an exception.");
		}
		
		Assert.assertEquals("Wrong result size.", expCnt, this.output.getNumberOfRecords());
	}
	
	@Test
	public void testBlockEmptyInnerCrossTask() {
		int keyCnt1 = 10;
		int valCnt1 = 1;
		
		int keyCnt2 = 0;
		int valCnt2 = 0;
		
		final int expCnt = keyCnt1*valCnt1*keyCnt2*valCnt2;
		
		setOutput(this.output);
		
		addInput(new UniformPactRecordGenerator(keyCnt1, valCnt1, false));
		addInput(new UniformPactRecordGenerator(keyCnt2, valCnt2, false));
				
		getTaskConfig().setDriverStrategy(DriverStrategy.NESTEDLOOP_BLOCKED_OUTER_FIRST);
		getTaskConfig().setMemoryDriver(CROSS_MEM);
		
		final CrossDriver<PactRecord, PactRecord, PactRecord> testTask = new CrossDriver<PactRecord, PactRecord, PactRecord>();
		
		try {
			testDriver(testTask, MockCrossStub.class);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("Test failed due to an exception.");
		}
		
		Assert.assertEquals("Wrong result size.", expCnt, this.output.getNumberOfRecords());
	}
	
	@Test
	public void testBlockEmptyOuterCrossTask() {
		int keyCnt1 = 10;
		int valCnt1 = 1;
		
		int keyCnt2 = 0;
		int valCnt2 = 0;
		
		final int expCnt = keyCnt1*valCnt1*keyCnt2*valCnt2;
		
		setOutput(this.output);
		
		addInput(new UniformPactRecordGenerator(keyCnt1, valCnt1, false));
		addInput(new UniformPactRecordGenerator(keyCnt2, valCnt2, false));
				
		getTaskConfig().setDriverStrategy(DriverStrategy.NESTEDLOOP_BLOCKED_OUTER_SECOND);
		getTaskConfig().setMemoryDriver(CROSS_MEM);
		
		final CrossDriver<PactRecord, PactRecord, PactRecord> testTask = new CrossDriver<PactRecord, PactRecord, PactRecord>();
		
		try {
			testDriver(testTask, MockCrossStub.class);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("Test failed due to an exception.");
		}
		
		Assert.assertEquals("Wrong result size.", expCnt, this.output.getNumberOfRecords());
	}
	
	
	
	@Test
	public void testCancelBlockCrossTaskInit() {
		int keyCnt = 10;
		int valCnt = 1;

		setOutput(this.output);
		
		addInput(new UniformPactRecordGenerator(keyCnt, valCnt, false));
		addInput(new DelayingInfinitiveInputIterator(100));
		
		getTaskConfig().setDriverStrategy(DriverStrategy.NESTEDLOOP_BLOCKED_OUTER_FIRST);
		getTaskConfig().setMemoryDriver(CROSS_MEM);
		
		final CrossDriver<PactRecord, PactRecord, PactRecord> testTask = new CrossDriver<PactRecord, PactRecord, PactRecord>();
		
		final AtomicBoolean success = new AtomicBoolean(false);
		
		Thread taskRunner = new Thread() {
			@Override
			public void run() {
				try {
					testDriver(testTask, MockCrossStub.class);
					success.set(true);
				} catch (Exception ie) {
					ie.printStackTrace();
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
		
		Assert.assertTrue("Exception was thrown despite proper canceling.", success.get());
	}
	
	@Test
	public void testCancelBlockCrossTaskCrossing() {
		int keyCnt = 10;
		int valCnt = 1;
		
		setOutput(this.output);
		
		addInput(new UniformPactRecordGenerator(keyCnt, valCnt, false));
		addInput(new DelayingInfinitiveInputIterator(100));
		
		getTaskConfig().setDriverStrategy(DriverStrategy.NESTEDLOOP_BLOCKED_OUTER_SECOND);
		getTaskConfig().setMemoryDriver(CROSS_MEM);
		
		final CrossDriver<PactRecord, PactRecord, PactRecord> testTask = new CrossDriver<PactRecord, PactRecord, PactRecord>();
		
		final AtomicBoolean success = new AtomicBoolean(false);
		
		Thread taskRunner = new Thread() {
			@Override
			public void run() {
				try {
					testDriver(testTask, MockCrossStub.class);
					success.set(true);
				} catch (Exception ie) {
					ie.printStackTrace();
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
		
		Assert.assertTrue("Exception was thrown despite proper canceling.", success.get());
	}
	
	@Test
	public void testCancelStreamCrossTaskInit() {
		int keyCnt = 10;
		int valCnt = 1;
		
		setOutput(this.output);
		
		addInput(new UniformPactRecordGenerator(keyCnt, valCnt, false));
		addInput(new DelayingInfinitiveInputIterator(100));
		
		getTaskConfig().setDriverStrategy(DriverStrategy.NESTEDLOOP_STREAMED_OUTER_FIRST);
		getTaskConfig().setMemoryDriver(CROSS_MEM);
		
		final CrossDriver<PactRecord, PactRecord, PactRecord> testTask = new CrossDriver<PactRecord, PactRecord, PactRecord>();
		
		final AtomicBoolean success = new AtomicBoolean(false);
		
		Thread taskRunner = new Thread() {
			@Override
			public void run() {
				try {
					testDriver(testTask, MockCrossStub.class);
					success.set(true);
				} catch (Exception ie) {
					ie.printStackTrace();
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
		
		Assert.assertTrue("Exception was thrown despite proper canceling.", success.get());
	}
	
	@Test
	public void testCancelStreamCrossTaskCrossing() {
		int keyCnt = 10;
		int valCnt = 1;
		
		setOutput(this.output);
		
		addInput(new UniformPactRecordGenerator(keyCnt, valCnt, false));
		addInput(new DelayingInfinitiveInputIterator(100));
		
		getTaskConfig().setDriverStrategy(DriverStrategy.NESTEDLOOP_STREAMED_OUTER_SECOND);
		getTaskConfig().setMemoryDriver(CROSS_MEM);
		
		final CrossDriver<PactRecord, PactRecord, PactRecord> testTask = new CrossDriver<PactRecord, PactRecord, PactRecord>();
		
		final AtomicBoolean success = new AtomicBoolean(false);
		
		Thread taskRunner = new Thread() {
			@Override
			public void run() {
				try {
					testDriver(testTask, MockCrossStub.class);
					success.set(true);
				} catch (Exception ie) {
					ie.printStackTrace();
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
		
		Assert.assertTrue("Exception was thrown despite proper canceling.", success.get());
	}
	
	public static final class MockCrossStub extends CrossFunction
	{
		@Override
		public void cross(PactRecord record1, PactRecord record2, Collector<PactRecord> out) {
			out.collect(record1);
		}
	}
	
	public static final class MockFailingCrossStub extends CrossFunction
	{
		private int cnt = 0;
		
		@Override
		public void cross(PactRecord record1, PactRecord record2, Collector<PactRecord> out) {
			if (++this.cnt >= 10) {
				throw new ExpectedTestException();
			}
			out.collect(record1);
		}
	}
}
