/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.task;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.junit.Test;
import org.junit.Assert;

import eu.stratosphere.api.common.functions.GenericMapper;
import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.pact.runtime.test.util.DiscardingOutputCollector;
import eu.stratosphere.pact.runtime.test.util.DriverTestBase;
import eu.stratosphere.pact.runtime.test.util.ExpectedTestException;
import eu.stratosphere.pact.runtime.test.util.InfiniteInputIterator;
import eu.stratosphere.pact.runtime.test.util.TaskCancelThread;
import eu.stratosphere.pact.runtime.test.util.UniformRecordGenerator;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;

public class MapTaskTest extends DriverTestBase<GenericMapper<Record, Record>> {
	
	private static final Log LOG = LogFactory.getLog(MapTaskTest.class);
	
	private final CountingOutputCollector output = new CountingOutputCollector();
	
	
	public MapTaskTest() {
		super(0, 0);
	}
	
	@Test
	public void testMapTask() {
		final int keyCnt = 100;
		final int valCnt = 20;
		
		addInput(new UniformRecordGenerator(keyCnt, valCnt, false));
		setOutput(this.output);
		
		final MapDriver<Record, Record> testDriver = new MapDriver<Record, Record>();
		
		try {
			testDriver(testDriver, MockMapStub.class);
		} catch (Exception e) {
			LOG.debug(e);
			Assert.fail("Invoke method caused exception.");
		}
		
		Assert.assertEquals("Wrong result set size.", keyCnt*valCnt, this.output.getNumberOfRecords());
	}
	
	@Test
	public void testFailingMapTask() {
		final int keyCnt = 100;
		final int valCnt = 20;
		
		addInput(new UniformRecordGenerator(keyCnt, valCnt, false));
		setOutput(new DiscardingOutputCollector());
		
		final MapDriver<Record, Record> testTask = new MapDriver<Record, Record>();
		try {
			testDriver(testTask, MockFailingMapStub.class);
			Assert.fail("Function exception was not forwarded.");
		} catch (ExpectedTestException e) {
			// good!
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("Exception in test.");
		}
	}
	
	@Test
	public void testCancelMapTask() {
		addInput(new InfiniteInputIterator());
		setOutput(new DiscardingOutputCollector());
		
		final MapDriver<Record, Record> testTask = new MapDriver<Record, Record>();
		
		final AtomicBoolean success = new AtomicBoolean(false);
		
		final Thread taskRunner = new Thread() {
			@Override
			public void run() {
				try {
					testDriver(testTask, MockMapStub.class);
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
		
		Assert.assertTrue("Test threw an exception even though it was properly canceled.", success.get());
	}
	
	public static class MockMapStub extends MapFunction {
		@Override
		public void map(Record record, Collector<Record> out) throws Exception {
			out.collect(record);
		}
		
	}
	
	public static class MockFailingMapStub extends MapFunction {

		private int cnt = 0;
		
		@Override
		public void map(Record record, Collector<Record> out) throws Exception {
			if (++this.cnt >= 10) {
				throw new ExpectedTestException();
			}
			out.collect(record);
		}
	}
}
