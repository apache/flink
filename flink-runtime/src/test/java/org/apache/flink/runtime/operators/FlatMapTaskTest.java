/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.flink.runtime.operators;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.runtime.operators.testutils.DiscardingOutputCollector;
import org.apache.flink.runtime.operators.testutils.DriverTestBase;
import org.apache.flink.runtime.operators.testutils.ExpectedTestException;
import org.apache.flink.runtime.operators.testutils.InfiniteInputIterator;
import org.apache.flink.runtime.operators.testutils.TaskCancelThread;
import org.apache.flink.runtime.operators.testutils.UniformRecordGenerator;
import org.apache.flink.types.Record;
import org.apache.flink.util.Collector;
import org.junit.Assert;
import org.junit.Test;

public class FlatMapTaskTest extends DriverTestBase<FlatMapFunction<Record, Record>> {
	
	private static final Logger LOG = LoggerFactory.getLogger(FlatMapTaskTest.class);
	
	private final CountingOutputCollector output = new CountingOutputCollector();
	
	
	public FlatMapTaskTest(ExecutionConfig config) {
		super(config, 0, 0);
	}
	
	@Test
	public void testMapTask() {
		final int keyCnt = 100;
		final int valCnt = 20;
		
		addInput(new UniformRecordGenerator(keyCnt, valCnt, false));
		setOutput(this.output);
		
		final FlatMapDriver<Record, Record> testDriver = new FlatMapDriver<>();
		
		try {
			testDriver(testDriver, MockMapStub.class);
		} catch (Exception e) {
			LOG.debug("Exception while running the test driver.", e);
			Assert.fail("Invoke method caused exception.");
		}
		
		Assert.assertEquals("Wrong result set size.", keyCnt*valCnt, this.output.getNumberOfRecords());
	}
	
	@Test
	public void testFailingMapTask() {
		final int keyCnt = 100;
		final int valCnt = 20;
		
		addInput(new UniformRecordGenerator(keyCnt, valCnt, false));
		setOutput(new DiscardingOutputCollector<Record>());
		
		final FlatMapDriver<Record, Record> testTask = new FlatMapDriver<>();
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
		setOutput(new DiscardingOutputCollector<Record>());
		
		final FlatMapDriver<Record, Record> testTask = new FlatMapDriver<>();
		
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
	
	public static class MockMapStub extends RichFlatMapFunction<Record, Record> {
		private static final long serialVersionUID = 1L;
		
		@Override
		public void flatMap(Record record, Collector<Record> out) throws Exception {
			out.collect(record);
		}
		
	}
	
	public static class MockFailingMapStub extends RichFlatMapFunction<Record, Record> {
		private static final long serialVersionUID = 1L;
		
		private int cnt = 0;
		
		@Override
		public void flatMap(Record record, Collector<Record> out) throws Exception {
			if (++this.cnt >= 10) {
				throw new ExpectedTestException();
			}
			out.collect(record);
		}
	}
}
