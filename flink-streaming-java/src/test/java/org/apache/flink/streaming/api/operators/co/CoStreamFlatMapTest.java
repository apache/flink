/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators.co;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.streaming.util.TwoInputStreamOperatorTestHarness;
import org.apache.flink.util.Collector;

import org.junit.Assert;
import org.junit.Test;

import java.io.Serializable;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Tests for {@link CoStreamFlatMap}. These test that:
 *
 * <ul>
 *     <li>RichFunction methods are called correctly</li>
 *     <li>Timestamps of processed elements match the input timestamp</li>
 *     <li>Watermarks are correctly forwarded</li>
 * </ul>
 */
public class CoStreamFlatMapTest implements Serializable {
	private static final long serialVersionUID = 1L;

	private static final class MyCoFlatMap implements CoFlatMapFunction<String, Integer, String> {
		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap1(String value, Collector<String> coll) {
			for (int i = 0; i < value.length(); i++) {
				coll.collect(value.substring(i, i + 1));
			}
		}

		@Override
		public void flatMap2(Integer value, Collector<String> coll) {
			coll.collect(value.toString());
		}
	}

	@Test
	public void testCoFlatMap() throws Exception {
		CoStreamFlatMap<String, Integer, String> operator = new CoStreamFlatMap<String, Integer, String>(new MyCoFlatMap());

		TwoInputStreamOperatorTestHarness<String, Integer, String> testHarness = new TwoInputStreamOperatorTestHarness<String, Integer, String>(operator);

		long initialTime = 0L;
		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<Object>();

		testHarness.open();

		testHarness.processElement1(new StreamRecord<String>("abc", initialTime + 1));
		testHarness.processElement1(new StreamRecord<String>("def", initialTime + 2));
		testHarness.processWatermark1(new Watermark(initialTime + 2));
		testHarness.processElement1(new StreamRecord<String>("ghi", initialTime + 3));

		testHarness.processElement2(new StreamRecord<Integer>(1, initialTime + 1));
		testHarness.processElement2(new StreamRecord<Integer>(2, initialTime + 2));
		testHarness.processWatermark2(new Watermark(initialTime + 3));
		testHarness.processElement2(new StreamRecord<Integer>(3, initialTime + 3));
		testHarness.processElement2(new StreamRecord<Integer>(4, initialTime + 4));
		testHarness.processElement2(new StreamRecord<Integer>(5, initialTime + 5));

		expectedOutput.add(new StreamRecord<String>("a", initialTime + 1));
		expectedOutput.add(new StreamRecord<String>("b", initialTime + 1));
		expectedOutput.add(new StreamRecord<String>("c", initialTime + 1));
		expectedOutput.add(new StreamRecord<String>("d", initialTime + 2));
		expectedOutput.add(new StreamRecord<String>("e", initialTime + 2));
		expectedOutput.add(new StreamRecord<String>("f", initialTime + 2));
		expectedOutput.add(new StreamRecord<String>("g", initialTime + 3));
		expectedOutput.add(new StreamRecord<String>("h", initialTime + 3));
		expectedOutput.add(new StreamRecord<String>("i", initialTime + 3));

		expectedOutput.add(new StreamRecord<String>("1", initialTime + 1));
		expectedOutput.add(new StreamRecord<String>("2", initialTime + 2));
		expectedOutput.add(new Watermark(initialTime + 2));
		expectedOutput.add(new StreamRecord<String>("3", initialTime + 3));
		expectedOutput.add(new StreamRecord<String>("4", initialTime + 4));
		expectedOutput.add(new StreamRecord<String>("5", initialTime + 5));

		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());
	}

	@Test
	public void testOpenClose() throws Exception {
		CoStreamFlatMap<String, Integer, String> operator = new CoStreamFlatMap<String, Integer, String>(new TestOpenCloseCoFlatMapFunction());

		TwoInputStreamOperatorTestHarness<String, Integer, String> testHarness = new TwoInputStreamOperatorTestHarness<String, Integer, String>(operator);

		long initialTime = 0L;

		testHarness.open();

		testHarness.processElement1(new StreamRecord<String>("Hello", initialTime));
		testHarness.processElement2(new StreamRecord<Integer>(42, initialTime));

		testHarness.close();

		Assert.assertTrue("RichFunction methods where not called.", TestOpenCloseCoFlatMapFunction.closeCalled);
		Assert.assertTrue("Output contains no elements.", testHarness.getOutput().size() > 0);
	}

	// This must only be used in one test, otherwise the static fields will be changed
	// by several tests concurrently
	private static class TestOpenCloseCoFlatMapFunction extends RichCoFlatMapFunction<String, Integer, String> {
		private static final long serialVersionUID = 1L;

		public static boolean openCalled = false;
		public static boolean closeCalled = false;

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			if (closeCalled) {
				Assert.fail("Close called before open.");
			}
			openCalled = true;
		}

		@Override
		public void close() throws Exception {
			super.close();
			if (!openCalled) {
				Assert.fail("Open was not called before close.");
			}
			closeCalled = true;
		}

		@Override
		public void flatMap1(String value, Collector<String> out) throws Exception {
			if (!openCalled) {
				Assert.fail("Open was not called before run.");
			}
			out.collect(value);
		}

		@Override
		public void flatMap2(Integer value, Collector<String> out) throws Exception {
			if (!openCalled) {
				Assert.fail("Open was not called before run.");
			}
			out.collect(value.toString());
		}

	}
}
