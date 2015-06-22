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

package org.apache.flink.streaming.api.operators;

import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.RichFoldFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link StreamGroupedFold}. These test that:
 *
 * <ul>
 *     <li>RichFunction methods are called correctly</li>
 *     <li>Timestamps of processed elements match the input timestamp</li>
 *     <li>Watermarks are correctly forwarded</li>
 * </ul>
 */
public class StreamGroupedFoldTest {

	private static class MyFolder implements FoldFunction<Integer, String> {

		private static final long serialVersionUID = 1L;

		@Override
		public String fold(String accumulator, Integer value) throws Exception {
			return accumulator + value.toString();
		}

	}

	@Test
	@SuppressWarnings("unchecked")
	public void testGroupedFold() throws Exception {
		TypeInformation<String> outType = TypeExtractor.getForObject("A string");

		StreamGroupedFold<Integer, String> operator = new StreamGroupedFold<Integer, String>(
				new MyFolder(), new KeySelector<Integer, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public String getKey(Integer value) throws Exception {
				return value.toString();
			}
		}, "100", outType);

		OneInputStreamOperatorTestHarness<Integer, String> testHarness = new OneInputStreamOperatorTestHarness<Integer, String>(operator);

		long initialTime = 0L;
		ConcurrentLinkedQueue expectedOutput = new ConcurrentLinkedQueue();

		testHarness.open();

		testHarness.processElement(new StreamRecord<Integer>(1, initialTime + 1));
		testHarness.processElement(new StreamRecord<Integer>(1, initialTime + 2));
		testHarness.processWatermark(new Watermark(initialTime + 2));
		testHarness.processElement(new StreamRecord<Integer>(2, initialTime + 3));
		testHarness.processElement(new StreamRecord<Integer>(2, initialTime + 4));
		testHarness.processElement(new StreamRecord<Integer>(3, initialTime + 5));

		expectedOutput.add(new StreamRecord<String>("1001", initialTime + 1));
		expectedOutput.add(new StreamRecord<String>("10011", initialTime + 2));
		expectedOutput.add(new Watermark(initialTime + 2));
		expectedOutput.add(new StreamRecord<String>("1002", initialTime + 3));
		expectedOutput.add(new StreamRecord<String>("10022", initialTime + 4));
		expectedOutput.add(new StreamRecord<String>("1003", initialTime + 5));

		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());
	}

	@Test
	public void testOpenClose() throws Exception {
		StreamGroupedFold<Integer, String> operator = new StreamGroupedFold<Integer, String>(new TestOpenCloseFoldFunction(), new KeySelector<Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer getKey(Integer value) throws Exception {
				return value;
			}
		}, "init", BasicTypeInfo.STRING_TYPE_INFO);
		OneInputStreamOperatorTestHarness<Integer, String> testHarness = new OneInputStreamOperatorTestHarness<Integer, String>(operator);

		long initialTime = 0L;

		testHarness.open();

		testHarness.processElement(new StreamRecord<Integer>(1, initialTime));
		testHarness.processElement(new StreamRecord<Integer>(2, initialTime));

		testHarness.close();

		Assert.assertTrue("RichFunction methods where not called.", TestOpenCloseFoldFunction.closeCalled);
		Assert.assertTrue("Output contains no elements.", testHarness.getOutput().size() > 0);
	}

	// This must only be used in one test, otherwise the static fields will be changed
	// by several tests concurrently
	private static class TestOpenCloseFoldFunction extends RichFoldFunction<Integer, String> {
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
		public String fold(String acc, Integer in) throws Exception {
			if (!openCalled) {
				Assert.fail("Open was not called before run.");
			}
			return acc + in;
		}
	}
}
