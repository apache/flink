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

package org.apache.flink.streaming.api.datastream;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

/**
 * Tests that verify correct behavior when applying split/getSideOutput operations on one {@link DataStream}.
 */
public class SplitSideOutputTest {

	private static final OutputTag<String> outputTag = new OutputTag<String>("outputTag") {};

	@Test
	public void testSideOutputAfterSelectIsForbidden() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		SingleOutputStreamOperator<String> processInput = env.fromElements("foo")
			.process(new DummyProcessFunction());

		processInput.split(Collections::singleton);

		try {
			processInput.getSideOutput(outputTag);
			Assert.fail("Should have failed early with an exception.");
		} catch (UnsupportedOperationException expected){
			// expected
		}
	}

	@Test
	public void testSelectAfterSideOutputIsForbidden() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		SingleOutputStreamOperator<String> processInput = env.fromElements("foo")
			.process(new DummyProcessFunction());

		processInput.getSideOutput(outputTag);

		try {
			processInput.split(Collections::singleton);
			Assert.fail("Should have failed early with an exception.");
		} catch (UnsupportedOperationException expected){
			// expected
		}
	}

	private static final class DummyProcessFunction extends ProcessFunction<String, String> {

		@Override
		public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
		}
	}
}
