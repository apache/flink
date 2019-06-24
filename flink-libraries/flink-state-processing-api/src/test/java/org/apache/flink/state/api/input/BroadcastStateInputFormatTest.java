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

package org.apache.flink.state.api.input;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.state.api.input.splits.OperatorStateInputSplit;
import org.apache.flink.state.api.runtime.OperatorIDGenerator;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.operators.co.CoBroadcastWithNonKeyedOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;
import org.apache.flink.streaming.util.TwoInputStreamOperatorTestHarness;
import org.apache.flink.util.Collector;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Test for operator broadcast state input format.
 */
public class BroadcastStateInputFormatTest {
	private static MapStateDescriptor<Integer, Integer> descriptor = new MapStateDescriptor<>("state", Types.INT, Types.INT);

	@Test
	public void testReadBroadcastState() throws Exception {
		try (TwoInputStreamOperatorTestHarness<Void, Integer, Void> testHarness = getTestHarness()) {
			testHarness.open();

			testHarness.processElement2(new StreamRecord<>(1));
			testHarness.processElement2(new StreamRecord<>(2));
			testHarness.processElement2(new StreamRecord<>(3));

			OperatorSubtaskState subtaskState = testHarness.snapshot(0, 0);
			OperatorState state = new OperatorState(OperatorIDGenerator.fromUid("uid"), 1, 4);
			state.putState(0, subtaskState);

			OperatorStateInputSplit split = new OperatorStateInputSplit(subtaskState.getManagedOperatorState(), 0);

			BroadcastStateInputFormat<Integer, Integer> format = new BroadcastStateInputFormat<>(state, descriptor);

			format.setRuntimeContext(new MockStreamingRuntimeContext(false, 1, 0));
			format.open(split);

			Map<Integer, Integer> results = new HashMap<>(3);

			while (!format.reachedEnd()) {
				Tuple2<Integer, Integer> entry = format.nextRecord(null);
				results.put(entry.f0, entry.f1);
			}

			Map<Integer, Integer> expected = new HashMap<>(3);
			expected.put(1, 1);
			expected.put(2, 2);
			expected.put(3, 3);

			Assert.assertEquals("Failed to read correct list state from state backend", expected, results);
		}
	}

	private TwoInputStreamOperatorTestHarness<Void, Integer, Void> getTestHarness() throws Exception {
		return new TwoInputStreamOperatorTestHarness<>(
			new CoBroadcastWithNonKeyedOperator<>(
				new StatefulFunction(), Collections.singletonList(descriptor)));
	}

	static class StatefulFunction extends BroadcastProcessFunction<Void, Integer, Void> {

		@Override
		public void processElement(Void value, ReadOnlyContext ctx, Collector<Void> out) {}

		@Override
		public void processBroadcastElement(Integer value, Context ctx, Collector<Void> out) throws Exception {
			ctx.getBroadcastState(descriptor).put(value, value);
		}
	}
}

