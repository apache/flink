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

package org.apache.flink.streaming.api.operators.windowing;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.windowing.GroupedStreamDiscretizer;
import org.apache.flink.streaming.api.operators.windowing.GroupedWindowBuffer;
import org.apache.flink.streaming.api.operators.windowing.StreamWindowBuffer;
import org.apache.flink.streaming.api.windowing.StreamWindow;
import org.apache.flink.streaming.api.windowing.WindowEvent;
import org.apache.flink.streaming.api.windowing.policy.CloneableEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.CloneableTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.CountTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.TumblingEvictionPolicy;
import org.apache.flink.streaming.api.windowing.windowbuffer.BasicWindowBuffer;
import org.apache.flink.streaming.util.MockContext;
import org.junit.Test;

public class GroupedStreamDiscretizerTest {

	KeySelector<Tuple2<Integer, String>, ?> keySelector = new KeySelector<Tuple2<Integer, String>, String>() {

		private static final long serialVersionUID = 1L;

		@Override
		public String getKey(Tuple2<Integer, String> value) throws Exception {
			return value.f1;
		}
	};

	/**
	 * Test for not active distributed triggers with single field
	 */
	@Test
	public void groupedDiscretizerTest() {

		List<Integer> inputs = new ArrayList<Integer>();
		inputs.add(1);
		inputs.add(2);
		inputs.add(2);
		inputs.add(3);
		inputs.add(4);
		inputs.add(5);
		inputs.add(10);
		inputs.add(11);
		inputs.add(11);

		Set<StreamWindow<Integer>> expected = new HashSet<StreamWindow<Integer>>();
		expected.add(StreamWindow.fromElements(2, 2));
		expected.add(StreamWindow.fromElements(1, 3));
		expected.add(StreamWindow.fromElements(5, 11));
		expected.add(StreamWindow.fromElements(4, 10));
		expected.add(StreamWindow.fromElements(11));

		KeySelector<Integer, Integer> keySelector = new KeySelector<Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer getKey(Integer value) {
				return value % 2;
			}
		};

		CloneableTriggerPolicy<Integer> trigger = new CountTriggerPolicy<Integer>(2);
		CloneableEvictionPolicy<Integer> eviction = new TumblingEvictionPolicy<Integer>();

		GroupedStreamDiscretizer<Integer> discretizer = new GroupedStreamDiscretizer<Integer>(
				keySelector, trigger, eviction);

		StreamWindowBuffer<Integer> buffer = new GroupedWindowBuffer<Integer>(
				new BasicWindowBuffer<Integer>(), keySelector);

		List<WindowEvent<Integer>> bufferEvents = MockContext.createAndExecute(discretizer,
				inputs);
		List<StreamWindow<Integer>> result = MockContext.createAndExecute(buffer, bufferEvents);

		assertEquals(expected, new HashSet<StreamWindow<Integer>>(result));

	}

}
