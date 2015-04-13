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
import java.util.List;

import org.apache.flink.streaming.api.operators.windowing.StreamDiscretizer;
import org.apache.flink.streaming.api.operators.windowing.StreamWindowBuffer;
import org.apache.flink.streaming.api.windowing.StreamWindow;
import org.apache.flink.streaming.api.windowing.WindowEvent;
import org.apache.flink.streaming.api.windowing.helper.Timestamp;
import org.apache.flink.streaming.api.windowing.helper.TimestampWrapper;
import org.apache.flink.streaming.api.windowing.policy.CountTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.EvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.TimeEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.TimeTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.TriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.TumblingEvictionPolicy;
import org.apache.flink.streaming.api.windowing.windowbuffer.BasicWindowBuffer;
import org.apache.flink.streaming.util.MockContext;
import org.junit.Test;

public class StreamDiscretizerTest {

	
	@Test
	public void testDiscretizer() {

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

		List<StreamWindow<Integer>> expected = new ArrayList<StreamWindow<Integer>>();
		expected.add(StreamWindow.fromElements(1, 2, 2, 3, 4));
		expected.add(StreamWindow.fromElements(3, 4, 5));
		expected.add(StreamWindow.fromElements(5));
		expected.add(StreamWindow.fromElements(10));
		expected.add(StreamWindow.fromElements(10, 11, 11));

		Timestamp<Integer> myTimeStamp = new Timestamp<Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public long getTimestamp(Integer value) {
				return value;
			}
		};

		TriggerPolicy<Integer> trigger = new TimeTriggerPolicy<Integer>(2L,
				new TimestampWrapper<Integer>(myTimeStamp, 3));

		EvictionPolicy<Integer> eviction = new TimeEvictionPolicy<Integer>(4L,
				new TimestampWrapper<Integer>(myTimeStamp, 1));
		
		

		StreamDiscretizer<Integer> discretizer = new StreamDiscretizer<Integer>(trigger, eviction);
		StreamWindowBuffer<Integer> buffer = new StreamWindowBuffer<Integer>(new BasicWindowBuffer<Integer>());

		List<WindowEvent<Integer>> bufferEvents = MockContext.createAndExecute(discretizer, inputs);
		List<StreamWindow<Integer>> result = MockContext.createAndExecute(buffer, bufferEvents);
		
		assertEquals(expected, result);
	}

	@Test
	public void testDiscretizer2() {

		List<Integer> inputs = new ArrayList<Integer>();
		inputs.add(1);
		inputs.add(2);
		inputs.add(2);
		inputs.add(3);
		inputs.add(4);

		List<StreamWindow<Integer>> expected = new ArrayList<StreamWindow<Integer>>();
		expected.add(StreamWindow.fromElements(1, 2));
		expected.add(StreamWindow.fromElements(2, 3));
		expected.add(StreamWindow.fromElements(4));

		TriggerPolicy<Integer> trigger = new CountTriggerPolicy<Integer>(2);

		EvictionPolicy<Integer> eviction = new TumblingEvictionPolicy<Integer>();

		StreamDiscretizer<Integer> discretizer = new StreamDiscretizer<Integer>(trigger, eviction);
		StreamWindowBuffer<Integer> buffer = new StreamWindowBuffer<Integer>(new BasicWindowBuffer<Integer>());

		List<WindowEvent<Integer>> bufferEvents = MockContext.createAndExecute(discretizer, inputs);
		List<StreamWindow<Integer>> result = MockContext.createAndExecute(buffer, bufferEvents);
		assertEquals(expected, result);
	}
}
