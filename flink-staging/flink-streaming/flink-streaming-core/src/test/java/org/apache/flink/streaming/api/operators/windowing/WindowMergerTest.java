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

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.windowing.StreamWindow;
import org.apache.flink.streaming.util.MockContext;
import org.junit.Test;

public class WindowMergerTest {

	@Test
	public void test() throws Exception {
		OneInputStreamOperator<StreamWindow<Integer>, StreamWindow<Integer>> windowMerger = new WindowMerger<Integer>();

		StreamWindow<Integer> w1 = new StreamWindow<Integer>();
		StreamWindow<Integer> w2 = StreamWindow.fromElements(1, 2, 3, 4);
		StreamWindow<Integer> w3 = StreamWindow.fromElements(-1, 2, 3, 4);
		StreamWindow<Integer> w4_1 = new StreamWindow<Integer>(1, 2);
		StreamWindow<Integer> w4_2 = new StreamWindow<Integer>(1, 2);
		w4_1.add(1);
		w4_2.add(2);

		List<StreamWindow<Integer>> expected = new ArrayList<StreamWindow<Integer>>();
		expected.add(w1);
		expected.add(w2);
		expected.add(w3);
		expected.add(StreamWindow.fromElements(1, 2));

		List<StreamWindow<Integer>> input = new ArrayList<StreamWindow<Integer>>();
		input.add(w1);
		input.add(w4_1);
		input.addAll(StreamWindow.split(w2, 2));
		input.addAll(StreamWindow.partitionBy(w3, new KeySelector<Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer getKey(Integer value) throws Exception {
				return value % 2;
			}
		}, false));
		input.add(w4_2);

		List<StreamWindow<Integer>> output = MockContext.createAndExecute(windowMerger, input);

		assertEquals(expected.size(), expected.size());
		for (int i = 0; i < output.size(); i++) {
			assertEquals(new HashSet<Integer>(expected.get(i)), new HashSet<Integer>(output.get(i)));
		}

	}

}
