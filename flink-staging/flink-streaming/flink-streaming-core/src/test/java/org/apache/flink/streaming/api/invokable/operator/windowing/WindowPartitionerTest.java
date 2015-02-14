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

package org.apache.flink.streaming.api.invokable.operator.windowing;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.invokable.StreamInvokable;
import org.apache.flink.streaming.api.windowing.StreamWindow;
import org.apache.flink.streaming.util.MockContext;
import org.junit.Test;

public class WindowPartitionerTest {

	@Test
	public void test() throws Exception {
		StreamInvokable<StreamWindow<Integer>, StreamWindow<Integer>> splitPartitioner = new WindowPartitioner<Integer>(
				2);

		StreamInvokable<StreamWindow<Integer>, StreamWindow<Integer>> gbPartitioner = new WindowPartitioner<Integer>(
				new MyKey());

		StreamWindow<Integer> w1 = new StreamWindow<Integer>();
		StreamWindow<Integer> w2 = StreamWindow.fromElements(1, 2, 3, 4);

		List<StreamWindow<Integer>> expected1 = new ArrayList<StreamWindow<Integer>>();
		expected1.addAll(w1.split(2));
		expected1.addAll(w2.split(2));

		List<StreamWindow<Integer>> expected2 = new ArrayList<StreamWindow<Integer>>();
		expected2.addAll(w1.partitionBy(new MyKey()));
		expected2.addAll(w2.partitionBy(new MyKey()));

		List<StreamWindow<Integer>> input = new ArrayList<StreamWindow<Integer>>();
		input.add(w1);
		input.add(w2);

		List<StreamWindow<Integer>> output1 = MockContext.createAndExecute(splitPartitioner, input);
		List<StreamWindow<Integer>> output2 = MockContext.createAndExecute(gbPartitioner, input);

		assertEquals(expected1, output1);
		assertEquals(expected2, output2);

	}

	private static class MyKey implements KeySelector<Integer, Object> {

		private static final long serialVersionUID = 1L;

		@Override
		public Object getKey(Integer value) throws Exception {
			return value / 2;
		}

	}

}
