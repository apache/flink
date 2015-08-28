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

import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.windowing.StreamWindow;
import org.apache.flink.streaming.util.MockContext;
import org.junit.Test;

public class WindowFlattenerTest {

	@Test
	public void test() {
		OneInputStreamOperator<StreamWindow<Integer>, Integer> flattener = new WindowFlattener<Integer>();

		StreamWindow<Integer> w1 = StreamWindow.fromElements(1, 2, 3);
		StreamWindow<Integer> w2 = new StreamWindow<Integer>();

		List<StreamWindow<Integer>> input = new ArrayList<StreamWindow<Integer>>();
		input.add(w1);
		input.add(w2);

		List<Integer> expected = new ArrayList<Integer>();
		expected.addAll(w1);
		expected.addAll(w2);

		List<Integer> output = MockContext.createAndExecute(flattener, input);

		assertEquals(expected, output);

	}

}
