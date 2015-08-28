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

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.windowing.StreamWindow;
import org.apache.flink.streaming.util.MockContext;
import org.junit.Test;

public class WindowFolderTest {

	@Test
	public void test() {
		OneInputStreamOperator<StreamWindow<Integer>, StreamWindow<String>> windowReducer = new WindowFolder<Integer,String>(
				new FoldFunction<Integer, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public String fold(String accumulator, Integer value) throws Exception {
						return accumulator + value.toString();
					}
				}, "");

		List<StreamWindow<Integer>> input = new ArrayList<StreamWindow<Integer>>();
		input.add(StreamWindow.fromElements(1, 2, 3));
		input.add(new StreamWindow<Integer>());
		input.add(StreamWindow.fromElements(-1));

		List<StreamWindow<String>> expected = new ArrayList<StreamWindow<String>>();
		expected.add(StreamWindow.fromElements("123"));
		expected.add(new StreamWindow<String>());
		expected.add(StreamWindow.fromElements("-1"));

		List<StreamWindow<String>> output = MockContext.createAndExecute(windowReducer, input);

		assertEquals(expected, output);

	}
}
