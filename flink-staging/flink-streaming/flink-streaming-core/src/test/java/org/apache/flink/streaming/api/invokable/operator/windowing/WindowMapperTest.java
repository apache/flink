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

import org.apache.flink.streaming.api.function.WindowMapFunction;
import org.apache.flink.streaming.api.invokable.StreamInvokable;
import org.apache.flink.streaming.api.windowing.StreamWindow;
import org.apache.flink.streaming.util.MockContext;
import org.apache.flink.util.Collector;
import org.junit.Test;

public class WindowMapperTest {

	@Test
	public void test() {
		StreamInvokable<StreamWindow<Integer>, StreamWindow<Integer>> windowMapper = new WindowMapper<Integer, Integer>(
				new WindowMapFunction<Integer, Integer>() {

					private static final long serialVersionUID = 1L;

					@Override
					public void mapWindow(Iterable<Integer> values, Collector<Integer> out)
							throws Exception {
						for (Integer v : values) {
							out.collect(v);
						}
					}
				});

		List<StreamWindow<Integer>> input = new ArrayList<StreamWindow<Integer>>();
		input.add(StreamWindow.fromElements(1, 2, 3));
		input.add(new StreamWindow<Integer>());

		List<StreamWindow<Integer>> output = MockContext.createAndExecute(windowMapper, input);

		assertEquals(input, output);

	}

}
