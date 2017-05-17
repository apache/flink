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

package org.apache.flink.streaming.api.collector;

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link OutputSelector}.
 */
public class OutputSelectorTest {

	static final class MyOutputSelector implements OutputSelector<Tuple1<Integer>> {

		private static final long serialVersionUID = 1L;

		@Override
		public Iterable<String> select(Tuple1<Integer> tuple) {

			String[] outputs = new String[tuple.f0];

			for (Integer i = 0; i < tuple.f0; i++) {
				outputs[i] = i.toString();
			}
			return Arrays.asList(outputs);
		}
	}

	@Test
	public void testGetOutputs() {
		OutputSelector<Tuple1<Integer>> selector = new MyOutputSelector();
		List<String> expectedOutputs = new ArrayList<String>();
		expectedOutputs.add("0");
		expectedOutputs.add("1");
		assertEquals(expectedOutputs, selector.select(new Tuple1<Integer>(2)));
		expectedOutputs.add("2");
		assertEquals(expectedOutputs, selector.select(new Tuple1<Integer>(3)));
	}

}
