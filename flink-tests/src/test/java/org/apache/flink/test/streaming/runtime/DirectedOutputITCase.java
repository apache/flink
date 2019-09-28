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

package org.apache.flink.test.streaming.runtime;

import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.streaming.runtime.util.TestListResultSink;
import org.apache.flink.test.util.AbstractTestBase;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Integration tests for a streaming {@link OutputSelector}.
 */
public class DirectedOutputITCase extends AbstractTestBase {

	private static final String TEN = "ten";
	private static final String ODD = "odd";
	private static final String EVEN = "even";
	private static final String NON_SELECTED = "nonSelected";

	static final class MyOutputSelector implements OutputSelector<Long> {
		private static final long serialVersionUID = 1L;

		List<String> outputs = new ArrayList<String>();

		@Override
		public Iterable<String> select(Long value) {
			outputs.clear();
			if (value % 2 == 0) {
				outputs.add(EVEN);
			} else {
				outputs.add(ODD);
			}

			if (value == 10L) {
				outputs.add(TEN);
			}

			if (value == 11L) {
				outputs.add(NON_SELECTED);
			}
			return outputs;
		}
	}

	@Test
	public void outputSelectorTest() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(3);

		TestListResultSink<Long> evenSink = new TestListResultSink<Long>();
		TestListResultSink<Long> oddAndTenSink = new TestListResultSink<Long>();
		TestListResultSink<Long> evenAndOddSink = new TestListResultSink<Long>();
		TestListResultSink<Long> allSink = new TestListResultSink<Long>();

		SplitStream<Long> source = env.generateSequence(1, 11).split(new MyOutputSelector());
		source.select(EVEN).addSink(evenSink);
		source.select(ODD, TEN).addSink(oddAndTenSink);
		source.select(EVEN, ODD).addSink(evenAndOddSink);
		source.addSink(allSink);

		env.execute();
		assertEquals(Arrays.asList(2L, 4L, 6L, 8L, 10L), evenSink.getSortedResult());
		assertEquals(Arrays.asList(1L, 3L, 5L, 7L, 9L, 10L, 11L), oddAndTenSink.getSortedResult());
		assertEquals(Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L),
				evenAndOddSink.getSortedResult());
		assertEquals(Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L),
				allSink.getSortedResult());
	}
}
