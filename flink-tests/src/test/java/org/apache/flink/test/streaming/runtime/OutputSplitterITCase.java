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
import org.apache.flink.streaming.api.datastream.DataStream;
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
public class OutputSplitterITCase extends AbstractTestBase {

	private static ArrayList<Integer> expectedSplitterResult = new ArrayList<Integer>();

	@SuppressWarnings("unchecked")
	@Test
	public void testOnMergedDataStream() throws Exception {
		TestListResultSink<Integer> splitterResultSink1 = new TestListResultSink<Integer>();
		TestListResultSink<Integer> splitterResultSink2 = new TestListResultSink<Integer>();

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.setBufferTimeout(1);

		DataStream<Integer> d1 = env.fromElements(0, 2, 4, 6, 8);
		DataStream<Integer> d2 = env.fromElements(1, 3, 5, 7, 9);

		d1 = d1.union(d2);

		d1.split(new OutputSelector<Integer>() {
			private static final long serialVersionUID = 8354166915727490130L;

			@Override
			public Iterable<String> select(Integer value) {
				List<String> s = new ArrayList<String>();
				if (value > 4) {
					s.add(">");
				} else {
					s.add("<");
				}
				return s;
			}
		}).select(">").addSink(splitterResultSink1);

		d1.split(new OutputSelector<Integer>() {
			private static final long serialVersionUID = -6822487543355994807L;

			@Override
			public Iterable<String> select(Integer value) {
				List<String> s = new ArrayList<String>();
				if (value % 3 == 0) {
					s.add("yes");
				} else {
					s.add("no");
				}
				return s;
			}
		}).select("yes").addSink(splitterResultSink2);
		env.execute();

		expectedSplitterResult.clear();
		expectedSplitterResult.addAll(Arrays.asList(5, 6, 7, 8, 9));
		assertEquals(expectedSplitterResult, splitterResultSink1.getSortedResult());

		expectedSplitterResult.clear();
		expectedSplitterResult.addAll(Arrays.asList(0, 3, 6, 9));
		assertEquals(expectedSplitterResult, splitterResultSink2.getSortedResult());
	}

	@Test
	public void testOnSingleDataStream() throws Exception {
		TestListResultSink<Integer> splitterResultSink1 = new TestListResultSink<Integer>();
		TestListResultSink<Integer> splitterResultSink2 = new TestListResultSink<Integer>();

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.setBufferTimeout(1);

		DataStream<Integer> ds = env.fromElements(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

		ds.split(new OutputSelector<Integer>() {
			private static final long serialVersionUID = 2524335410904414121L;

			@Override
			public Iterable<String> select(Integer value) {
				List<String> s = new ArrayList<String>();
				if (value % 2 == 0) {
					s.add("even");
				} else {
					s.add("odd");
				}
				return s;
			}
		}).select("even").addSink(splitterResultSink1);

		ds.split(new OutputSelector<Integer>() {

			private static final long serialVersionUID = -511693919586034092L;

			@Override
			public Iterable<String> select(Integer value) {
				List<String> s = new ArrayList<String>();
				if (value % 4 == 0) {
					s.add("yes");
				} else {
					s.add("no");
				}
				return s;
			}
		}).select("yes").addSink(splitterResultSink2);
		env.execute();

		expectedSplitterResult.clear();
		expectedSplitterResult.addAll(Arrays.asList(0, 2, 4, 6, 8));
		assertEquals(expectedSplitterResult, splitterResultSink1.getSortedResult());

		expectedSplitterResult.clear();
		expectedSplitterResult.addAll(Arrays.asList(0, 4, 8));
		assertEquals(expectedSplitterResult, splitterResultSink2.getSortedResult());
	}
}
