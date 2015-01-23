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

package org.apache.flink.streaming.api;

import static org.junit.Assert.assertEquals;

import org.apache.flink.streaming.api.collector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.sink.SinkFunction;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class OutputSplitterTest {

	private static final long MEMORYSIZE = 32;

	private static ArrayList<Integer> splitterResult1 = new ArrayList<Integer>();
	private static ArrayList<Integer> splitterResult2 = new ArrayList<Integer>();


	private static ArrayList<Integer> expectedSplitterResult = new ArrayList<Integer>();

	@SuppressWarnings("unchecked")
	@Test
	public void testOnMergedDataStream() throws Exception {
		splitterResult1.clear();
		splitterResult2.clear();

		StreamExecutionEnvironment env = new TestStreamEnvironment(1, MEMORYSIZE);
		env.setBufferTimeout(1);

		DataStream<Integer> d1 = env.fromElements(0,2,4,6,8);
		DataStream<Integer> d2 = env.fromElements(1,3,5,7,9);

		d1 = d1.merge(d2);

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
		}).select(">").addSink(new SinkFunction<Integer>() {

			private static final long serialVersionUID = 5827187510526388104L;

			@Override
			public void invoke(Integer value) {
				splitterResult1.add(value);
			}
		});

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
		}).select("yes").addSink(new SinkFunction<Integer>() {
			private static final long serialVersionUID = -2674335071267854599L;

			@Override
			public void invoke(Integer value) {
				splitterResult2.add(value);
			}
		});
		env.execute();

		Collections.sort(splitterResult1);
		Collections.sort(splitterResult2);

		expectedSplitterResult.clear();
		expectedSplitterResult.addAll(Arrays.asList(5,6,7,8,9));
		assertEquals(expectedSplitterResult, splitterResult1);

		expectedSplitterResult.clear();
		expectedSplitterResult.addAll(Arrays.asList(0,3,6,9));
		assertEquals(expectedSplitterResult, splitterResult2);
	}

	@Test
	public void testOnSingleDataStream() throws Exception {
		splitterResult1.clear();
		splitterResult2.clear();

		StreamExecutionEnvironment env = new TestStreamEnvironment(1, MEMORYSIZE);
		env.setBufferTimeout(1);

		DataStream<Integer> ds = env.fromElements(0,1,2,3,4,5,6,7,8,9);

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
		}).select("even").addSink(new SinkFunction<Integer>() {

			private static final long serialVersionUID = -2995092337537209535L;

			@Override
			public void invoke(Integer value) {
				splitterResult1.add(value);
			}
		});

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
		}).select("yes").addSink(new SinkFunction<Integer>() {

			private static final long serialVersionUID = -1749077049727705424L;

			@Override
			public void invoke(Integer value) {
				splitterResult2.add(value);
			}
		});
		env.execute();

		Collections.sort(splitterResult1);
		Collections.sort(splitterResult2);

		expectedSplitterResult.clear();
		expectedSplitterResult.addAll(Arrays.asList(0,2,4,6,8));
		assertEquals(expectedSplitterResult, splitterResult1);

		expectedSplitterResult.clear();
		expectedSplitterResult.addAll(Arrays.asList(0,4,8));
		assertEquals(expectedSplitterResult, splitterResult2);
	}
}
