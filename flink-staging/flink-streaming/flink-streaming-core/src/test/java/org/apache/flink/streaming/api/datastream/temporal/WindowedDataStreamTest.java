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

package org.apache.flink.streaming.api.datastream.temporal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.DataStreamTest;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.JoinWindowFunction;
import org.apache.flink.streaming.api.windowing.helper.Timestamp;
import org.apache.flink.streaming.util.CustomPOJO;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.streaming.util.keys.KeySelectorUtil;
import org.junit.Test;

public class WindowedDataStreamTest {

	private static final long MEMORYSIZE = 32;
	private static int PARALLELISM = 3;

	@SuppressWarnings("unchecked")
	@Test
	public void windowTest() {
		StreamExecutionEnvironment env = new TestStreamEnvironment(PARALLELISM, MEMORYSIZE);

		DataStreamSource<CustomPOJO> src = env.fromElements(new CustomPOJO());
		DataStreamSource<String> stringSrc = env.fromElements("a");

		Timestamp<CustomPOJO> timestamp1 = new Timestamp<CustomPOJO>() {
			@Override
			public long getTimestamp(CustomPOJO value) {
				if (value == null) {
					return 10L;
				}
				return 0L;
			}
		};
		Timestamp<String> timestamp2 = new Timestamp<String>() {
			@Override
			public long getTimestamp(String value) {
				if (value == null) {
					return 20L;
				}
				return 0;
			}
		};

		// join
		StreamJoinOperator<CustomPOJO, String> join = src.join(stringSrc);
		assertEquals(src.getId(), join.input1.getId());
		assertEquals(stringSrc.getId(), join.input2.getId());

		KeySelector<String, Integer> keySelector = new KeySelector<String, Integer>() {
			@Override
			public Integer getKey(String value) throws Exception {
				return null;
			}
		};
		JoinFunction<CustomPOJO, String, Long> joinFunction = new JoinFunction<CustomPOJO, String, Long>() {
			@Override
			public Long join(CustomPOJO first, String second) throws Exception {
				return null;
			}
		};

		StreamJoinOperator.JoinWindow<CustomPOJO, String> windowJoin = join
				.onWindow(4L, timestamp1, timestamp2);
		assertEquals(4L, join.windowSize);
		assertEquals(10L, join.timeStamp1.getTimestamp(null));
		assertEquals(20L, join.timeStamp2.getTimestamp(null));

		StreamJoinOperator.JoinWindow<CustomPOJO, String> every = windowJoin.every(1L, TimeUnit.MINUTES);
		assertEquals(60000L, join.slideInterval);

		StreamJoinOperator.JoinedStream<CustomPOJO, String> joinedStream = every
				.where("i")
				.equalTo(keySelector);

		assertTrue(joinedStream.predicate.keys1 instanceof KeySelectorUtil.ComparableKeySelector);
		assertEquals(keySelector, joinedStream.predicate.keys2);

		DataStream<Long> with = joinedStream.with(joinFunction);

		assertTrue(DataStreamTest.getFunctionForDataStream(with) instanceof JoinWindowFunction);
	}
}
