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

package org.apache.flink.streaming.api.datastream;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test for {@link CoGroupedStreams}.
 */
public class CoGroupedStreamsTest {
	private DataStream<String> dataStream1;
	private DataStream<String> dataStream2;
	private KeySelector<String, String> keySelector;
	private TumblingEventTimeWindows tsAssigner;
	private CoGroupFunction<String, String, String> coGroupFunction;

	@Before
	public void setUp() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		dataStream1 = env.fromElements("a1", "a2", "a3");
		dataStream2 = env.fromElements("a1", "a2");
		keySelector = element -> element;
		tsAssigner = TumblingEventTimeWindows.of(Time.milliseconds(1L));
		coGroupFunction = (CoGroupFunction<String, String, String>) (first, second, out) -> out.collect("");
	}

	@Test
	public void testDelegateToCoGrouped() {
		Time lateness = Time.milliseconds(42L);

		CoGroupedStreams.WithWindow<String, String, String, TimeWindow> withLateness = dataStream1
			.coGroup(dataStream2)
			.where(keySelector)
			.equalTo(keySelector)
			.window(tsAssigner)
			.allowedLateness(lateness);

		withLateness.apply(coGroupFunction, BasicTypeInfo.STRING_TYPE_INFO);

		Assert.assertEquals(lateness.toMilliseconds(), withLateness.getWindowedStream().getAllowedLateness());
	}

	@Test
	public void testSetAllowedLateness() {
		Time lateness = Time.milliseconds(42L);

		CoGroupedStreams.WithWindow<String, String, String, TimeWindow> withLateness = dataStream1
			.coGroup(dataStream2)
			.where(keySelector)
			.equalTo(keySelector)
			.window(tsAssigner)
			.allowedLateness(lateness);

		Assert.assertEquals(lateness.toMilliseconds(), withLateness.getAllowedLateness().toMilliseconds());
	}

}
