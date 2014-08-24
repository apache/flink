/**
 *
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
 *
 */

package org.apache.flink.streaming.api.collector;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;

import org.apache.flink.api.java.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitDataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.sink.SinkFunction;
import org.apache.flink.streaming.util.LogUtils;
import org.apache.log4j.Level;
import org.junit.Test;

public class DirectedOutputTest {

	static HashSet<Long> evenSet = new HashSet<Long>();
	static HashSet<Long> oddSet = new HashSet<Long>();

	private static class PlusTwo extends RichMapFunction<Long, Long> {

		private static final long serialVersionUID = 1L;

		@Override
		public Long map(Long arg0) throws Exception {
			arg0 += 2;
			return arg0;
		}
	}

	private static class EvenSink implements SinkFunction<Long> {

		private static final long serialVersionUID = 1L;

		@Override
		public void invoke(Long tuple) {
			evenSet.add(tuple);
		}
	}

	private static class OddSink implements SinkFunction<Long> {

		private static final long serialVersionUID = 1L;

		@Override
		public void invoke(Long tuple) {
			oddSet.add(tuple);
		}
	}

	private static class MySelector extends OutputSelector<Long> {

		private static final long serialVersionUID = 1L;

		@Override
		public void select(Long tuple, Collection<String> outputs) {
			int mod = (int) (tuple % 2);
			switch (mod) {
			case 0:
				outputs.add("ds1");
				break;
			case 1:
				outputs.add("ds2");
				break;
			}
		}
	}

	@SuppressWarnings("unused")
	@Test
	public void directOutputTest() throws Exception {
		LogUtils.initializeDefaultConsoleLogger(Level.OFF, Level.OFF);

		LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
		SplitDataStream<Long> s = env.generateSequence(1, 6).split(new MySelector(),
				new String[] { "ds1", "ds2" });
		DataStream<Long> ds1 = s.select("ds1").shuffle().map(new PlusTwo()).addSink(new EvenSink());
		DataStream<Long> ds2 = s.select("ds2").map(new PlusTwo()).addSink(new OddSink());

		env.executeTest(32);

		HashSet<Long> expectedEven = new HashSet<Long>(Arrays.asList(4L, 6L, 8L));
		HashSet<Long> expectedOdd = new HashSet<Long>(Arrays.asList(3L, 5L, 7L));

		assertEquals(expectedEven, evenSet);
		assertEquals(expectedOdd, oddSet);
	}
}
