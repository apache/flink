/**
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

import static org.junit.Assert.assertTrue;

import org.apache.flink.api.java.functions.RichFlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeDataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.LogUtils;
import org.junit.Test;

public class IterateTest {

	private static final long MEMORYSIZE = 32;
	private static boolean iterated = false;

	public static final class IterationHead extends RichFlatMapFunction<Boolean, Boolean> {

		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(Boolean value, Collector<Boolean> out) throws Exception {
			if (value) {
				iterated = true;
			} else {
				out.collect(value);
			}

		}

	}

	public static final class IterationTail extends RichFlatMapFunction<Boolean, Boolean> {

		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(Boolean value, Collector<Boolean> out) throws Exception {
			out.collect(true);

		}

	}

	public static final class MySink implements SinkFunction<Boolean> {

		private static final long serialVersionUID = 1L;

		@Override
		public void invoke(Boolean tuple) {
		}

	}

	@Test
	public void test() throws Exception {
		LogUtils.initializeDefaultTestConsoleLogger();

		LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);

		env.setBufferTimeout(10);

		DataStream<Boolean> source = env.fromElements(false, false, false);

		IterativeDataStream<Boolean> iteration = source.iterate().setMaxWaitTime(3000);

		DataStream<Boolean> increment = iteration.flatMap(new IterationHead()).flatMap(
				new IterationTail());

		iteration.closeWith(increment).addSink(new MySink());

		env.executeTest(MEMORYSIZE);

		assertTrue(iterated);

	}

}
