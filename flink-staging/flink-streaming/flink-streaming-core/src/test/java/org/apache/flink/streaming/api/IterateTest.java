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

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeDataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class IterateTest {

	private static final long MEMORYSIZE = 32;
	private static boolean iterated[];
	private static int PARALLELISM = 2;

	public static final class IterationHead extends RichFlatMapFunction<Boolean,Boolean> {

		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(Boolean value, Collector<Boolean> out) throws Exception {
			int indx = getRuntimeContext().getIndexOfThisSubtask();
			if (value) {
				iterated[indx] = true;
			} else {
				out.collect(value);
			}

		}

	}

	public static final class IterationTail extends RichFlatMapFunction<Boolean,Boolean> {

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

	public StreamExecutionEnvironment constructIterativeJob(StreamExecutionEnvironment env){
		env.setBufferTimeout(10);

		DataStream<Boolean> source = env.fromCollection(Collections.nCopies(PARALLELISM, false));

		IterativeDataStream<Boolean> iteration = source.iterate(3000);

		DataStream<Boolean> increment = iteration.flatMap(new IterationHead()).flatMap(
				new IterationTail());

		iteration.closeWith(increment).addSink(new MySink());
		return env;
	}

	@Test
	public void test() throws Exception {
		StreamExecutionEnvironment env = new TestStreamEnvironment(PARALLELISM, MEMORYSIZE);
		iterated = new boolean[PARALLELISM];

		env = constructIterativeJob(env);

		env.execute();

		for (boolean iter : iterated) {
			assertTrue(iter);
		}

	}

	@Test
	public void testWithCheckPointing() throws Exception {
		StreamExecutionEnvironment env = new TestStreamEnvironment(PARALLELISM, MEMORYSIZE);

		env = constructIterativeJob(env);

		env.enableCheckpointing();
		try {
			env.execute();

			// this statement should never be reached
			fail();
		} catch (UnsupportedOperationException e) {
			// expected behaviour
		}

	}

}
