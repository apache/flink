/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.streaming.api;

import static org.junit.Assert.*;

import org.junit.Test;

import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.util.Collector;

public class BatchTest {

	private static final int PARALLELISM = 1;
	private static final int SOURCE_PARALELISM = 1;
	private static final int SINK_PARALELISM = 3;
	private static int count = 0;
	private static boolean partitionCorrect = true;

	private static final class MySource extends SourceFunction<Tuple1<String>> {

		private Tuple1<String> outTuple = new Tuple1<String>();

		@Override
		public void invoke(Collector<Tuple1<String>> collector) throws Exception {
			for (int i = 0; i < 20; i++) {
				outTuple.f0 = "string #" + i;
				collector.collect(outTuple);
			}
		}
	}

	private static final class MyMap extends FlatMapFunction<Tuple1<String>, Tuple1<String>> {

		@Override
		public void flatMap(Tuple1<String> value, Collector<Tuple1<String>> out) throws Exception {
			out.collect(value);
		}
	}

	private static final class MySink extends SinkFunction<Tuple1<String>> {

		@Override
		public void invoke(Tuple1<String> tuple) {
			count++;
		}
	}

	private static final class MyPartitionSink extends SinkFunction<Tuple1<String>> {

		int hash = -1000;

		@Override
		public void invoke(Tuple1<String> tuple) {
			if (hash == -1000)
				hash = tuple.f0.hashCode() % SINK_PARALELISM;
			else {
				if (hash != tuple.f0.hashCode() % SINK_PARALELISM)
					partitionCorrect = false;
			}
		}
	}

	@Test
	public void test() throws Exception {
		StreamExecutionEnvironment env = new StreamExecutionEnvironment();

		DataStream<Tuple1<String>> dataStream = env.addSource(new MySource(), SOURCE_PARALELISM)
				.flatMap(new MyMap(), PARALLELISM).batch(4).flatMap(new MyMap(), PARALLELISM)
				.batch(2).flatMap(new MyMap(), PARALLELISM).batch(5)
				.flatMap(new MyMap(), PARALLELISM).batch(4).addSink(new MySink());

		env.execute();

		assertEquals(20, count);
	}

	@Test
	public void partitionTest() throws Exception {
		StreamExecutionEnvironment env = new StreamExecutionEnvironment();

		DataStream<Tuple1<String>> dataStream = env.addSource(new MySource(), SOURCE_PARALELISM)
				.flatMap(new MyMap(), PARALLELISM).batch(4).partitionBy(0)
				.addSink(new MyPartitionSink(), SINK_PARALELISM);

		env.execute(SINK_PARALELISM);

		assertTrue(partitionCorrect);
	}
}
