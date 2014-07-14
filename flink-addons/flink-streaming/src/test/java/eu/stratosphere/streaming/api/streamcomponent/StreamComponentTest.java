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

package eu.stratosphere.streaming.api.streamcomponent;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.streaming.api.DataStream;
import eu.stratosphere.streaming.api.SinkFunction;
import eu.stratosphere.streaming.api.SourceFunction;
import eu.stratosphere.streaming.api.StreamExecutionEnvironment;
import eu.stratosphere.util.Collector;

public class StreamComponentTest {

	public static Map<Integer, Integer> data = new HashMap<Integer, Integer>();

	public static class MySource extends SourceFunction<Tuple1<Integer>> {
		private static final long serialVersionUID = 1L;

		private Tuple1<Integer> tuple = new Tuple1<Integer>(0);

		@Override
		public void invoke(Collector<Tuple1<Integer>> collector) throws Exception {
			for (int i = 0; i < 10; i++) {
				tuple.f0 = i;
				collector.collect(tuple);
				System.out.println("collecting " + tuple);
			}
		}
	}

	public static class MyTask extends MapFunction<Tuple1<Integer>, Tuple2<Integer, Integer>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<Integer, Integer> map(Tuple1<Integer> value) throws Exception {
			Integer i = value.f0;
			System.out.println("mapping " + i);
			return new Tuple2<Integer, Integer>(i, i + 1);
		}
	}
	
	// TODO test multiple tasks
	// public static class MyOtherTask extends MapFunction<Tuple1<Integer>,
	// Tuple2<Integer, Integer>> {
	// private static final long serialVersionUID = 1L;
	//
	// @Override
	// public Tuple2<Integer, Integer> map(Tuple1<Integer> value) throws
	// Exception {
	// Integer i = value.f0;
	// return new Tuple2<Integer, Integer>(-i - 1, -i - 2);
	// }
	// }

	public static class MySink extends SinkFunction<Tuple2<Integer, Integer>> {
		private static final long serialVersionUID = 1L;

		@Override
		public void invoke(Tuple2<Integer, Integer> tuple) {
			Integer k = tuple.getField(0);
			Integer v = tuple.getField(1);
			data.put(k, v);
		}
	}

	@BeforeClass
	public static void runStream() {
		StreamExecutionEnvironment context = new StreamExecutionEnvironment();

		DataStream<Tuple2<Integer, Integer>> oneTask = context.addSource(new MySource())
				.map(new MyTask()).addSink(new MySink());

		context.execute();
	}

	@Test
	public void test() {
		assertEquals(10, data.keySet().size());

		for (Integer k : data.keySet()) {
			assertEquals((Integer) (k + 1), data.get(k));
		}
	}
}
