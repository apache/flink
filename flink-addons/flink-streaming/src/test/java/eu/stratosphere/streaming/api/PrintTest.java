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

import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.util.Collector;

public class PrintTest {

	public static final class MyFlatMap extends
			FlatMapFunction<Tuple2<Integer, String>, Tuple2<Integer, String>> {

		@Override
		public void flatMap(Tuple2<Integer, String> value, Collector<Tuple2<Integer, String>> out)
				throws Exception {
			out.collect(new Tuple2<Integer, String>(value.f0 * value.f0, value.f1));

		}

	}

	public static final class MySource extends SourceFunction<Tuple2<Integer, String>> {

		@Override
		public void invoke(Collector<Tuple2<Integer, String>> collector) throws Exception {
			for (int i = 0; i < 10; i++) {
				collector.collect(new Tuple2<Integer, String>(i, "test"));
			}
		}
	}

	@Test
	public void test() throws Exception {

		StreamExecutionEnvironment env = new StreamExecutionEnvironment();
		DataStream<Tuple2<Integer, String>> source = env.addSource(new MySource(), 1);
		DataStream<Tuple2<Integer, String>> map = source.flatMap(new MyFlatMap(), 1).print();
		env.fromElements(2, 3, 4).print();
		Set<Integer> a = new HashSet<Integer>();
		a.add(-2);
		a.add(-100);
		env.fromCollection(a).print();
		source.print();
		env.execute();

	}
	
	
}
