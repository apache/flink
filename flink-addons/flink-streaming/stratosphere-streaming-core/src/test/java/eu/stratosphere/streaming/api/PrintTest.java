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
import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.util.Collector;

public class PrintTest {

	public static final class MyFlatMap extends
			FlatMapFunction<Tuple2<Integer, String>, Tuple2<Integer, String>> {

		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(Tuple2<Integer, String> value,
				Collector<Tuple2<Integer, String>> out) throws Exception {
			out.collect(new Tuple2<Integer, String>(value.f0 * value.f0,
					value.f1));

		}

	}

	private static final long MEMORYSIZE = 32;

	public static final class Increment extends
			FlatMapFunction<Tuple1<Integer>, Tuple1<Integer>> {

		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(Tuple1<Integer> value,
				Collector<Tuple1<Integer>> out) throws Exception {
			if (value.f0 < 5) {
				out.collect(new Tuple1<Integer>(value.f0 + 1));
			}

		}

	}

	public static final class Forward extends
			FlatMapFunction<Tuple1<Integer>, Tuple1<Integer>> {

		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(Tuple1<Integer> value,
				Collector<Tuple1<Integer>> out) throws Exception {
			out.collect(value);

		}

	}

	@Test
	public void test() throws Exception {

		LocalStreamEnvironment env = StreamExecutionEnvironment
				.createLocalEnvironment(1);
		
		 env.generateSequence(1, 10).print();
		 Set<Integer> a = new HashSet<Integer>();
		 a.add(-2);
		 a.add(-100);
		 env.fromCollection(a).print();
		env.executeTest(MEMORYSIZE);

	}

}
