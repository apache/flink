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

package org.apache.flink.streaming.api.invokable.operator;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.DataStream;
import org.apache.flink.streaming.api.LocalStreamEnvironment;
import org.apache.flink.streaming.api.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.co.CoMapFunction;
import org.junit.Assert;
import org.junit.Test;

public class CoMapTest implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private static Set<String> result = new HashSet<String>();
	private static Set<String> expected = new HashSet<String>();

	@Test
	public void test() {
		expected.add("a");
		expected.add("b");
		expected.add("c");
		expected.add("1");
		expected.add("2");
		expected.add("3");
		expected.add("4");
		
		LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
		
		DataStream<Tuple1<Integer>> ds1 = env.fromElements(1, 2, 3, 4);

		@SuppressWarnings("unused")
		DataStream<Tuple1<Boolean>> ds2 = env.fromElements("a", "b", "c").coMapWith(new CoMapFunction<Tuple1<String>, Tuple1<Integer>, Tuple1<Boolean>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple1<Boolean> map1(Tuple1<String> value) {
				System.out.println("1: " + value);
				result.add(value.f0);
				return new Tuple1<Boolean>(true);
			}

			@Override
			public Tuple1<Boolean> map2(Tuple1<Integer> value) {
				System.out.println("2: " +value);
				result.add(value.f0.toString());
				return new Tuple1<Boolean>(false);
			}
		}, ds1)
		.print();
		
		env.executeTest(32);
		Assert.assertArrayEquals(expected.toArray(), result.toArray());
	}
}
