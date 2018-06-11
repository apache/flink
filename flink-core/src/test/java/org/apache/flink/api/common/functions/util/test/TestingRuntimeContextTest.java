/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.common.functions.util.test;

import org.apache.flink.api.common.functions.BroadcastVariableInitializer;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import org.junit.Assert;
import org.junit.Test;

import java.util.*;

/**
 * Test for the {@link TestingRuntimeContext}.
 */
public class TestingRuntimeContextTest {

	@Test
	public void testWordDistinctFlat() throws Exception {
		TestingRuntimeContext ctx = new TestingRuntimeContext(false);
		ctx.setBroadcastVariable("exWords", Collections.singletonList("Eat"));
		WordDistinctFlat flat = new WordDistinctFlat();
		flat.setRuntimeContext(ctx);
		flat.flatMap("Eat,Eat,Walk,Run", ctx.getCollector());
		Assert.assertArrayEquals(ctx.getCollectorOutput().toArray(), new String[]{"Walk", "Run"});
	}

	@Test
	public void testCharDistinctFlat() throws Exception {
		TestingRuntimeContext ctx = new TestingRuntimeContext(false);
		ctx.setBroadcastVariable("exclude", Collections.singletonList('c'));
		CharDistinctFlat flatFunc = new CharDistinctFlat();
		flatFunc.setRuntimeContext(ctx);
		flatFunc.flatMap("abbcd", ctx.getCollector());
		Assert.assertArrayEquals(ctx.getCollectorOutput().toArray(), new Character[]{'a', 'b', 'd'});
	}

	@Test
	public void testReverseMap() throws Exception {
		ReverseMap m = new ReverseMap();
		String result = m.map("12345678");
		Assert.assertEquals(result, "87654321");
	}

	@Test
	public void testAttributionSumCoGroup() throws Exception {
		TestingRuntimeContext ctx = new TestingRuntimeContext(false);
		ctx.setBroadcastVariable("min", Collections.singletonList(3));
		Collector<Integer> collector = ctx.getCollector();
		AttributionSumCoGroup cg = new AttributionSumCoGroup();
		cg.setRuntimeContext(ctx);
		List<Tuple2<String, Integer>> array1 = new ArrayList<>();
		array1.add(new Tuple2<>("1", 2));
		array1.add(new Tuple2<>("1", 3));
		array1.add(new Tuple2<>("1", 4));
		List<Tuple2<String, Integer>> array2 = new ArrayList<>();
		array2.add(new Tuple2<>("1", 1));
		cg.coGroup(array1, array2, collector);
		Assert.assertArrayEquals(ctx.getCollectorOutput().toArray(), new Integer[]{5, 9});
	}

	/**
	 *  User-defined flatMap function to deduplicate words.
	 */
	public static class WordDistinctFlat extends RichFlatMapFunction<String, String> {

		@Override
		public void flatMap(String value, Collector<String> out) throws Exception {
			Set<String> wordsList = new HashSet<>(Arrays.asList(value.split(",")));
			List<String> excludeWords = getRuntimeContext().getBroadcastVariable("exWords");
			for (String word: wordsList) {
				if (!excludeWords.contains(word)) {
					out.collect(word);
				}
			}
		}
	}

	/**
	 *  User-defined flatMap function to deduplicate characters.
	 */
	public static class CharDistinctFlat extends RichFlatMapFunction<String, Character> {

		@Override
		public void flatMap(String value, Collector<Character> out) throws Exception {
			List<Character> excludes = getRuntimeContext().getBroadcastVariable("exclude");
			Set<Character> characters = new HashSet<>();
			char[] chars = value.toCharArray();
			for (char c : chars) {
				if (!characters.contains(c) && !excludes.contains(c)) {
					characters.add(c);
					out.collect(c);
				}
			}
		}
	}

	/**
	 *  User-defined map function that reverses string.
	 */
	public static class ReverseMap implements MapFunction<String, String> {

		@Override
		public String map(String value) throws Exception {
			return new StringBuffer(value).reverse().toString();
		}
	}

	/**
	 *  User-defined map function that cogroups data which is bigger than minimum.
	 */
	public static class AttributionSumCoGroup extends RichCoGroupFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Integer> {

		@Override
		public void coGroup(Iterable<Tuple2<String, Integer>> first, Iterable<Tuple2<String, Integer>> second, Collector<Integer> out) throws Exception {
			int sum = 0;
			int minThreshold = getRuntimeContext().getBroadcastVariableWithInitializer("min", new BroadcastVariableInitializer<Integer, Integer>() {

				@Override
				public Integer initializeBroadcastVariable(Iterable<Integer> data) {
					for (Integer v : data) {
						if (v > 0) {
							return v;
						}
					}
					return 0;
				}
			});
			for (Tuple2<String, Integer> aFirst : first) {
				int v = aFirst.f1;
				sum += v;
				if (sum > minThreshold) {
					out.collect(sum);
				}
			}
		}
	}

}
