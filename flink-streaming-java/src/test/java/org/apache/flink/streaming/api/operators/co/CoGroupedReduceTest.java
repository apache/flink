///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// *    http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package org.apache.flink.streaming.api.operators.co;
//
//import static org.junit.Assert.assertEquals;
//
//import java.util.Arrays;
//import java.util.List;
//
//import org.apache.flink.api.java.functions.KeySelector;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.api.java.tuple.Tuple3;
//import org.apache.flink.streaming.api.functions.co.CoReduceFunction;
//import org.apache.flink.streaming.api.operators.co.CoStreamGroupedReduce;
//import org.apache.flink.streaming.util.MockCoContext;
//import org.junit.Test;
//
//public class CoGroupedReduceTest {
//
//	private final static class MyCoReduceFunction implements
//			CoReduceFunction<Tuple3<String, String, String>, Tuple2<Integer, Integer>, String> {
//		private static final long serialVersionUID = 1L;
//
//		@Override
//		public Tuple3<String, String, String> reduce1(Tuple3<String, String, String> value1,
//				Tuple3<String, String, String> value2) {
//			return new Tuple3<String, String, String>(value1.f0, value1.f1 + value2.f1, value1.f2);
//		}
//
//		@Override
//		public Tuple2<Integer, Integer> reduce2(Tuple2<Integer, Integer> value1,
//				Tuple2<Integer, Integer> value2) {
//			return new Tuple2<Integer, Integer>(value1.f0, value1.f1 + value2.f1);
//		}
//
//		@Override
//		public String map1(Tuple3<String, String, String> value) {
//			return value.f1;
//		}
//
//		@Override
//		public String map2(Tuple2<Integer, Integer> value) {
//			return value.f1.toString();
//		}
//	}
//
//	@SuppressWarnings("unchecked")
//	@Test
//	public void coGroupedReduceTest() {
//		Tuple3<String, String, String> word1 = new Tuple3<String, String, String>("a", "word1", "b");
//		Tuple3<String, String, String> word2 = new Tuple3<String, String, String>("b", "word2", "a");
//		Tuple3<String, String, String> word3 = new Tuple3<String, String, String>("a", "word3", "a");
//		Tuple2<Integer, Integer> int1 = new Tuple2<Integer, Integer>(2, 1);
//		Tuple2<Integer, Integer> int2 = new Tuple2<Integer, Integer>(1, 2);
//		Tuple2<Integer, Integer> int3 = new Tuple2<Integer, Integer>(0, 3);
//		Tuple2<Integer, Integer> int4 = new Tuple2<Integer, Integer>(2, 4);
//		Tuple2<Integer, Integer> int5 = new Tuple2<Integer, Integer>(1, 5);
//
//		KeySelector<Tuple3<String, String, String>, ?> keySelector0 = new KeySelector<Tuple3<String, String, String>, String>() {
//
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public String getKey(Tuple3<String, String, String> value) throws Exception {
//				return value.f0;
//			}
//		};
//
//		KeySelector<Tuple2<Integer, Integer>, ?> keySelector1 = new KeySelector<Tuple2<Integer, Integer>, Integer>() {
//
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public Integer getKey(Tuple2<Integer, Integer> value) throws Exception {
//				return value.f0;
//			}
//		};
//
//		KeySelector<Tuple3<String, String, String>, ?> keySelector2 = new KeySelector<Tuple3<String, String, String>, String>() {
//
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public String getKey(Tuple3<String, String, String> value) throws Exception {
//				return value.f2;
//			}
//		};
//
//		CoStreamGroupedReduce<Tuple3<String, String, String>, Tuple2<Integer, Integer>, String> invokable = new CoStreamGroupedReduce<Tuple3<String, String, String>, Tuple2<Integer, Integer>, String>(
//				new MyCoReduceFunction(), keySelector0, keySelector1);
//
//		List<String> expected = Arrays.asList("word1", "1", "word2", "2", "word1word3", "3", "5",
//				"7");
//
//		List<String> actualList = MockCoContext.createAndExecute(invokable,
//				Arrays.asList(word1, word2, word3), Arrays.asList(int1, int2, int3, int4, int5));
//
//		assertEquals(expected, actualList);
//
//		invokable = new CoStreamGroupedReduce<Tuple3<String, String, String>, Tuple2<Integer, Integer>, String>(
//				new MyCoReduceFunction(), keySelector2, keySelector1);
//
//		expected = Arrays.asList("word1", "1", "word2", "2", "word2word3", "3", "5", "7");
//
//		actualList = MockCoContext.createAndExecute(invokable, Arrays.asList(word1, word2, word3),
//				Arrays.asList(int1, int2, int3, int4, int5));
//
//		assertEquals(expected, actualList);
//	}
//}
