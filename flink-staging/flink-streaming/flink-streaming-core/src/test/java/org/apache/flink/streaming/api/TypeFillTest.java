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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.List;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.CoReduceFunction;
import org.apache.flink.streaming.api.functions.co.CoWindowFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.junit.Test;

@SuppressWarnings("serial")
public class TypeFillTest {

	@Test
	public void test() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		try {
			env.addSource(new TestSource<Integer>()).print();
			fail();
		} catch (Exception e) {
		}

		DataStream<Long> source = env.generateSequence(1, 10);

		try {
			source.map(new TestMap<Long, Long>()).print();
			fail();
		} catch (Exception e) {
		}
		try {
			source.flatMap(new TestFlatMap<Long, Long>()).print();
			fail();
		} catch (Exception e) {
		}
		try {
			source.connect(source).map(new TestCoMap<Long, Long, Integer>()).print();
			fail();
		} catch (Exception e) {
		}
		try {
			source.connect(source).flatMap(new TestCoFlatMap<Long, Long, Integer>()).print();
			fail();
		} catch (Exception e) {
		}
		try {
			source.connect(source).reduce(new TestCoReduce<Long, Long, Integer>()).print();
			fail();
		} catch (Exception e) {
		}
		try {
			source.connect(source).windowReduce(new TestCoWindow<Long, Long, String>(), 10, 100)
					.print();
			fail();
		} catch (Exception e) {
		}

		env.addSource(new TestSource<Integer>()).returns("Integer");
		source.map(new TestMap<Long, Long>()).returns(Long.class).print();
		source.flatMap(new TestFlatMap<Long, Long>()).returns("Long").print();
		source.connect(source).map(new TestCoMap<Long, Long, Integer>()).returns("Integer").print();
		source.connect(source).flatMap(new TestCoFlatMap<Long, Long, Integer>())
				.returns(BasicTypeInfo.INT_TYPE_INFO).print();
		source.connect(source).reduce(new TestCoReduce<Long, Long, Integer>())
				.returns(Integer.class).print();
		source.connect(source).windowReduce(new TestCoWindow<Long, Long, String>(), 10, 100)
				.returns("String").print();

		assertEquals(BasicTypeInfo.LONG_TYPE_INFO,
				source.map(new TestMap<Long, Long>()).returns(Long.class).getType());

		SingleOutputStreamOperator<String, ?> map = source.map(new MapFunction<Long, String>() {

			@Override
			public String map(Long value) throws Exception {
				return null;
			}
		});

		map.print();
		try {
			map.returns("String");
			fail();
		} catch (Exception e) {
		}

	}

	private class TestSource<T> implements SourceFunction<T> {


		@Override
		public boolean reachedEnd() throws Exception {
			return false;
		}

		@Override
		public T next() throws Exception {
			return null;
		}

	}

	private class TestMap<T, O> implements MapFunction<T, O> {
		@Override
		public O map(T value) throws Exception {
			return null;
		}
	}

	private class TestFlatMap<T, O> implements FlatMapFunction<T, O> {
		@Override
		public void flatMap(T value, Collector<O> out) throws Exception {
		}
	}

	private class TestCoMap<IN1, IN2, OUT> implements CoMapFunction<IN1, IN2, OUT> {

		@Override
		public OUT map1(IN1 value) {
			return null;
		}

		@Override
		public OUT map2(IN2 value) {
			return null;
		}

	}

	private class TestCoFlatMap<IN1, IN2, OUT> implements CoFlatMapFunction<IN1, IN2, OUT> {

		@Override
		public void flatMap1(IN1 value, Collector<OUT> out) throws Exception {
		}

		@Override
		public void flatMap2(IN2 value, Collector<OUT> out) throws Exception {
		}

	}

	private class TestCoReduce<IN1, IN2, OUT> implements CoReduceFunction<IN1, IN2, OUT> {

		@Override
		public IN1 reduce1(IN1 value1, IN1 value2) {
			return null;
		}

		@Override
		public IN2 reduce2(IN2 value1, IN2 value2) {
			return null;
		}

		@Override
		public OUT map1(IN1 value) {
			return null;
		}

		@Override
		public OUT map2(IN2 value) {
			return null;
		}

	}

	private class TestCoWindow<IN1, IN2, OUT> implements CoWindowFunction<IN1, IN2, OUT> {

		@Override
		public void coWindow(List<IN1> first, List<IN2> second, Collector<OUT> out)
				throws Exception {
		}

	}

}
