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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests for handling missing type information either by calling {@code returns()} or having an
 * explicit type information parameter.
 */
@SuppressWarnings("serial")
public class TypeFillTest {

    @Test
    public void test() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        try {
            env.addSource(new TestSource<Integer>()).print();
            fail();
        } catch (Exception ignored) {
        }

        DataStream<Long> source = env.generateSequence(1, 10);

        try {
            source.map(new TestMap<Long, Long>()).print();
            fail();
        } catch (Exception ignored) {
        }

        try {
            source.flatMap(new TestFlatMap<Long, Long>()).print();
            fail();
        } catch (Exception ignored) {
        }

        try {
            source.connect(source).map(new TestCoMap<Long, Long, Integer>()).print();
            fail();
        } catch (Exception ignored) {
        }

        try {
            source.connect(source).flatMap(new TestCoFlatMap<Long, Long, Integer>()).print();
            fail();
        } catch (Exception ignored) {
        }

        try {
            source.keyBy(new TestKeySelector<Long, String>()).print();
            fail();
        } catch (Exception ignored) {
        }

        try {
            source.connect(source)
                    .keyBy(new TestKeySelector<Long, String>(), new TestKeySelector<>());
            fail();
        } catch (Exception ignored) {
        }

        try {
            source.coGroup(source).where(new TestKeySelector<>()).equalTo(new TestKeySelector<>());
            fail();
        } catch (Exception ignored) {
        }

        try {
            source.join(source).where(new TestKeySelector<>()).equalTo(new TestKeySelector<>());
            fail();
        } catch (Exception ignored) {
        }

        try {
            source.keyBy((in) -> in)
                    .intervalJoin(source.keyBy((in) -> in))
                    .between(Time.milliseconds(10L), Time.milliseconds(10L))
                    .process(new TestProcessJoinFunction<>())
                    .print();
            fail();
        } catch (Exception ignored) {
        }

        env.addSource(new TestSource<Integer>()).returns(Integer.class);
        source.map(new TestMap<Long, Long>()).returns(Long.class).print();
        source.flatMap(new TestFlatMap<Long, Long>()).returns(new TypeHint<Long>() {}).print();
        source.connect(source)
                .map(new TestCoMap<Long, Long, Integer>())
                .returns(BasicTypeInfo.INT_TYPE_INFO)
                .print();
        source.connect(source)
                .flatMap(new TestCoFlatMap<Long, Long, Integer>())
                .returns(BasicTypeInfo.INT_TYPE_INFO)
                .print();
        source.connect(source)
                .keyBy(new TestKeySelector<>(), new TestKeySelector<>(), Types.STRING);
        source.coGroup(source)
                .where(new TestKeySelector<>(), Types.STRING)
                .equalTo(new TestKeySelector<>(), Types.STRING);
        source.join(source)
                .where(new TestKeySelector<>(), Types.STRING)
                .equalTo(new TestKeySelector<>(), Types.STRING);
        source.keyBy((in) -> in)
                .intervalJoin(source.keyBy((in) -> in))
                .between(Time.milliseconds(10L), Time.milliseconds(10L))
                .process(new TestProcessJoinFunction<Long, Long, String>())
                .returns(Types.STRING);
        source.keyBy((in) -> in)
                .intervalJoin(source.keyBy((in) -> in))
                .between(Time.milliseconds(10L), Time.milliseconds(10L))
                .process(new TestProcessJoinFunction<>(), Types.STRING);

        assertEquals(
                BasicTypeInfo.LONG_TYPE_INFO,
                source.map(new TestMap<Long, Long>()).returns(Long.class).getType());

        SingleOutputStreamOperator<String> map =
                source.map(
                        new MapFunction<Long, String>() {

                            @Override
                            public String map(Long value) throws Exception {
                                return null;
                            }
                        });

        map.print();
        try {
            map.returns(String.class);
            fail();
        } catch (Exception ignored) {
        }
    }

    private static class TestSource<T> implements SourceFunction<T> {
        private static final long serialVersionUID = 1L;

        @Override
        public void run(SourceContext<T> ctx) throws Exception {}

        @Override
        public void cancel() {}
    }

    private static class TestMap<T, O> implements MapFunction<T, O> {
        @Override
        public O map(T value) throws Exception {
            return null;
        }
    }

    private static class TestFlatMap<T, O> implements FlatMapFunction<T, O> {
        @Override
        public void flatMap(T value, Collector<O> out) throws Exception {}
    }

    private static class TestCoMap<IN1, IN2, OUT> implements CoMapFunction<IN1, IN2, OUT> {

        @Override
        public OUT map1(IN1 value) {
            return null;
        }

        @Override
        public OUT map2(IN2 value) {
            return null;
        }
    }

    private static class TestCoFlatMap<IN1, IN2, OUT> implements CoFlatMapFunction<IN1, IN2, OUT> {

        @Override
        public void flatMap1(IN1 value, Collector<OUT> out) throws Exception {}

        @Override
        public void flatMap2(IN2 value, Collector<OUT> out) throws Exception {}
    }

    private static class TestKeySelector<IN, KEY> implements KeySelector<IN, KEY> {

        @Override
        public KEY getKey(IN value) throws Exception {
            return null;
        }
    }

    private static class TestProcessJoinFunction<IN1, IN2, OUT>
            extends ProcessJoinFunction<IN1, IN2, OUT> {

        @Override
        public void processElement(IN1 left, IN2 right, Context ctx, Collector<OUT> out)
                throws Exception {
            // nothing to do
        }
    }
}
