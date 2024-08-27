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
import org.apache.flink.api.common.functions.InvalidTypesException;
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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for handling missing type information either by calling {@code returns()} or having an
 * explicit type information parameter.
 */
@SuppressWarnings("serial")
class TypeFillTest {

    @Test
    void test() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        assertThatThrownBy(() -> env.addSource(new TestSource<Integer>()).print())
                .isInstanceOf(InvalidTypesException.class);

        DataStream<Long> source = env.fromSequence(1, 10);

        assertThatThrownBy(() -> source.map(new TestMap<Long, Long>()).print())
                .isInstanceOf(InvalidTypesException.class);

        assertThatThrownBy(() -> source.flatMap(new TestFlatMap<Long, Long>()).print())
                .isInstanceOf(InvalidTypesException.class);

        assertThatThrownBy(
                        () ->
                                source.connect(source)
                                        .map(new TestCoMap<Long, Long, Integer>())
                                        .print())
                .isInstanceOf(InvalidTypesException.class);

        assertThatThrownBy(
                        () ->
                                source.connect(source)
                                        .flatMap(new TestCoFlatMap<Long, Long, Integer>())
                                        .print())
                .isInstanceOf(InvalidTypesException.class);

        assertThatThrownBy(() -> source.keyBy(new TestKeySelector<Long, String>()).print())
                .isInstanceOf(InvalidTypesException.class);

        assertThatThrownBy(
                        () ->
                                source.connect(source)
                                        .keyBy(
                                                new TestKeySelector<Long, String>(),
                                                new TestKeySelector<>()))
                .isInstanceOf(InvalidTypesException.class);

        assertThatThrownBy(
                        () ->
                                source.coGroup(source)
                                        .where(new TestKeySelector<>())
                                        .equalTo(new TestKeySelector<>()))
                .isInstanceOf(InvalidTypesException.class);

        assertThatThrownBy(
                        () ->
                                source.join(source)
                                        .where(new TestKeySelector<>())
                                        .equalTo(new TestKeySelector<>()))
                .isInstanceOf(InvalidTypesException.class);

        assertThatThrownBy(
                        () ->
                                source.keyBy((in) -> in)
                                        .intervalJoin(source.keyBy((in) -> in))
                                        .between(Time.milliseconds(10L), Time.milliseconds(10L))
                                        .process(new TestProcessJoinFunction<>())
                                        .print())
                .isInstanceOf(InvalidTypesException.class);

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

        assertThat(source.map(new TestMap<Long, Long>()).returns(Long.class).getType())
                .isEqualTo(BasicTypeInfo.LONG_TYPE_INFO);

        SingleOutputStreamOperator<String> map =
                source.map(
                        new MapFunction<Long, String>() {

                            @Override
                            public String map(Long value) throws Exception {
                                return null;
                            }
                        });

        map.print();
        assertThatThrownBy(() -> map.returns(String.class))
                .isInstanceOf(IllegalStateException.class);
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
