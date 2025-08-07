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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

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

        assertThatThrownBy(
                        () ->
                                env.fromSource(
                                                new NoopSource<Integer>(),
                                                WatermarkStrategy.noWatermarks(),
                                                "NoopSource")
                                        .print())
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
                                        .between(Duration.ofMillis(10L), Duration.ofMillis(10L))
                                        .process(new TestProcessJoinFunction<>())
                                        .print())
                .isInstanceOf(InvalidTypesException.class);

        env.fromSource(new NoopSource<Integer>(), WatermarkStrategy.noWatermarks(), "NoopSource")
                .returns(Integer.class);
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
                .between(Duration.ofMillis(10L), Duration.ofMillis(10L))
                .process(new TestProcessJoinFunction<Long, Long, String>())
                .returns(Types.STRING);
        source.keyBy((in) -> in)
                .intervalJoin(source.keyBy((in) -> in))
                .between(Duration.ofMillis(10L), Duration.ofMillis(10L))
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

    private static class NoopSource<T> implements Source<T, SourceSplit, Void> {
        private static final long serialVersionUID = 1L;

        @Override
        public Boundedness getBoundedness() {
            return Boundedness.BOUNDED;
        }

        @Override
        public SourceReader<T, SourceSplit> createReader(SourceReaderContext readerContext) {
            return new NoopSourceReader<>();
        }

        @Override
        public SplitEnumerator<SourceSplit, Void> createEnumerator(
                SplitEnumeratorContext<SourceSplit> enumContext) {
            return new NoopSplitEnumerator(enumContext);
        }

        @Override
        public SplitEnumerator<SourceSplit, Void> restoreEnumerator(
                SplitEnumeratorContext<SourceSplit> enumContext, Void checkpoint) {
            return createEnumerator(enumContext);
        }

        @Override
        public SimpleVersionedSerializer<SourceSplit> getSplitSerializer() {
            return new NoopSplitSerializer();
        }

        @Override
        public SimpleVersionedSerializer<Void> getEnumeratorCheckpointSerializer() {
            return new SimpleVersionedSerializer<Void>() {
                @Override
                public int getVersion() {
                    return 1;
                }

                @Override
                public byte[] serialize(Void obj) {
                    return new byte[0];
                }

                @Override
                public Void deserialize(int version, byte[] serialized) {
                    return null;
                }
            };
        }
    }

    private static class NoopSourceReader<T> implements SourceReader<T, SourceSplit> {
        @Override
        public void start() {}

        @Override
        public InputStatus pollNext(ReaderOutput<T> output) {
            return InputStatus.END_OF_INPUT;
        }

        @Override
        public List<SourceSplit> snapshotState(long checkpointId) {
            return java.util.Collections.emptyList();
        }

        @Override
        public void addSplits(List<SourceSplit> splits) {}

        @Override
        public void notifyNoMoreSplits() {}

        @Override
        public CompletableFuture<Void> isAvailable() {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void close() {}
    }

    private static class NoopSplitEnumerator implements SplitEnumerator<SourceSplit, Void> {
        private final SplitEnumeratorContext<SourceSplit> context;

        public NoopSplitEnumerator(SplitEnumeratorContext<SourceSplit> context) {
            this.context = context;
        }

        @Override
        public void start() {}

        @Override
        public void handleSplitRequest(int subtaskId, String requesterHostname) {}

        @Override
        public void addSplitsBack(List<SourceSplit> splits, int subtaskId) {}

        @Override
        public void addReader(int subtaskId) {}

        @Override
        public Void snapshotState(long checkpointId) {
            return null;
        }

        @Override
        public void close() {}
    }

    private static class NoopSplitSerializer implements SimpleVersionedSerializer<SourceSplit> {
        @Override
        public int getVersion() {
            return 1;
        }

        @Override
        public byte[] serialize(SourceSplit split) {
            return new byte[0];
        }

        @Override
        public SourceSplit deserialize(int version, byte[] serialized) {
            return null;
        }
    }
}
