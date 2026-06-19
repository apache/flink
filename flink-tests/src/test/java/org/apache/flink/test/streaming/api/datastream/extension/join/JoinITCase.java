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

package org.apache.flink.test.streaming.api.datastream.extension.join;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.connector.dsv2.DataStreamV2SourceUtils;
import org.apache.flink.api.connector.dsv2.WrappedSink;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.datastream.api.ExecutionEnvironment;
import org.apache.flink.datastream.api.builtin.BuiltinFuncs;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.apache.flink.datastream.api.context.RuntimeContext;
import org.apache.flink.datastream.api.extension.join.JoinFunction;
import org.apache.flink.datastream.api.extension.join.JoinType;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.api.function.TwoInputNonBroadcastStreamProcessFunction;
import org.apache.flink.datastream.api.stream.KeyedPartitionStream;
import org.apache.flink.datastream.api.stream.NonKeyedPartitionStream;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.test.util.AbstractTestBase;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.test.util.TestBaseUtils.compareResultsByLinesInMemory;
import static org.assertj.core.api.Assertions.assertThat;

/** Test the Join extension on DataStream V2. */
class JoinITCase extends AbstractTestBase implements Serializable {
    private transient ExecutionEnvironment env;
    private static List<String> sinkResults;

    @BeforeEach
    void before() throws Exception {
        env = ExecutionEnvironment.getInstance();
        sinkResults = new ArrayList<>();
    }

    @AfterEach
    void after() throws Exception {
        sinkResults.clear();
    }

    @Test
    void testInnerJoinWithSameKey() throws Exception {
        NonKeyedPartitionStream<KeyAndValue> source1 =
                getSourceStream(
                        Arrays.asList(
                                KeyAndValue.of("key", 0),
                                KeyAndValue.of("key", 1),
                                KeyAndValue.of("key", 2)),
                        "source1");
        NonKeyedPartitionStream<KeyAndValue> source2 =
                getSourceStream(
                        Arrays.asList(
                                KeyAndValue.of("key", 0),
                                KeyAndValue.of("key", 1),
                                KeyAndValue.of("key", 2)),
                        "source2");

        NonKeyedPartitionStream<String> joinedStream =
                BuiltinFuncs.join(
                        source1,
                        (KeySelector<KeyAndValue, String>) keyAndValue -> keyAndValue.key,
                        source2,
                        (KeySelector<KeyAndValue, String>) keyAndValue -> keyAndValue.key,
                        new TestJoinFunction(),
                        JoinType.INNER);

        joinedStream.toSink(new WrappedSink<>(new TestSink()));
        env.execute("testInnerJoinWithSameKey");

        expectInAnyOrder(
                "key:0:0", "key:0:1", "key:0:2", "key:1:0", "key:1:1", "key:1:2", "key:2:0",
                "key:2:1", "key:2:2");
    }

    @Test
    void testInnerJoinWithMultipleKeys() throws Exception {
        NonKeyedPartitionStream<KeyAndValue> source1 =
                getSourceStream(
                        Arrays.asList(
                                KeyAndValue.of("key0", 0),
                                KeyAndValue.of("key1", 1),
                                KeyAndValue.of("key2", 2),
                                KeyAndValue.of("key2", 3)),
                        "source1");
        NonKeyedPartitionStream<KeyAndValue> source2 =
                getSourceStream(
                        Arrays.asList(
                                KeyAndValue.of("key2", 4),
                                KeyAndValue.of("key2", 5),
                                KeyAndValue.of("key0", 6),
                                KeyAndValue.of("key1", 7)),
                        "source2");

        NonKeyedPartitionStream<String> joinedStream =
                BuiltinFuncs.join(
                        source1,
                        (KeySelector<KeyAndValue, String>) keyAndValue -> keyAndValue.key,
                        source2,
                        (KeySelector<KeyAndValue, String>) keyAndValue -> keyAndValue.key,
                        new TestJoinFunction(),
                        JoinType.INNER);

        joinedStream.toSink(new WrappedSink<>(new TestSink()));
        env.execute("testInnerJoinWithMultipleKeys");

        expectInAnyOrder("key0:0:6", "key1:1:7", "key2:2:4", "key2:2:5", "key2:3:4", "key2:3:5");
    }

    @Test
    void testInnerJoinWhenLeftInputNoData() throws Exception {
        NonKeyedPartitionStream<KeyAndValue> source1 =
                getSourceStream(
                        Arrays.asList(
                                KeyAndValue.of("key0", 0),
                                KeyAndValue.of("key1", 1),
                                KeyAndValue.of("key2", 2),
                                KeyAndValue.of("key2", 3)),
                        "source1");
        NonKeyedPartitionStream<KeyAndValue> source2 =
                getSourceStream(new ArrayList<>(), "source2");

        NonKeyedPartitionStream<String> joinedStream =
                BuiltinFuncs.join(
                        source1,
                        (KeySelector<KeyAndValue, String>) keyAndValue -> keyAndValue.key,
                        source2,
                        (KeySelector<KeyAndValue, String>) keyAndValue -> keyAndValue.key,
                        new TestJoinFunction(),
                        JoinType.INNER);

        joinedStream.toSink(new WrappedSink<>(new TestSink()));
        env.execute("testInnerJoinWhenLeftInputNoData");

        expectInAnyOrder();
    }

    @Test
    void testInnerJoinWhenRightInputNoData() throws Exception {
        NonKeyedPartitionStream<KeyAndValue> source1 =
                getSourceStream(new ArrayList<>(), "source1");
        NonKeyedPartitionStream<KeyAndValue> source2 =
                getSourceStream(
                        Arrays.asList(
                                KeyAndValue.of("key0", 0),
                                KeyAndValue.of("key1", 1),
                                KeyAndValue.of("key2", 2),
                                KeyAndValue.of("key2", 3)),
                        "source2");

        NonKeyedPartitionStream<String> joinedStream =
                BuiltinFuncs.join(
                        source1,
                        (KeySelector<KeyAndValue, String>) keyAndValue -> keyAndValue.key,
                        source2,
                        (KeySelector<KeyAndValue, String>) keyAndValue -> keyAndValue.key,
                        new TestJoinFunction(),
                        JoinType.INNER);

        joinedStream.toSink(new WrappedSink<>(new TestSink()));
        env.execute("testInnerJoinWhenRightInputNoData");

        expectInAnyOrder();
    }

    /** Test Join using {@link BuiltinFuncs#join(JoinFunction)}. */
    @Test
    void testJoinWithWrappedJoinProcessFunction() throws Exception {
        NonKeyedPartitionStream<KeyAndValue> source1 =
                getSourceStream(
                        Arrays.asList(
                                KeyAndValue.of("key", 0),
                                KeyAndValue.of("key", 1),
                                KeyAndValue.of("key", 2)),
                        "source1");
        NonKeyedPartitionStream<KeyAndValue> source2 =
                getSourceStream(
                        Arrays.asList(
                                KeyAndValue.of("key", 0),
                                KeyAndValue.of("key", 1),
                                KeyAndValue.of("key", 2)),
                        "source2");

        KeyedPartitionStream<String, KeyAndValue> keyedStream1 =
                source1.keyBy((KeySelector<KeyAndValue, String>) keyAndValue -> keyAndValue.key);
        KeyedPartitionStream<String, KeyAndValue> keyedStream2 =
                source2.keyBy((KeySelector<KeyAndValue, String>) keyAndValue -> keyAndValue.key);
        TwoInputNonBroadcastStreamProcessFunction<KeyAndValue, KeyAndValue, String>
                wrappedJoinProcessFunction = BuiltinFuncs.join(new TestJoinFunction());
        NonKeyedPartitionStream<String> joinedStream =
                keyedStream1.connectAndProcess(keyedStream2, wrappedJoinProcessFunction);

        joinedStream.toSink(new WrappedSink<>(new TestSink()));
        env.execute("testInnerJoinWithSameKey");

        expectInAnyOrder(
                "key:0:0", "key:0:1", "key:0:2", "key:1:0", "key:1:1", "key:1:2", "key:2:0",
                "key:2:1", "key:2:2");
    }

    /**
     * Test Join using {@link BuiltinFuncs#join(KeyedPartitionStream, KeyedPartitionStream,
     * JoinFunction)}.
     */
    @Test
    void testJoinWithKeyedStream() throws Exception {
        NonKeyedPartitionStream<KeyAndValue> source1 =
                getSourceStream(
                        Arrays.asList(
                                KeyAndValue.of("key", 0),
                                KeyAndValue.of("key", 1),
                                KeyAndValue.of("key", 2)),
                        "source1");
        NonKeyedPartitionStream<KeyAndValue> source2 =
                getSourceStream(
                        Arrays.asList(
                                KeyAndValue.of("key", 0),
                                KeyAndValue.of("key", 1),
                                KeyAndValue.of("key", 2)),
                        "source2");

        KeyedPartitionStream<String, KeyAndValue> keyedStream1 =
                source1.keyBy((KeySelector<KeyAndValue, String>) keyAndValue -> keyAndValue.key);
        KeyedPartitionStream<String, KeyAndValue> keyedStream2 =
                source2.keyBy((KeySelector<KeyAndValue, String>) keyAndValue -> keyAndValue.key);

        NonKeyedPartitionStream<String> joinedStream =
                BuiltinFuncs.join(keyedStream1, keyedStream2, new TestJoinFunction());

        joinedStream.toSink(new WrappedSink<>(new TestSink()));
        env.execute("testInnerJoinWithSameKey");

        expectInAnyOrder(
                "key:0:0", "key:0:1", "key:0:2", "key:1:0", "key:1:1", "key:1:2", "key:2:0",
                "key:2:1", "key:2:2");
    }

    /**
     * Test Join using {@link BuiltinFuncs#join(NonKeyedPartitionStream, KeySelector,
     * NonKeyedPartitionStream, KeySelector, JoinFunction)}.
     */
    @Test
    void testJoinWithNonKeyedStream() throws Exception {
        NonKeyedPartitionStream<KeyAndValue> source1 =
                getSourceStream(
                        Arrays.asList(
                                KeyAndValue.of("key", 0),
                                KeyAndValue.of("key", 1),
                                KeyAndValue.of("key", 2)),
                        "source1");
        NonKeyedPartitionStream<KeyAndValue> source2 =
                getSourceStream(
                        Arrays.asList(
                                KeyAndValue.of("key", 0),
                                KeyAndValue.of("key", 1),
                                KeyAndValue.of("key", 2)),
                        "source2");

        NonKeyedPartitionStream<String> joinedStream =
                BuiltinFuncs.join(
                        source1,
                        (KeySelector<KeyAndValue, String>) keyAndValue -> keyAndValue.key,
                        source2,
                        (KeySelector<KeyAndValue, String>) keyAndValue -> keyAndValue.key,
                        new TestJoinFunction());

        joinedStream.toSink(new WrappedSink<>(new TestSink()));
        env.execute("testInnerJoinWithSameKey");

        expectInAnyOrder(
                "key:0:0", "key:0:1", "key:0:2", "key:1:0", "key:1:1", "key:1:2", "key:2:0",
                "key:2:1", "key:2:2");
    }

    @Test
    void testJoinWithTuple() throws Exception {
        final String resultPath = getTempDirPath("result");

        NonKeyedPartitionStream<Tuple2<String, Integer>> source1 =
                env.fromSource(
                        DataStreamV2SourceUtils.fromData(
                                Arrays.asList(
                                        Tuple2.of("key", 0),
                                        Tuple2.of("key", 1),
                                        Tuple2.of("key", 2))),
                        "source1");

        NonKeyedPartitionStream<Tuple2<String, Integer>> source2 =
                env.fromSource(
                        DataStreamV2SourceUtils.fromData(
                                Arrays.asList(
                                        Tuple2.of("key", 0),
                                        Tuple2.of("key", 1),
                                        Tuple2.of("key", 2))),
                        "source2");

        NonKeyedPartitionStream<Tuple3<String, Integer, Integer>> joinedStream =
                BuiltinFuncs.join(
                        source1,
                        (KeySelector<Tuple2<String, Integer>, String>) elem -> elem.f0,
                        source2,
                        (KeySelector<Tuple2<String, Integer>, String>) elem -> elem.f0,
                        new JoinFunction<
                                Tuple2<String, Integer>,
                                Tuple2<String, Integer>,
                                Tuple3<String, Integer, Integer>>() {

                            @Override
                            public void processRecord(
                                    Tuple2<String, Integer> leftRecord,
                                    Tuple2<String, Integer> rightRecord,
                                    Collector<Tuple3<String, Integer, Integer>> output,
                                    RuntimeContext ctx)
                                    throws Exception {
                                output.collect(
                                        Tuple3.of(leftRecord.f0, leftRecord.f1, rightRecord.f1));
                            }
                        });

        joinedStream.toSink(
                new WrappedSink<>(
                        FileSink.<Tuple3<String, Integer, Integer>>forRowFormat(
                                        new Path(resultPath), new SimpleStringEncoder<>())
                                .withRollingPolicy(
                                        DefaultRollingPolicy.builder()
                                                .withMaxPartSize(MemorySize.ofMebiBytes(1))
                                                .withRolloverInterval(Duration.ofSeconds(10))
                                                .build())
                                .build()));

        env.execute("testJoinWithTuple");

        compareResultsByLinesInMemory(
                "(key,0,0)\n"
                        + "(key,0,1)\n"
                        + "(key,0,2)\n"
                        + "(key,1,0)\n"
                        + "(key,1,1)\n"
                        + "(key,1,2)\n"
                        + "(key,2,0)\n"
                        + "(key,2,1)\n"
                        + "(key,2,2)\n",
                resultPath);
    }

    private static class KeyAndValue {
        public final String key;
        public final int value;

        public KeyAndValue(String key, int value) {
            this.key = key;
            this.value = value;
        }

        public static KeyAndValue of(String key, int value) {
            return new KeyAndValue(key, value);
        }
    }

    private NonKeyedPartitionStream<KeyAndValue> getSourceStream(
            Collection<KeyAndValue> data, String sourceName) {
        if (data.isEmpty()) {
            return getEmptySourceStream(data, sourceName);
        }
        return env.fromSource(DataStreamV2SourceUtils.fromData(data), sourceName);
    }

    private NonKeyedPartitionStream<KeyAndValue> getEmptySourceStream(
            Collection<KeyAndValue> data, String sourceName) {
        // Since env#fromSource fails when the data is empty, we add a dummy record and then filter
        // it out.
        data.add(KeyAndValue.of("", -1));
        NonKeyedPartitionStream<KeyAndValue> source =
                env.fromSource(DataStreamV2SourceUtils.fromData(data), sourceName);
        return source.process(
                new OneInputStreamProcessFunction<KeyAndValue, KeyAndValue>() {
                    @Override
                    public void processRecord(
                            KeyAndValue record,
                            Collector<KeyAndValue> output,
                            PartitionedContext<KeyAndValue> ctx)
                            throws Exception {}
                });
    }

    private static class TestJoinFunction
            implements JoinFunction<KeyAndValue, KeyAndValue, String> {

        @Override
        public void processRecord(
                KeyAndValue leftRecord,
                KeyAndValue rightRecord,
                Collector<String> output,
                RuntimeContext ctx)
                throws Exception {
            assertThat(leftRecord.key).isNotNull();
            assertThat(leftRecord.key).isEqualTo(rightRecord.key);
            String result = leftRecord.key + ":" + leftRecord.value + ":" + rightRecord.value;
            output.collect(result);
        }
    }

    private static class TestSink implements Sink<String> {

        @Override
        public SinkWriter<String> createWriter(WriterInitContext context) throws IOException {
            return new TestSinkWriter();
        }
    }

    private static class TestSinkWriter implements SinkWriter<String> {
        @Override
        public void write(String element, Context context)
                throws IOException, InterruptedException {
            sinkResults.add(element);
        }

        @Override
        public void flush(boolean endOfInput) throws IOException, InterruptedException {}

        @Override
        public void close() throws Exception {}
    }

    private static void expectInAnyOrder(String... expected) {
        List<String> listExpected = Arrays.asList(expected);
        Collections.sort(listExpected);
        Collections.sort(sinkResults);
        assertThat(listExpected).isEqualTo(sinkResults);
    }
}
