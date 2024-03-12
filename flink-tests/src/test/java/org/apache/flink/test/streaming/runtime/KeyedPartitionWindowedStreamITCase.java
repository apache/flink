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

package org.apache.flink.test.streaming.runtime;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedPartitionWindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;

import org.apache.flink.shaded.guava31.com.google.common.collect.Lists;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Integration tests for {@link KeyedPartitionWindowedStream}. */
class KeyedPartitionWindowedStreamITCase {

    private static final int EVENT_NUMBER = 100;

    private static final String TEST_EVENT = "Test";

    @Test
    void testMapPartition() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<String, String>> source = env.fromData(createSource());
        CloseableIterator<String> resultIterator =
                source.map(
                                new MapFunction<Tuple2<String, String>, Tuple2<String, String>>() {
                                    @Override
                                    public Tuple2<String, String> map(Tuple2<String, String> value)
                                            throws Exception {
                                        return value;
                                    }
                                })
                        .setParallelism(2)
                        .keyBy(
                                new KeySelector<Tuple2<String, String>, String>() {
                                    @Override
                                    public String getKey(Tuple2<String, String> value)
                                            throws Exception {
                                        return value.f0;
                                    }
                                })
                        .fullWindowPartition()
                        .mapPartition(
                                new MapPartitionFunction<Tuple2<String, String>, String>() {
                                    @Override
                                    public void mapPartition(
                                            Iterable<Tuple2<String, String>> values,
                                            Collector<String> out)
                                            throws Exception {
                                        StringBuilder sb = new StringBuilder();
                                        for (Tuple2<String, String> value : values) {
                                            sb.append(value.f0);
                                            sb.append(value.f1);
                                        }
                                        out.collect(sb.toString());
                                    }
                                })
                        .executeAndCollect();
        expectInAnyOrder(
                resultIterator,
                createExpectedString(1),
                createExpectedString(2),
                createExpectedString(3));
    }

    @Test
    void testReduce() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<String, Integer>> source =
                env.fromData(
                        Tuple2.of("key1", 1),
                        Tuple2.of("key1", 999),
                        Tuple2.of("key2", 2),
                        Tuple2.of("key2", 998),
                        Tuple2.of("key3", 3),
                        Tuple2.of("key3", 997));
        CloseableIterator<String> resultIterator =
                source.map(
                                new MapFunction<
                                        Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                                    @Override
                                    public Tuple2<String, Integer> map(
                                            Tuple2<String, Integer> value) throws Exception {
                                        return value;
                                    }
                                })
                        .setParallelism(2)
                        .keyBy(
                                new KeySelector<Tuple2<String, Integer>, String>() {
                                    @Override
                                    public String getKey(Tuple2<String, Integer> value)
                                            throws Exception {
                                        return value.f0;
                                    }
                                })
                        .fullWindowPartition()
                        .reduce(
                                new ReduceFunction<Tuple2<String, Integer>>() {
                                    @Override
                                    public Tuple2<String, Integer> reduce(
                                            Tuple2<String, Integer> value1,
                                            Tuple2<String, Integer> value2)
                                            throws Exception {
                                        return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                                    }
                                })
                        .map(
                                new MapFunction<Tuple2<String, Integer>, String>() {
                                    @Override
                                    public String map(Tuple2<String, Integer> value)
                                            throws Exception {
                                        return value.f0 + value.f1;
                                    }
                                })
                        .executeAndCollect();
        expectInAnyOrder(resultIterator, "key11000", "key21000", "key31000");
    }

    @Test
    void testAggregate() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<String, Integer>> source =
                env.fromData(
                        Tuple2.of("Key1", 1),
                        Tuple2.of("Key1", 2),
                        Tuple2.of("Key2", 2),
                        Tuple2.of("Key2", 1),
                        Tuple2.of("Key3", 1),
                        Tuple2.of("Key3", 1),
                        Tuple2.of("Key3", 1));
        CloseableIterator<String> resultIterator =
                source.map(
                                new MapFunction<
                                        Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                                    @Override
                                    public Tuple2<String, Integer> map(
                                            Tuple2<String, Integer> value) throws Exception {
                                        return value;
                                    }
                                })
                        .setParallelism(2)
                        .keyBy(
                                new KeySelector<Tuple2<String, Integer>, String>() {
                                    @Override
                                    public String getKey(Tuple2<String, Integer> value)
                                            throws Exception {
                                        return value.f0;
                                    }
                                })
                        .fullWindowPartition()
                        .aggregate(
                                new AggregateFunction<
                                        Tuple2<String, Integer>, TestAccumulator, String>() {
                                    @Override
                                    public TestAccumulator createAccumulator() {
                                        return new TestAccumulator();
                                    }

                                    @Override
                                    public TestAccumulator add(
                                            Tuple2<String, Integer> value,
                                            TestAccumulator accumulator) {
                                        accumulator.addTestField(value.f1);
                                        return accumulator;
                                    }

                                    @Override
                                    public String getResult(TestAccumulator accumulator) {
                                        return accumulator.getTestField();
                                    }

                                    @Override
                                    public TestAccumulator merge(
                                            TestAccumulator a, TestAccumulator b) {
                                        throw new RuntimeException();
                                    }
                                })
                        .executeAndCollect();
        expectInAnyOrder(resultIterator, "97", "97", "97");
    }

    @Test
    void testSortPartitionOfTupleElementsAscending() throws Exception {
        expectInAnyOrder(
                sortPartitionOfTupleElementsInOrder(Order.ASCENDING),
                "0 1 3 7 ",
                "0 1 79 100 ",
                "8 55 66 77 ");
    }

    @Test
    void testSortPartitionOfTupleElementsDescending() throws Exception {
        expectInAnyOrder(
                sortPartitionOfTupleElementsInOrder(Order.DESCENDING),
                "7 3 1 0 ",
                "100 79 1 0 ",
                "77 66 55 8 ");
    }

    @Test
    void testSortPartitionOfPojoElementsAscending() throws Exception {
        expectInAnyOrder(
                sortPartitionOfPojoElementsInOrder(Order.ASCENDING),
                "0 1 3 7 ",
                "0 1 79 100 ",
                "8 55 66 77 ");
    }

    @Test
    void testSortPartitionOfPojoElementsDescending() throws Exception {
        expectInAnyOrder(
                sortPartitionOfPojoElementsInOrder(Order.DESCENDING),
                "7 3 1 0 ",
                "100 79 1 0 ",
                "77 66 55 8 ");
    }

    @Test
    public void testSortPartitionByKeySelectorAscending() throws Exception {
        expectInAnyOrder(
                sortPartitionByKeySelectorInOrder(Order.ASCENDING),
                "0 1 3 7 ",
                "0 1 79 100 ",
                "8 55 66 77 ");
    }

    @Test
    void testSortPartitionByKeySelectorDescending() throws Exception {
        expectInAnyOrder(
                sortPartitionByKeySelectorInOrder(Order.DESCENDING),
                "7 3 1 0 ",
                "100 79 1 0 ",
                "77 66 55 8 ");
    }

    private CloseableIterator<String> sortPartitionOfTupleElementsInOrder(Order order)
            throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<String, Integer>> source =
                env.fromData(
                        Tuple2.of("key1", 0),
                        Tuple2.of("key1", 7),
                        Tuple2.of("key1", 3),
                        Tuple2.of("key1", 1),
                        Tuple2.of("key2", 1),
                        Tuple2.of("key2", 100),
                        Tuple2.of("key2", 0),
                        Tuple2.of("key2", 79),
                        Tuple2.of("key3", 77),
                        Tuple2.of("key3", 66),
                        Tuple2.of("key3", 55),
                        Tuple2.of("key3", 8));
        return source.map(
                        new MapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                            @Override
                            public Tuple2<String, Integer> map(Tuple2<String, Integer> value)
                                    throws Exception {
                                return value;
                            }
                        })
                .setParallelism(2)
                .keyBy(
                        new KeySelector<Tuple2<String, Integer>, String>() {
                            @Override
                            public String getKey(Tuple2<String, Integer> value) throws Exception {
                                return value.f0;
                            }
                        })
                .fullWindowPartition()
                .sortPartition(1, order)
                .fullWindowPartition()
                .mapPartition(
                        new MapPartitionFunction<Tuple2<String, Integer>, String>() {
                            @Override
                            public void mapPartition(
                                    Iterable<Tuple2<String, Integer>> values,
                                    Collector<String> out) {
                                StringBuilder sb = new StringBuilder();
                                String preKey = null;
                                for (Tuple2<String, Integer> value : values) {
                                    if (preKey != null && !preKey.equals(value.f0)) {
                                        out.collect(sb.toString());
                                        sb = new StringBuilder();
                                    }
                                    sb.append(value.f1);
                                    sb.append(" ");
                                    preKey = value.f0;
                                }
                                out.collect(sb.toString());
                            }
                        })
                .executeAndCollect();
    }

    private CloseableIterator<String> sortPartitionOfPojoElementsInOrder(Order order)
            throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<TestPojo> source =
                env.fromData(
                        new TestPojo("key1", 0),
                        new TestPojo("key1", 7),
                        new TestPojo("key1", 3),
                        new TestPojo("key1", 1),
                        new TestPojo("key2", 1),
                        new TestPojo("key2", 100),
                        new TestPojo("key2", 0),
                        new TestPojo("key2", 79),
                        new TestPojo("key3", 77),
                        new TestPojo("key3", 66),
                        new TestPojo("key3", 55),
                        new TestPojo("key3", 8));
        return source.map(
                        new MapFunction<TestPojo, TestPojo>() {
                            @Override
                            public TestPojo map(TestPojo value) throws Exception {
                                return value;
                            }
                        })
                .setParallelism(2)
                .keyBy(
                        new KeySelector<TestPojo, String>() {
                            @Override
                            public String getKey(TestPojo value) throws Exception {
                                return value.getKey();
                            }
                        })
                .fullWindowPartition()
                .sortPartition("value", order)
                .fullWindowPartition()
                .mapPartition(
                        new MapPartitionFunction<TestPojo, String>() {
                            @Override
                            public void mapPartition(
                                    Iterable<TestPojo> values, Collector<String> out) {
                                StringBuilder sb = new StringBuilder();
                                String preKey = null;
                                for (TestPojo value : values) {
                                    if (preKey != null && !preKey.equals(value.getKey())) {
                                        out.collect(sb.toString());
                                        sb = new StringBuilder();
                                    }
                                    sb.append(value.getValue());
                                    sb.append(" ");
                                    preKey = value.getKey();
                                }
                                out.collect(sb.toString());
                            }
                        })
                .executeAndCollect();
    }

    private CloseableIterator<String> sortPartitionByKeySelectorInOrder(Order order)
            throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<TestPojo> source =
                env.fromData(
                        new TestPojo("key1", 0),
                        new TestPojo("key1", 7),
                        new TestPojo("key1", 3),
                        new TestPojo("key1", 1),
                        new TestPojo("key2", 1),
                        new TestPojo("key2", 100),
                        new TestPojo("key2", 0),
                        new TestPojo("key2", 79),
                        new TestPojo("key3", 77),
                        new TestPojo("key3", 66),
                        new TestPojo("key3", 55),
                        new TestPojo("key3", 8));
        return source.map(
                        new MapFunction<TestPojo, TestPojo>() {
                            @Override
                            public TestPojo map(TestPojo value) throws Exception {
                                return value;
                            }
                        })
                .setParallelism(2)
                .keyBy(
                        new KeySelector<TestPojo, String>() {
                            @Override
                            public String getKey(TestPojo value) throws Exception {
                                return value.getKey();
                            }
                        })
                .fullWindowPartition()
                .sortPartition(
                        new KeySelector<TestPojo, Integer>() {
                            @Override
                            public Integer getKey(TestPojo value) throws Exception {
                                return value.getValue();
                            }
                        },
                        order)
                .fullWindowPartition()
                .mapPartition(
                        new MapPartitionFunction<TestPojo, String>() {
                            @Override
                            public void mapPartition(
                                    Iterable<TestPojo> values, Collector<String> out) {
                                StringBuilder sb = new StringBuilder();
                                String preKey = null;
                                for (TestPojo value : values) {
                                    if (preKey != null && !preKey.equals(value.getKey())) {
                                        out.collect(sb.toString());
                                        sb = new StringBuilder();
                                    }
                                    sb.append(value.getValue());
                                    sb.append(" ");
                                    preKey = value.getKey();
                                }
                                out.collect(sb.toString());
                            }
                        })
                .executeAndCollect();
    }

    private Collection<Tuple2<String, String>> createSource() {
        List<Tuple2<String, String>> source = new ArrayList<>();
        for (int index = 0; index < EVENT_NUMBER; ++index) {
            source.add(Tuple2.of("k1", TEST_EVENT));
            source.add(Tuple2.of("k2", TEST_EVENT));
            source.add(Tuple2.of("k3", TEST_EVENT));
        }
        return source;
    }

    private String createExpectedString(int key) {
        StringBuilder stringBuilder = new StringBuilder();
        for (int index = 0; index < EVENT_NUMBER; ++index) {
            stringBuilder.append("k").append(key).append(TEST_EVENT);
        }
        return stringBuilder.toString();
    }

    private void expectInAnyOrder(CloseableIterator<String> resultIterator, String... expected) {
        List<String> listExpected = Lists.newArrayList(expected);
        List<String> testResults = Lists.newArrayList(resultIterator);
        Collections.sort(listExpected);
        Collections.sort(testResults);
        assertThat(testResults).isEqualTo(listExpected);
    }

    /** The test accumulator. */
    private static class TestAccumulator {
        private Integer testField = 100;

        public void addTestField(Integer number) {
            testField = testField - number;
        }

        public String getTestField() {
            return String.valueOf(testField);
        }
    }

    /** The test pojo. */
    public static class TestPojo {

        public String key;

        public Integer value;

        public TestPojo() {}

        public TestPojo(Integer value) {
            this.value = value;
        }

        public TestPojo(String key, Integer value) {
            this.key = key;
            this.value = value;
        }

        public Integer getValue() {
            return value;
        }

        public void setValue(Integer value) {
            this.value = value;
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }
    }
}
