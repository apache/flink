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

package org.apache.flink.test.streaming.runtime;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.util.Collector;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Integration test for streaming programs using POJOs and key selectors.
 *
 * <p>See FLINK-3697
 */
public class DataStreamPojoITCase extends AbstractTestBase {
    static List<Data> elements = new ArrayList<>();

    static {
        elements.add(new Data(0, 0, 0));
        elements.add(new Data(0, 0, 0));
        elements.add(new Data(1, 1, 1));
        elements.add(new Data(1, 1, 1));
        elements.add(new Data(2, 2, 3));
        elements.add(new Data(2, 2, 3));
    }

    /** Test composite key on the Data POJO (with nested fields). */
    @Test
    public void testCompositeKeyOnNestedPojo() throws Exception {
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.getConfig().disableObjectReuse();
        see.setParallelism(3);

        DataStream<Data> dataStream = see.fromCollection(elements);

        DataStream<Data> summedStream =
                dataStream
                        .keyBy("aaa", "abc", "wxyz")
                        .sum("sum")
                        .keyBy("aaa", "abc", "wxyz")
                        .flatMap(
                                new FlatMapFunction<Data, Data>() {
                                    private static final long serialVersionUID =
                                            788865239171396315L;
                                    Data[] first = new Data[3];

                                    @Override
                                    public void flatMap(Data value, Collector<Data> out)
                                            throws Exception {
                                        if (first[value.aaa] == null) {
                                            first[value.aaa] = value;
                                            if (value.sum != 1) {
                                                throw new RuntimeException(
                                                        "Expected the sum to be one");
                                            }
                                        } else {
                                            if (value.sum != 2) {
                                                throw new RuntimeException(
                                                        "Expected the sum to be two");
                                            }
                                            if (first[value.aaa].aaa != value.aaa) {
                                                throw new RuntimeException("aaa key wrong");
                                            }
                                            if (first[value.aaa].abc != value.abc) {
                                                throw new RuntimeException("abc key wrong");
                                            }
                                            if (first[value.aaa].wxyz != value.wxyz) {
                                                throw new RuntimeException("wxyz key wrong");
                                            }
                                        }
                                    }
                                });

        summedStream.print();

        see.execute();
    }

    /** Test composite & nested key on the Data POJO. */
    @Test
    public void testNestedKeyOnNestedPojo() throws Exception {
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.getConfig().disableObjectReuse();
        see.setParallelism(4);

        DataStream<Data> dataStream = see.fromCollection(elements);

        DataStream<Data> summedStream =
                dataStream
                        .keyBy("aaa", "stats.count")
                        .sum("sum")
                        .keyBy("aaa", "stats.count")
                        .flatMap(
                                new FlatMapFunction<Data, Data>() {
                                    private static final long serialVersionUID =
                                            -3678267280397950258L;
                                    Data[] first = new Data[3];

                                    @Override
                                    public void flatMap(Data value, Collector<Data> out)
                                            throws Exception {
                                        if (value.stats.count != 123) {
                                            throw new RuntimeException(
                                                    "Wrong value for value.stats.count");
                                        }
                                        if (first[value.aaa] == null) {
                                            first[value.aaa] = value;
                                            if (value.sum != 1) {
                                                throw new RuntimeException(
                                                        "Expected the sum to be one");
                                            }
                                        } else {
                                            if (value.sum != 2) {
                                                throw new RuntimeException(
                                                        "Expected the sum to be two");
                                            }
                                            if (first[value.aaa].aaa != value.aaa) {
                                                throw new RuntimeException("aaa key wrong");
                                            }
                                            if (first[value.aaa].abc != value.abc) {
                                                throw new RuntimeException("abc key wrong");
                                            }
                                            if (first[value.aaa].wxyz != value.wxyz) {
                                                throw new RuntimeException("wxyz key wrong");
                                            }
                                        }
                                    }
                                });

        summedStream.print();

        see.execute();
    }

    @Test
    public void testNestedPojoFieldAccessor() throws Exception {
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.getConfig().disableObjectReuse();
        see.setParallelism(4);

        DataStream<Data> dataStream = see.fromCollection(elements);

        DataStream<Data> summedStream =
                dataStream
                        .keyBy("aaa")
                        .sum("stats.count")
                        .keyBy("aaa")
                        .flatMap(
                                new FlatMapFunction<Data, Data>() {
                                    Data[] first = new Data[3];

                                    @Override
                                    public void flatMap(Data value, Collector<Data> out)
                                            throws Exception {
                                        if (first[value.aaa] == null) {
                                            first[value.aaa] = value;
                                            if (value.stats.count != 123) {
                                                throw new RuntimeException(
                                                        "Expected stats.count to be 123");
                                            }
                                        } else {
                                            if (value.stats.count != 2 * 123) {
                                                throw new RuntimeException(
                                                        "Expected stats.count to be 2 * 123");
                                            }
                                        }
                                    }
                                });

        summedStream.print();

        see.execute();
    }

    @Test(expected = CompositeType.InvalidFieldReferenceException.class)
    public void testFailOnNestedPojoFieldAccessor() throws Exception {
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Data> dataStream = see.fromCollection(elements);
        dataStream.keyBy("aaa", "stats.count").sum("stats.nonExistingField");
    }

    /** POJO. */
    public static class Data {
        public int sum; // sum
        public int aaa; // keyBy
        public int abc; // keyBy
        public long wxyz; // keyBy
        public int t1;
        public int t2;
        public Policy policy;
        public Stats stats;

        public Data() {}

        public Data(int aaa, int abc, int wxyz) {
            this.sum = 1;
            this.aaa = aaa;
            this.abc = abc;
            this.wxyz = wxyz;
            this.stats = new Stats();
            this.stats.count = 123L;
        }

        @Override
        public String toString() {
            return "Data{" + "sum=" + sum + ", aaa=" + aaa + ", abc=" + abc + ", wxyz=" + wxyz
                    + '}';
        }
    }

    /** POJO. */
    public static class Policy {
        public short a;
        public short b;
        public boolean c;
        public boolean d;

        public Policy() {}
    }

    /** POJO. */
    public static class Stats {
        public long count;
        public float a;
        public float b;
        public float c;
        public float d;
        public float e;

        public Stats() {}
    }
}
