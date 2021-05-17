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

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.test.streaming.runtime.util.TestListResultSink;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.util.Collector;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

/** Integration tests for co-streams. */
@SuppressWarnings("serial")
public class CoStreamITCase extends AbstractTestBase {

    @Test
    public void test() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        TestListResultSink<String> resultSink = new TestListResultSink<String>();

        DataStream<Integer> src = env.fromElements(1, 3, 5);

        DataStream<Integer> filter1 =
                src.filter(
                                new FilterFunction<Integer>() {
                                    @Override
                                    public boolean filter(Integer value) throws Exception {
                                        return true;
                                    }
                                })
                        .keyBy(
                                new KeySelector<Integer, Integer>() {
                                    @Override
                                    public Integer getKey(Integer value) throws Exception {
                                        return value;
                                    }
                                });

        DataStream<Tuple2<Integer, Integer>> filter2 =
                src.map(
                                new MapFunction<Integer, Tuple2<Integer, Integer>>() {

                                    @Override
                                    public Tuple2<Integer, Integer> map(Integer value)
                                            throws Exception {
                                        return new Tuple2<>(value, value + 1);
                                    }
                                })
                        .rebalance()
                        .filter(
                                new FilterFunction<Tuple2<Integer, Integer>>() {

                                    @Override
                                    public boolean filter(Tuple2<Integer, Integer> value)
                                            throws Exception {
                                        return true;
                                    }
                                })
                        .disableChaining()
                        .keyBy(
                                new KeySelector<Tuple2<Integer, Integer>, Integer>() {

                                    @Override
                                    public Integer getKey(Tuple2<Integer, Integer> value)
                                            throws Exception {
                                        return value.f0;
                                    }
                                });

        DataStream<String> connected =
                filter1.connect(filter2)
                        .flatMap(
                                new CoFlatMapFunction<Integer, Tuple2<Integer, Integer>, String>() {

                                    @Override
                                    public void flatMap1(Integer value, Collector<String> out)
                                            throws Exception {
                                        out.collect(value.toString());
                                    }

                                    @Override
                                    public void flatMap2(
                                            Tuple2<Integer, Integer> value, Collector<String> out)
                                            throws Exception {
                                        out.collect(value.toString());
                                    }
                                });

        connected.addSink(resultSink);

        env.execute();

        List<String> expected = Arrays.asList("(1,2)", "(3,4)", "(5,6)", "1", "3", "5");
        List<String> result = resultSink.getResult();
        Collections.sort(result);
        assertEquals(expected, result);
    }
}
