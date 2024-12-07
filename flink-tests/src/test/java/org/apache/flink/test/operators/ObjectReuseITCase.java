/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.operators;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.test.util.MultipleProgramsTestBaseJUnit4;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;

/** These check whether the object-reuse execution mode does really reuse objects. */
@SuppressWarnings("serial")
@RunWith(Parameterized.class)
public class ObjectReuseITCase extends MultipleProgramsTestBaseJUnit4 {

    private static final List<Tuple2<String, Integer>> REDUCE_DATA =
            Arrays.asList(
                    new Tuple2<>("a", 1),
                    new Tuple2<>("a", 2),
                    new Tuple2<>("a", 3),
                    new Tuple2<>("a", 4),
                    new Tuple2<>("a", 50));

    private static final List<Tuple2<String, Integer>> GROUP_REDUCE_DATA =
            Arrays.asList(
                    new Tuple2<>("a", 1),
                    new Tuple2<>("a", 2),
                    new Tuple2<>("a", 3),
                    new Tuple2<>("a", 4),
                    new Tuple2<>("a", 5));

    private final boolean objectReuse;

    public ObjectReuseITCase(boolean objectReuse) {
        super(TestExecutionMode.CLUSTER);
        this.objectReuse = objectReuse;
    }

    @Test
    public void testKeyedReduce() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        if (objectReuse) {
            env.getConfig().enableObjectReuse();
        } else {
            env.getConfig().disableObjectReuse();
        }

        DataStreamSource<Tuple2<String, Integer>> input = env.fromData(REDUCE_DATA);

        DataStream<Tuple2<String, Integer>> result =
                input.keyBy(x -> x.f0)
                        .reduce(
                                new ReduceFunction<Tuple2<String, Integer>>() {

                                    @Override
                                    public Tuple2<String, Integer> reduce(
                                            Tuple2<String, Integer> value1,
                                            Tuple2<String, Integer> value2) {
                                        value2.f1 += value1.f1;
                                        return value2;
                                    }
                                });

        Tuple2<String, Integer> res = result.executeAndCollect().next();
        assertEquals(new Tuple2<>("a", 60), res);
    }

    @Test
    public void testGlobalReduce() throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        if (objectReuse) {
            env.getConfig().enableObjectReuse();
        } else {
            env.getConfig().disableObjectReuse();
        }

        DataStreamSource<Tuple2<String, Integer>> input = env.fromData(REDUCE_DATA);

        DataStream<Tuple2<String, Integer>> result =
                input.windowAll(GlobalWindows.createWithEndOfStreamTrigger())
                        .reduce(
                                new ReduceFunction<Tuple2<String, Integer>>() {

                                    @Override
                                    public Tuple2<String, Integer> reduce(
                                            Tuple2<String, Integer> value1,
                                            Tuple2<String, Integer> value2) {

                                        if (value1.f1 % 3 == 0) {
                                            value1.f1 += value2.f1;
                                            return value1;
                                        } else {
                                            value2.f1 += value1.f1;
                                            return value2;
                                        }
                                    }
                                });

        Tuple2<String, Integer> res = result.executeAndCollect().next();
        assertEquals(new Tuple2<>("a", 60), res);
    }

    @Parameterized.Parameters(name = "Execution mode = CLUSTER, Reuse = {0}")
    public static Collection<Object[]> executionModes() {
        return Arrays.asList(
                new Object[] {
                    false,
                },
                new Object[] {true});
    }
}
