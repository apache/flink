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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.streaming.runtime.util.NoOpIntMap;
import org.apache.flink.test.streaming.runtime.util.TestListResultSink;
import org.apache.flink.test.util.AbstractTestBase;

import org.junit.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.sort;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/** IT case that tests the different stream partitioning schemes. */
@SuppressWarnings("serial")
public class PartitionerITCase extends AbstractTestBase {

    private static final int PARALLELISM = 3;

    private static final List<String> INPUT = Arrays.asList("a", "b", "c", "d", "e", "f", "g");

    @Test(expected = UnsupportedOperationException.class)
    public void testForwardFailsLowToHighParallelism() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Integer> src = env.fromElements(1, 2, 3);

        // this doesn't work because it goes from 1 to 3
        src.forward().map(new NoOpIntMap());

        env.execute();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testForwardFailsHightToLowParallelism() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // this does a rebalance that works
        DataStream<Integer> src = env.fromElements(1, 2, 3).map(new NoOpIntMap());

        // this doesn't work because it goes from 3 to 1
        src.forward().map(new NoOpIntMap()).setParallelism(1);

        env.execute();
    }

    @Test
    public void partitionerTest() {

        TestListResultSink<Tuple2<Integer, String>> hashPartitionResultSink =
                new TestListResultSink<Tuple2<Integer, String>>();
        TestListResultSink<Tuple2<Integer, String>> customPartitionResultSink =
                new TestListResultSink<Tuple2<Integer, String>>();
        TestListResultSink<Tuple2<Integer, String>> broadcastPartitionResultSink =
                new TestListResultSink<Tuple2<Integer, String>>();
        TestListResultSink<Tuple2<Integer, String>> forwardPartitionResultSink =
                new TestListResultSink<Tuple2<Integer, String>>();
        TestListResultSink<Tuple2<Integer, String>> rebalancePartitionResultSink =
                new TestListResultSink<Tuple2<Integer, String>>();
        TestListResultSink<Tuple2<Integer, String>> globalPartitionResultSink =
                new TestListResultSink<Tuple2<Integer, String>>();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);

        DataStream<Tuple1<String>> src =
                env.fromCollection(INPUT.stream().map(Tuple1::of).collect(Collectors.toList()));

        // partition by hash
        src.keyBy(0).map(new SubtaskIndexAssigner()).addSink(hashPartitionResultSink);

        // partition custom
        DataStream<Tuple2<Integer, String>> partitionCustom =
                src.partitionCustom(
                                new Partitioner<String>() {
                                    @Override
                                    public int partition(String key, int numPartitions) {
                                        if (key.equals("c")) {
                                            return 2;
                                        } else {
                                            return 0;
                                        }
                                    }
                                },
                                0)
                        .map(new SubtaskIndexAssigner());

        partitionCustom.addSink(customPartitionResultSink);

        // partition broadcast
        src.broadcast().map(new SubtaskIndexAssigner()).addSink(broadcastPartitionResultSink);

        // partition rebalance
        src.rebalance().map(new SubtaskIndexAssigner()).addSink(rebalancePartitionResultSink);

        // partition forward
        src.map(
                        new MapFunction<Tuple1<String>, Tuple1<String>>() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public Tuple1<String> map(Tuple1<String> value) throws Exception {
                                return value;
                            }
                        })
                .forward()
                .map(new SubtaskIndexAssigner())
                .addSink(forwardPartitionResultSink);

        // partition global
        src.global().map(new SubtaskIndexAssigner()).addSink(globalPartitionResultSink);

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

        List<Tuple2<Integer, String>> hashPartitionResult = hashPartitionResultSink.getResult();
        List<Tuple2<Integer, String>> customPartitionResult = customPartitionResultSink.getResult();
        List<Tuple2<Integer, String>> broadcastPartitionResult =
                broadcastPartitionResultSink.getResult();
        List<Tuple2<Integer, String>> forwardPartitionResult =
                forwardPartitionResultSink.getResult();
        List<Tuple2<Integer, String>> rebalancePartitionResult =
                rebalancePartitionResultSink.getResult();
        List<Tuple2<Integer, String>> globalPartitionResult = globalPartitionResultSink.getResult();

        verifyHashPartitioning(hashPartitionResult);
        verifyCustomPartitioning(customPartitionResult);
        verifyBroadcastPartitioning(broadcastPartitionResult);
        verifyRebalancePartitioning(forwardPartitionResult);
        verifyRebalancePartitioning(rebalancePartitionResult);
        verifyGlobalPartitioning(globalPartitionResult);
    }

    private static void verifyHashPartitioning(List<Tuple2<Integer, String>> hashPartitionResult) {
        HashMap<String, Integer> verifier = new HashMap<String, Integer>();
        for (Tuple2<Integer, String> elem : hashPartitionResult) {
            Integer subtaskIndex = verifier.get(elem.f1);
            if (subtaskIndex == null) {
                verifier.put(elem.f1, elem.f0);
            } else if (!Objects.equals(subtaskIndex, elem.f0)) {
                fail();
            }
        }
    }

    private static void verifyCustomPartitioning(
            List<Tuple2<Integer, String>> customPartitionResult) {
        for (Tuple2<Integer, String> stringWithSubtask : customPartitionResult) {
            if (stringWithSubtask.f1.equals("c")) {
                assertEquals(new Integer(2), stringWithSubtask.f0);
            } else {
                assertEquals(new Integer(0), stringWithSubtask.f0);
            }
        }
    }

    private static void verifyBroadcastPartitioning(
            List<Tuple2<Integer, String>> broadcastPartitionResult) {
        final Set<Tuple2<Integer, String>> expectedResult =
                INPUT.stream()
                        .flatMap(
                                input ->
                                        IntStream.range(0, PARALLELISM)
                                                .mapToObj(i -> Tuple2.of(i, input)))
                        .collect(Collectors.toSet());

        assertEquals(expectedResult, new HashSet<>(broadcastPartitionResult));
    }

    private static void verifyRebalancePartitioning(
            List<Tuple2<Integer, String>> rebalancePartitionResult) {
        sort(rebalancePartitionResult, Comparator.comparing(o -> o.f1));

        final Tuple2<Integer, String> firstEntry = rebalancePartitionResult.get(0);
        int offset = firstEntry.f0;

        final List<Tuple2<Integer, String>> expected =
                IntStream.range(0, rebalancePartitionResult.size())
                        .mapToObj(
                                index ->
                                        Tuple2.of((offset + index) % PARALLELISM, INPUT.get(index)))
                        .collect(Collectors.toList());

        assertEquals(expected, rebalancePartitionResult);
    }

    private static void verifyGlobalPartitioning(
            List<Tuple2<Integer, String>> globalPartitionResult) {
        final List<Tuple2<Integer, String>> expected =
                INPUT.stream().map(i -> Tuple2.of(0, i)).collect(Collectors.toList());

        assertEquals(expected, globalPartitionResult);
    }

    private static class SubtaskIndexAssigner
            extends RichMapFunction<Tuple1<String>, Tuple2<Integer, String>> {
        private static final long serialVersionUID = 1L;

        private int indexOfSubtask;

        @Override
        public void open(OpenContext openContext) throws Exception {
            super.open(openContext);
            RuntimeContext runtimeContext = getRuntimeContext();
            indexOfSubtask = runtimeContext.getIndexOfThisSubtask();
        }

        @Override
        public Tuple2<Integer, String> map(Tuple1<String> value) throws Exception {
            return new Tuple2<Integer, String>(indexOfSubtask, value.f0);
        }
    }
}
