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

import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.EnumTypeInfo;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.LegacyKeyedProcessOperator;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.runtime.partitioner.BroadcastPartitioner;
import org.apache.flink.streaming.runtime.partitioner.CustomPartitionerWrapper;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.GlobalPartitioner;
import org.apache.flink.streaming.runtime.partitioner.KeyGroupStreamPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RebalancePartitioner;
import org.apache.flink.streaming.runtime.partitioner.ShufflePartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLogger;

import org.hamcrest.core.StringStartsWith;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.annotation.Nullable;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for {@link DataStream}. */
@SuppressWarnings("serial")
public class DataStreamTest extends TestLogger {

    @Rule public ExpectedException expectedException = ExpectedException.none();

    /** Ensure that WatermarkStrategy is easy to use in the API, without superfluous generics. */
    @Test
    public void testErgonomicWatermarkStrategy() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> input = env.fromElements("bonjour");

        // as soon as you have a chain of methods the first call needs a generic
        input.assignTimestampsAndWatermarks(
                WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMillis(10)));

        // as soon as you have a chain of methods the first call needs to specify the generic type
        input.assignTimestampsAndWatermarks(
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofMillis(10))
                        .withTimestampAssigner((event, timestamp) -> 42L));
    }

    /**
     * Tests union functionality. This ensures that self-unions and unions of streams with differing
     * parallelism work.
     *
     * @throws Exception
     */
    @Test
    public void testUnion() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStream<Long> input1 =
                env.generateSequence(0, 0)
                        .map(
                                new MapFunction<Long, Long>() {
                                    @Override
                                    public Long map(Long value) throws Exception {
                                        return null;
                                    }
                                });

        DataStream<Long> selfUnion =
                input1.union(input1)
                        .map(
                                new MapFunction<Long, Long>() {
                                    @Override
                                    public Long map(Long value) throws Exception {
                                        return null;
                                    }
                                });

        DataStream<Long> input6 =
                env.generateSequence(0, 0)
                        .map(
                                new MapFunction<Long, Long>() {
                                    @Override
                                    public Long map(Long value) throws Exception {
                                        return null;
                                    }
                                });

        DataStream<Long> selfUnionDifferentPartition =
                input6.broadcast()
                        .union(input6)
                        .map(
                                new MapFunction<Long, Long>() {
                                    @Override
                                    public Long map(Long value) throws Exception {
                                        return null;
                                    }
                                });

        DataStream<Long> input2 =
                env.generateSequence(0, 0)
                        .map(
                                new MapFunction<Long, Long>() {
                                    @Override
                                    public Long map(Long value) throws Exception {
                                        return null;
                                    }
                                })
                        .setParallelism(4);

        DataStream<Long> input3 =
                env.generateSequence(0, 0)
                        .map(
                                new MapFunction<Long, Long>() {
                                    @Override
                                    public Long map(Long value) throws Exception {
                                        return null;
                                    }
                                })
                        .setParallelism(2);

        DataStream<Long> unionDifferingParallelism =
                input2.union(input3)
                        .map(
                                new MapFunction<Long, Long>() {
                                    @Override
                                    public Long map(Long value) throws Exception {
                                        return null;
                                    }
                                })
                        .setParallelism(4);

        DataStream<Long> input4 =
                env.generateSequence(0, 0)
                        .map(
                                new MapFunction<Long, Long>() {
                                    @Override
                                    public Long map(Long value) throws Exception {
                                        return null;
                                    }
                                })
                        .setParallelism(2);

        DataStream<Long> input5 =
                env.generateSequence(0, 0)
                        .map(
                                new MapFunction<Long, Long>() {
                                    @Override
                                    public Long map(Long value) throws Exception {
                                        return null;
                                    }
                                })
                        .setParallelism(4);

        DataStream<Long> unionDifferingPartitioning =
                input4.broadcast()
                        .union(input5)
                        .map(
                                new MapFunction<Long, Long>() {
                                    @Override
                                    public Long map(Long value) throws Exception {
                                        return null;
                                    }
                                })
                        .setParallelism(4);

        StreamGraph streamGraph = getStreamGraph(env);

        // verify self union
        assertTrue(streamGraph.getStreamNode(selfUnion.getId()).getInEdges().size() == 2);
        for (StreamEdge edge : streamGraph.getStreamNode(selfUnion.getId()).getInEdges()) {
            assertTrue(edge.getPartitioner() instanceof ForwardPartitioner);
        }

        // verify self union with different partitioners
        assertTrue(
                streamGraph.getStreamNode(selfUnionDifferentPartition.getId()).getInEdges().size()
                        == 2);
        boolean hasForward = false;
        boolean hasBroadcast = false;
        for (StreamEdge edge :
                streamGraph.getStreamNode(selfUnionDifferentPartition.getId()).getInEdges()) {
            if (edge.getPartitioner() instanceof ForwardPartitioner) {
                hasForward = true;
            }
            if (edge.getPartitioner() instanceof BroadcastPartitioner) {
                hasBroadcast = true;
            }
        }
        assertTrue(hasForward && hasBroadcast);

        // verify union of streams with differing parallelism
        assertTrue(
                streamGraph.getStreamNode(unionDifferingParallelism.getId()).getInEdges().size()
                        == 2);
        for (StreamEdge edge :
                streamGraph.getStreamNode(unionDifferingParallelism.getId()).getInEdges()) {
            if (edge.getSourceId() == input2.getId()) {
                assertTrue(edge.getPartitioner() instanceof ForwardPartitioner);
            } else if (edge.getSourceId() == input3.getId()) {
                assertTrue(edge.getPartitioner() instanceof RebalancePartitioner);
            } else {
                fail("Wrong input edge.");
            }
        }

        // verify union of streams with differing partitionings
        assertTrue(
                streamGraph.getStreamNode(unionDifferingPartitioning.getId()).getInEdges().size()
                        == 2);
        for (StreamEdge edge :
                streamGraph.getStreamNode(unionDifferingPartitioning.getId()).getInEdges()) {
            if (edge.getSourceId() == input4.getId()) {
                assertTrue(edge.getPartitioner() instanceof BroadcastPartitioner);
            } else if (edge.getSourceId() == input5.getId()) {
                assertTrue(edge.getPartitioner() instanceof ForwardPartitioner);
            } else {
                fail("Wrong input edge.");
            }
        }
    }

    /**
     * Tests {@link SingleOutputStreamOperator#name(String)} functionality.
     *
     * @throws Exception
     */
    @Test
    public void testNaming() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Long> dataStream1 =
                env.generateSequence(0, 0)
                        .name("testSource1")
                        .map(
                                new MapFunction<Long, Long>() {
                                    @Override
                                    public Long map(Long value) throws Exception {
                                        return null;
                                    }
                                })
                        .name("testMap");

        DataStream<Long> dataStream2 =
                env.generateSequence(0, 0)
                        .name("testSource2")
                        .map(
                                new MapFunction<Long, Long>() {
                                    @Override
                                    public Long map(Long value) throws Exception {
                                        return null;
                                    }
                                })
                        .name("testMap");

        dataStream1
                .connect(dataStream2)
                .flatMap(
                        new CoFlatMapFunction<Long, Long, Long>() {

                            @Override
                            public void flatMap1(Long value, Collector<Long> out)
                                    throws Exception {}

                            @Override
                            public void flatMap2(Long value, Collector<Long> out)
                                    throws Exception {}
                        })
                .name("testCoFlatMap")
                .windowAll(GlobalWindows.create())
                .trigger(PurgingTrigger.of(CountTrigger.of(10)))
                .reduce(
                        new ReduceFunction<Long>() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public Long reduce(Long value1, Long value2) throws Exception {
                                return null;
                            }
                        })
                .name("testWindowReduce")
                .print();

        // test functionality through the operator names in the execution plan
        String plan = env.getExecutionPlan();

        assertTrue(plan.contains("testSource1"));
        assertTrue(plan.contains("testSource2"));
        assertTrue(plan.contains("testMap"));
        assertTrue(plan.contains("testMap"));
        assertTrue(plan.contains("testCoFlatMap"));
        assertTrue(plan.contains("testWindowReduce"));
    }

    /**
     * Tests that {@link DataStream#keyBy} and {@link DataStream#partitionCustom(Partitioner, int)}
     * result in different and correct topologies. Does the some for the {@link ConnectedStreams}.
     */
    @Test
    public void testPartitioning() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<Long, Long>> src1 = env.fromElements(new Tuple2<>(0L, 0L));
        DataStream<Tuple2<Long, Long>> src2 = env.fromElements(new Tuple2<>(0L, 0L));
        ConnectedStreams<Tuple2<Long, Long>, Tuple2<Long, Long>> connected = src1.connect(src2);

        // Testing DataStream grouping
        DataStream<Tuple2<Long, Long>> group1 = src1.keyBy(0);
        DataStream<Tuple2<Long, Long>> group2 = src1.keyBy(1, 0);
        DataStream<Tuple2<Long, Long>> group3 = src1.keyBy("f0");
        DataStream<Tuple2<Long, Long>> group4 = src1.keyBy(new FirstSelector());

        int id1 = createDownStreamId(group1);
        int id2 = createDownStreamId(group2);
        int id3 = createDownStreamId(group3);
        int id4 = createDownStreamId(group4);

        assertTrue(isPartitioned(getStreamGraph(env).getStreamEdgesOrThrow(src1.getId(), id1)));
        assertTrue(isPartitioned(getStreamGraph(env).getStreamEdgesOrThrow(src1.getId(), id2)));
        assertTrue(isPartitioned(getStreamGraph(env).getStreamEdgesOrThrow(src1.getId(), id3)));
        assertTrue(isPartitioned(getStreamGraph(env).getStreamEdgesOrThrow(src1.getId(), id4)));

        assertTrue(isKeyed(group1));
        assertTrue(isKeyed(group2));
        assertTrue(isKeyed(group3));
        assertTrue(isKeyed(group4));

        // Testing DataStream partitioning
        DataStream<Tuple2<Long, Long>> partition1 = src1.keyBy(0);
        DataStream<Tuple2<Long, Long>> partition2 = src1.keyBy(1, 0);
        DataStream<Tuple2<Long, Long>> partition3 = src1.keyBy("f0");
        DataStream<Tuple2<Long, Long>> partition4 = src1.keyBy(new FirstSelector());

        int pid1 = createDownStreamId(partition1);
        int pid2 = createDownStreamId(partition2);
        int pid3 = createDownStreamId(partition3);
        int pid4 = createDownStreamId(partition4);

        assertTrue(isPartitioned(getStreamGraph(env).getStreamEdgesOrThrow(src1.getId(), pid1)));
        assertTrue(isPartitioned(getStreamGraph(env).getStreamEdgesOrThrow(src1.getId(), pid2)));
        assertTrue(isPartitioned(getStreamGraph(env).getStreamEdgesOrThrow(src1.getId(), pid3)));
        assertTrue(isPartitioned(getStreamGraph(env).getStreamEdgesOrThrow(src1.getId(), pid4)));

        assertTrue(isKeyed(partition1));
        assertTrue(isKeyed(partition3));
        assertTrue(isKeyed(partition2));
        assertTrue(isKeyed(partition4));

        // Testing DataStream custom partitioning
        Partitioner<Long> longPartitioner =
                new Partitioner<Long>() {
                    @Override
                    public int partition(Long key, int numPartitions) {
                        return 100;
                    }
                };

        DataStream<Tuple2<Long, Long>> customPartition1 = src1.partitionCustom(longPartitioner, 0);
        DataStream<Tuple2<Long, Long>> customPartition3 =
                src1.partitionCustom(longPartitioner, "f0");
        DataStream<Tuple2<Long, Long>> customPartition4 =
                src1.partitionCustom(longPartitioner, new FirstSelector());

        int cid1 = createDownStreamId(customPartition1);
        int cid2 = createDownStreamId(customPartition3);
        int cid3 = createDownStreamId(customPartition4);

        assertTrue(
                isCustomPartitioned(getStreamGraph(env).getStreamEdgesOrThrow(src1.getId(), cid1)));
        assertTrue(
                isCustomPartitioned(getStreamGraph(env).getStreamEdgesOrThrow(src1.getId(), cid2)));
        assertTrue(
                isCustomPartitioned(getStreamGraph(env).getStreamEdgesOrThrow(src1.getId(), cid3)));

        assertFalse(isKeyed(customPartition1));
        assertFalse(isKeyed(customPartition3));
        assertFalse(isKeyed(customPartition4));

        // Testing ConnectedStreams grouping
        ConnectedStreams<Tuple2<Long, Long>, Tuple2<Long, Long>> connectedGroup1 =
                connected.keyBy(0, 0);
        Integer downStreamId1 = createDownStreamId(connectedGroup1);

        ConnectedStreams<Tuple2<Long, Long>, Tuple2<Long, Long>> connectedGroup2 =
                connected.keyBy(new int[] {0}, new int[] {0});
        Integer downStreamId2 = createDownStreamId(connectedGroup2);

        ConnectedStreams<Tuple2<Long, Long>, Tuple2<Long, Long>> connectedGroup3 =
                connected.keyBy("f0", "f0");
        Integer downStreamId3 = createDownStreamId(connectedGroup3);

        ConnectedStreams<Tuple2<Long, Long>, Tuple2<Long, Long>> connectedGroup4 =
                connected.keyBy(new String[] {"f0"}, new String[] {"f0"});
        Integer downStreamId4 = createDownStreamId(connectedGroup4);

        ConnectedStreams<Tuple2<Long, Long>, Tuple2<Long, Long>> connectedGroup5 =
                connected.keyBy(new FirstSelector(), new FirstSelector());
        Integer downStreamId5 = createDownStreamId(connectedGroup5);

        assertTrue(
                isPartitioned(
                        getStreamGraph(env).getStreamEdgesOrThrow(src1.getId(), downStreamId1)));
        assertTrue(
                isPartitioned(
                        getStreamGraph(env).getStreamEdgesOrThrow(src2.getId(), downStreamId1)));

        assertTrue(
                isPartitioned(
                        getStreamGraph(env).getStreamEdgesOrThrow(src1.getId(), downStreamId2)));
        assertTrue(
                isPartitioned(
                        getStreamGraph(env).getStreamEdgesOrThrow(src2.getId(), downStreamId2)));

        assertTrue(
                isPartitioned(
                        getStreamGraph(env).getStreamEdgesOrThrow(src1.getId(), downStreamId3)));
        assertTrue(
                isPartitioned(
                        getStreamGraph(env).getStreamEdgesOrThrow(src2.getId(), downStreamId3)));

        assertTrue(
                isPartitioned(
                        getStreamGraph(env).getStreamEdgesOrThrow(src1.getId(), downStreamId4)));
        assertTrue(
                isPartitioned(
                        getStreamGraph(env).getStreamEdgesOrThrow(src2.getId(), downStreamId4)));

        assertTrue(
                isPartitioned(
                        getStreamGraph(env).getStreamEdgesOrThrow(src1.getId(), downStreamId5)));
        assertTrue(
                isPartitioned(
                        getStreamGraph(env).getStreamEdgesOrThrow(src2.getId(), downStreamId5)));

        assertTrue(isKeyed(connectedGroup1));
        assertTrue(isKeyed(connectedGroup2));
        assertTrue(isKeyed(connectedGroup3));
        assertTrue(isKeyed(connectedGroup4));
        assertTrue(isKeyed(connectedGroup5));

        // Testing ConnectedStreams partitioning
        ConnectedStreams<Tuple2<Long, Long>, Tuple2<Long, Long>> connectedPartition1 =
                connected.keyBy(0, 0);
        Integer connectDownStreamId1 = createDownStreamId(connectedPartition1);

        ConnectedStreams<Tuple2<Long, Long>, Tuple2<Long, Long>> connectedPartition2 =
                connected.keyBy(new int[] {0}, new int[] {0});
        Integer connectDownStreamId2 = createDownStreamId(connectedPartition2);

        ConnectedStreams<Tuple2<Long, Long>, Tuple2<Long, Long>> connectedPartition3 =
                connected.keyBy("f0", "f0");
        Integer connectDownStreamId3 = createDownStreamId(connectedPartition3);

        ConnectedStreams<Tuple2<Long, Long>, Tuple2<Long, Long>> connectedPartition4 =
                connected.keyBy(new String[] {"f0"}, new String[] {"f0"});
        Integer connectDownStreamId4 = createDownStreamId(connectedPartition4);

        ConnectedStreams<Tuple2<Long, Long>, Tuple2<Long, Long>> connectedPartition5 =
                connected.keyBy(new FirstSelector(), new FirstSelector());
        Integer connectDownStreamId5 = createDownStreamId(connectedPartition5);

        assertTrue(
                isPartitioned(
                        getStreamGraph(env)
                                .getStreamEdgesOrThrow(src1.getId(), connectDownStreamId1)));
        assertTrue(
                isPartitioned(
                        getStreamGraph(env)
                                .getStreamEdgesOrThrow(src2.getId(), connectDownStreamId1)));

        assertTrue(
                isPartitioned(
                        getStreamGraph(env)
                                .getStreamEdgesOrThrow(src1.getId(), connectDownStreamId2)));
        assertTrue(
                isPartitioned(
                        getStreamGraph(env)
                                .getStreamEdgesOrThrow(src2.getId(), connectDownStreamId2)));

        assertTrue(
                isPartitioned(
                        getStreamGraph(env)
                                .getStreamEdgesOrThrow(src1.getId(), connectDownStreamId3)));
        assertTrue(
                isPartitioned(
                        getStreamGraph(env)
                                .getStreamEdgesOrThrow(src2.getId(), connectDownStreamId3)));

        assertTrue(
                isPartitioned(
                        getStreamGraph(env)
                                .getStreamEdgesOrThrow(src1.getId(), connectDownStreamId4)));
        assertTrue(
                isPartitioned(
                        getStreamGraph(env)
                                .getStreamEdgesOrThrow(src2.getId(), connectDownStreamId4)));

        assertTrue(
                isPartitioned(
                        getStreamGraph(env)
                                .getStreamEdgesOrThrow(src1.getId(), connectDownStreamId5)));
        assertTrue(
                isPartitioned(
                        getStreamGraph(env)
                                .getStreamEdgesOrThrow(src2.getId(), connectDownStreamId5)));

        assertTrue(isKeyed(connectedPartition1));
        assertTrue(isKeyed(connectedPartition2));
        assertTrue(isKeyed(connectedPartition3));
        assertTrue(isKeyed(connectedPartition4));
        assertTrue(isKeyed(connectedPartition5));
    }

    /** Tests whether parallelism gets set. */
    @Test
    public void testParallelism() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple2<Long, Long>> src = env.fromElements(new Tuple2<>(0L, 0L));
        env.setParallelism(10);

        SingleOutputStreamOperator<Long> map =
                src.map(
                                new MapFunction<Tuple2<Long, Long>, Long>() {
                                    @Override
                                    public Long map(Tuple2<Long, Long> value) throws Exception {
                                        return null;
                                    }
                                })
                        .name("MyMap");

        DataStream<Long> windowed =
                map.windowAll(GlobalWindows.create())
                        .trigger(PurgingTrigger.of(CountTrigger.of(10)))
                        .reduce(
                                new ReduceFunction<Long>() {
                                    @Override
                                    public Long reduce(Long value1, Long value2) throws Exception {
                                        return null;
                                    }
                                });

        windowed.addSink(new DiscardingSink<Long>());

        DataStreamSink<Long> sink =
                map.addSink(
                        new SinkFunction<Long>() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public void invoke(Long value) throws Exception {}
                        });

        assertEquals(1, getStreamGraph(env).getStreamNode(src.getId()).getParallelism());
        assertEquals(10, getStreamGraph(env).getStreamNode(map.getId()).getParallelism());
        assertEquals(1, getStreamGraph(env).getStreamNode(windowed.getId()).getParallelism());
        assertEquals(
                10,
                getStreamGraph(env)
                        .getStreamNode(sink.getTransformation().getId())
                        .getParallelism());

        env.setParallelism(7);

        // Some parts, such as windowing rely on the fact that previous operators have a parallelism
        // set when instantiating the Discretizer. This would break if we dynamically changed
        // the parallelism of operations when changing the setting on the Execution Environment.
        assertEquals(1, getStreamGraph(env).getStreamNode(src.getId()).getParallelism());
        assertEquals(10, getStreamGraph(env).getStreamNode(map.getId()).getParallelism());
        assertEquals(1, getStreamGraph(env).getStreamNode(windowed.getId()).getParallelism());
        assertEquals(
                10,
                getStreamGraph(env)
                        .getStreamNode(sink.getTransformation().getId())
                        .getParallelism());

        try {
            src.setParallelism(3);
            fail();
        } catch (IllegalArgumentException success) {
            // do nothing
        }

        DataStreamSource<Long> parallelSource = env.generateSequence(0, 0);
        parallelSource.addSink(new DiscardingSink<Long>());
        assertEquals(7, getStreamGraph(env).getStreamNode(parallelSource.getId()).getParallelism());

        parallelSource.setParallelism(3);
        assertEquals(3, getStreamGraph(env).getStreamNode(parallelSource.getId()).getParallelism());

        map.setParallelism(2);
        assertEquals(2, getStreamGraph(env).getStreamNode(map.getId()).getParallelism());

        sink.setParallelism(4);
        assertEquals(
                4,
                getStreamGraph(env)
                        .getStreamNode(sink.getTransformation().getId())
                        .getParallelism());
    }

    /** Tests whether resources get set. */
    @Test
    public void testResources() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ResourceSpec minResource1 = ResourceSpec.newBuilder(1.0, 100).build();
        ResourceSpec preferredResource1 = ResourceSpec.newBuilder(2.0, 200).build();

        ResourceSpec minResource2 = ResourceSpec.newBuilder(1.0, 200).build();
        ResourceSpec preferredResource2 = ResourceSpec.newBuilder(2.0, 300).build();

        ResourceSpec minResource3 = ResourceSpec.newBuilder(1.0, 300).build();
        ResourceSpec preferredResource3 = ResourceSpec.newBuilder(2.0, 400).build();

        ResourceSpec minResource4 = ResourceSpec.newBuilder(1.0, 400).build();
        ResourceSpec preferredResource4 = ResourceSpec.newBuilder(2.0, 500).build();

        ResourceSpec minResource5 = ResourceSpec.newBuilder(1.0, 500).build();
        ResourceSpec preferredResource5 = ResourceSpec.newBuilder(2.0, 600).build();

        ResourceSpec minResource6 = ResourceSpec.newBuilder(1.0, 600).build();
        ResourceSpec preferredResource6 = ResourceSpec.newBuilder(2.0, 700).build();

        ResourceSpec minResource7 = ResourceSpec.newBuilder(1.0, 700).build();
        ResourceSpec preferredResource7 = ResourceSpec.newBuilder(2.0, 800).build();

        Method opMethod =
                SingleOutputStreamOperator.class.getDeclaredMethod(
                        "setResources", ResourceSpec.class, ResourceSpec.class);
        opMethod.setAccessible(true);

        Method sinkMethod =
                DataStreamSink.class.getDeclaredMethod(
                        "setResources", ResourceSpec.class, ResourceSpec.class);
        sinkMethod.setAccessible(true);

        DataStream<Long> source1 = env.generateSequence(0, 0);
        opMethod.invoke(source1, minResource1, preferredResource1);

        DataStream<Long> map1 =
                source1.map(
                        new MapFunction<Long, Long>() {
                            @Override
                            public Long map(Long value) throws Exception {
                                return null;
                            }
                        });
        opMethod.invoke(map1, minResource2, preferredResource2);

        DataStream<Long> source2 = env.generateSequence(0, 0);
        opMethod.invoke(source2, minResource3, preferredResource3);

        DataStream<Long> map2 =
                source2.map(
                        new MapFunction<Long, Long>() {
                            @Override
                            public Long map(Long value) throws Exception {
                                return null;
                            }
                        });
        opMethod.invoke(map2, minResource4, preferredResource4);

        DataStream<Long> connected =
                map1.connect(map2)
                        .flatMap(
                                new CoFlatMapFunction<Long, Long, Long>() {
                                    @Override
                                    public void flatMap1(Long value, Collector<Long> out)
                                            throws Exception {}

                                    @Override
                                    public void flatMap2(Long value, Collector<Long> out)
                                            throws Exception {}
                                });
        opMethod.invoke(connected, minResource5, preferredResource5);

        DataStream<Long> windowed =
                connected
                        .windowAll(GlobalWindows.create())
                        .trigger(PurgingTrigger.of(CountTrigger.of(10)))
                        .reduce(
                                new ReduceFunction<Long>() {
                                    private static final long serialVersionUID = 1L;

                                    @Override
                                    public Long reduce(Long value1, Long value2) throws Exception {
                                        return null;
                                    }
                                });
        opMethod.invoke(windowed, minResource6, preferredResource6);

        DataStreamSink<Long> sink = windowed.print();
        sinkMethod.invoke(sink, minResource7, preferredResource7);

        assertEquals(
                minResource1, getStreamGraph(env).getStreamNode(source1.getId()).getMinResources());
        assertEquals(
                preferredResource1,
                getStreamGraph(env).getStreamNode(source1.getId()).getPreferredResources());

        assertEquals(
                minResource2, getStreamGraph(env).getStreamNode(map1.getId()).getMinResources());
        assertEquals(
                preferredResource2,
                getStreamGraph(env).getStreamNode(map1.getId()).getPreferredResources());

        assertEquals(
                minResource3, getStreamGraph(env).getStreamNode(source2.getId()).getMinResources());
        assertEquals(
                preferredResource3,
                getStreamGraph(env).getStreamNode(source2.getId()).getPreferredResources());

        assertEquals(
                minResource4, getStreamGraph(env).getStreamNode(map2.getId()).getMinResources());
        assertEquals(
                preferredResource4,
                getStreamGraph(env).getStreamNode(map2.getId()).getPreferredResources());

        assertEquals(
                minResource5,
                getStreamGraph(env).getStreamNode(connected.getId()).getMinResources());
        assertEquals(
                preferredResource5,
                getStreamGraph(env).getStreamNode(connected.getId()).getPreferredResources());

        assertEquals(
                minResource6,
                getStreamGraph(env).getStreamNode(windowed.getId()).getMinResources());
        assertEquals(
                preferredResource6,
                getStreamGraph(env).getStreamNode(windowed.getId()).getPreferredResources());

        assertEquals(
                minResource7,
                getStreamGraph(env)
                        .getStreamNode(sink.getTransformation().getId())
                        .getMinResources());
        assertEquals(
                preferredResource7,
                getStreamGraph(env)
                        .getStreamNode(sink.getTransformation().getId())
                        .getPreferredResources());
    }

    @Test
    public void testTypeInfo() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Long> src1 = env.generateSequence(0, 0);
        assertEquals(TypeExtractor.getForClass(Long.class), src1.getType());

        DataStream<Tuple2<Integer, String>> map =
                src1.map(
                        new MapFunction<Long, Tuple2<Integer, String>>() {
                            @Override
                            public Tuple2<Integer, String> map(Long value) throws Exception {
                                return null;
                            }
                        });

        assertEquals(TypeExtractor.getForObject(new Tuple2<>(0, "")), map.getType());

        DataStream<String> window =
                map.windowAll(GlobalWindows.create())
                        .trigger(PurgingTrigger.of(CountTrigger.of(5)))
                        .apply(
                                new AllWindowFunction<
                                        Tuple2<Integer, String>, String, GlobalWindow>() {
                                    @Override
                                    public void apply(
                                            GlobalWindow window,
                                            Iterable<Tuple2<Integer, String>> values,
                                            Collector<String> out)
                                            throws Exception {}
                                });

        assertEquals(TypeExtractor.getForClass(String.class), window.getType());

        DataStream<CustomPOJO> flatten =
                window.windowAll(GlobalWindows.create())
                        .trigger(PurgingTrigger.of(CountTrigger.of(5)))
                        .aggregate(
                                new AggregateFunction<String, CustomPOJO, CustomPOJO>() {
                                    private static final long serialVersionUID = 1L;

                                    @Override
                                    public CustomPOJO createAccumulator() {
                                        return null;
                                    }

                                    @Override
                                    public CustomPOJO add(String value, CustomPOJO accumulator) {
                                        return null;
                                    }

                                    @Override
                                    public CustomPOJO getResult(CustomPOJO accumulator) {
                                        return null;
                                    }

                                    @Override
                                    public CustomPOJO merge(CustomPOJO a, CustomPOJO b) {
                                        return null;
                                    }
                                });

        assertEquals(TypeExtractor.getForClass(CustomPOJO.class), flatten.getType());
    }

    /**
     * Verify that a {@link KeyedStream#process(ProcessFunction)} call is correctly translated to an
     * operator.
     */
    @Test
    @Deprecated
    public void testKeyedStreamProcessTranslation() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> src = env.generateSequence(0, 0);

        ProcessFunction<Long, Integer> processFunction =
                new ProcessFunction<Long, Integer>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public void processElement(Long value, Context ctx, Collector<Integer> out)
                            throws Exception {
                        // Do nothing
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Integer> out)
                            throws Exception {
                        // Do nothing
                    }
                };

        DataStream<Integer> processed =
                src.keyBy(new IdentityKeySelector<Long>()).process(processFunction);

        processed.addSink(new DiscardingSink<Integer>());

        assertEquals(processFunction, getFunctionForDataStream(processed));
        assertTrue(getOperatorForDataStream(processed) instanceof LegacyKeyedProcessOperator);
    }

    /**
     * Verify that a {@link KeyedStream#process(KeyedProcessFunction)} call is correctly translated
     * to an operator.
     */
    @Test
    public void testKeyedStreamKeyedProcessTranslation() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> src = env.generateSequence(0, 0);

        KeyedProcessFunction<Long, Long, Integer> keyedProcessFunction =
                new KeyedProcessFunction<Long, Long, Integer>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public void processElement(Long value, Context ctx, Collector<Integer> out)
                            throws Exception {
                        // Do nothing
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Integer> out)
                            throws Exception {
                        // Do nothing
                    }
                };

        DataStream<Integer> processed =
                src.keyBy(new IdentityKeySelector<Long>()).process(keyedProcessFunction);

        processed.addSink(new DiscardingSink<Integer>());

        assertEquals(keyedProcessFunction, getFunctionForDataStream(processed));
        assertTrue(getOperatorForDataStream(processed) instanceof KeyedProcessOperator);
    }

    /**
     * Verify that a {@link DataStream#process(ProcessFunction)} call is correctly translated to an
     * operator.
     */
    @Test
    public void testProcessTranslation() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> src = env.generateSequence(0, 0);

        ProcessFunction<Long, Integer> processFunction =
                new ProcessFunction<Long, Integer>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public void processElement(Long value, Context ctx, Collector<Integer> out)
                            throws Exception {
                        // Do nothing
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Integer> out)
                            throws Exception {
                        // Do nothing
                    }
                };

        DataStream<Integer> processed = src.process(processFunction);

        processed.addSink(new DiscardingSink<Integer>());

        assertEquals(processFunction, getFunctionForDataStream(processed));
        assertTrue(getOperatorForDataStream(processed) instanceof ProcessOperator);
    }

    /**
     * Tests that with a {@link KeyedStream} we have to provide a {@link
     * KeyedBroadcastProcessFunction}.
     */
    @Test
    public void testFailedTranslationOnKeyed() {

        final MapStateDescriptor<Long, String> descriptor =
                new MapStateDescriptor<>(
                        "broadcast", BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final DataStream<Long> srcOne =
                env.generateSequence(0L, 5L)
                        .assignTimestampsAndWatermarks(
                                new CustomWmEmitter<Long>() {

                                    @Override
                                    public long extractTimestamp(
                                            Long element, long previousElementTimestamp) {
                                        return element;
                                    }
                                })
                        .keyBy((KeySelector<Long, Long>) value -> value);

        final DataStream<String> srcTwo =
                env.fromElements("Test:0", "Test:1", "Test:2", "Test:3", "Test:4", "Test:5")
                        .assignTimestampsAndWatermarks(
                                new CustomWmEmitter<String>() {
                                    @Override
                                    public long extractTimestamp(
                                            String element, long previousElementTimestamp) {
                                        return Long.parseLong(element.split(":")[1]);
                                    }
                                });

        BroadcastStream<String> broadcast = srcTwo.broadcast(descriptor);
        BroadcastConnectedStream<Long, String> bcStream = srcOne.connect(broadcast);

        expectedException.expect(IllegalArgumentException.class);
        bcStream.process(
                new BroadcastProcessFunction<Long, String, String>() {
                    @Override
                    public void processBroadcastElement(
                            String value, Context ctx, Collector<String> out) throws Exception {
                        // do nothing
                    }

                    @Override
                    public void processElement(
                            Long value, ReadOnlyContext ctx, Collector<String> out)
                            throws Exception {
                        // do nothing
                    }
                });
    }

    /** Tests that with a non-keyed stream we have to provide a {@link BroadcastProcessFunction}. */
    @Test
    public void testFailedTranslationOnNonKeyed() {

        final MapStateDescriptor<Long, String> descriptor =
                new MapStateDescriptor<>(
                        "broadcast", BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final DataStream<Long> srcOne =
                env.generateSequence(0L, 5L)
                        .assignTimestampsAndWatermarks(
                                new CustomWmEmitter<Long>() {

                                    @Override
                                    public long extractTimestamp(
                                            Long element, long previousElementTimestamp) {
                                        return element;
                                    }
                                });

        final DataStream<String> srcTwo =
                env.fromElements("Test:0", "Test:1", "Test:2", "Test:3", "Test:4", "Test:5")
                        .assignTimestampsAndWatermarks(
                                new CustomWmEmitter<String>() {
                                    @Override
                                    public long extractTimestamp(
                                            String element, long previousElementTimestamp) {
                                        return Long.parseLong(element.split(":")[1]);
                                    }
                                });

        BroadcastStream<String> broadcast = srcTwo.broadcast(descriptor);
        BroadcastConnectedStream<Long, String> bcStream = srcOne.connect(broadcast);

        expectedException.expect(IllegalArgumentException.class);
        bcStream.process(
                new KeyedBroadcastProcessFunction<String, Long, String, String>() {
                    @Override
                    public void processBroadcastElement(
                            String value, Context ctx, Collector<String> out) throws Exception {
                        // do nothing
                    }

                    @Override
                    public void processElement(
                            Long value, ReadOnlyContext ctx, Collector<String> out)
                            throws Exception {
                        // do nothing
                    }
                });
    }

    private abstract static class CustomWmEmitter<T>
            implements AssignerWithPunctuatedWatermarks<T> {

        @Nullable
        @Override
        public Watermark checkAndGetNextWatermark(T lastElement, long extractedTimestamp) {
            return new Watermark(extractedTimestamp);
        }
    }

    @Test
    public void operatorTest() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Long> src = env.generateSequence(0, 0);

        MapFunction<Long, Integer> mapFunction =
                new MapFunction<Long, Integer>() {
                    @Override
                    public Integer map(Long value) throws Exception {
                        return null;
                    }
                };
        DataStream<Integer> map = src.map(mapFunction);
        map.addSink(new DiscardingSink<Integer>());
        assertEquals(mapFunction, getFunctionForDataStream(map));

        FlatMapFunction<Long, Integer> flatMapFunction =
                new FlatMapFunction<Long, Integer>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public void flatMap(Long value, Collector<Integer> out) throws Exception {}
                };
        DataStream<Integer> flatMap = src.flatMap(flatMapFunction);
        flatMap.addSink(new DiscardingSink<Integer>());
        assertEquals(flatMapFunction, getFunctionForDataStream(flatMap));

        FilterFunction<Integer> filterFunction =
                new FilterFunction<Integer>() {
                    @Override
                    public boolean filter(Integer value) throws Exception {
                        return false;
                    }
                };

        DataStream<Integer> unionFilter = map.union(flatMap).filter(filterFunction);

        unionFilter.addSink(new DiscardingSink<Integer>());

        assertEquals(filterFunction, getFunctionForDataStream(unionFilter));

        try {
            getStreamGraph(env).getStreamEdgesOrThrow(map.getId(), unionFilter.getId());
        } catch (RuntimeException e) {
            fail(e.getMessage());
        }

        try {
            getStreamGraph(env).getStreamEdgesOrThrow(flatMap.getId(), unionFilter.getId());
        } catch (RuntimeException e) {
            fail(e.getMessage());
        }

        ConnectedStreams<Integer, Integer> connect = map.connect(flatMap);
        CoMapFunction<Integer, Integer, String> coMapper =
                new CoMapFunction<Integer, Integer, String>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public String map1(Integer value) {
                        return null;
                    }

                    @Override
                    public String map2(Integer value) {
                        return null;
                    }
                };
        DataStream<String> coMap = connect.map(coMapper);
        coMap.addSink(new DiscardingSink<String>());
        assertEquals(coMapper, getFunctionForDataStream(coMap));

        try {
            getStreamGraph(env).getStreamEdgesOrThrow(map.getId(), coMap.getId());
        } catch (RuntimeException e) {
            fail(e.getMessage());
        }

        try {
            getStreamGraph(env).getStreamEdgesOrThrow(flatMap.getId(), coMap.getId());
        } catch (RuntimeException e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testKeyedConnectedStreamsType() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> stream1 = env.fromElements(1, 2);
        DataStreamSource<Integer> stream2 = env.fromElements(1, 2);

        ConnectedStreams<Integer, Integer> connectedStreams =
                stream1.connect(stream2).keyBy(v -> v, v -> v);

        KeyedStream<?, ?> firstKeyedInput = (KeyedStream<?, ?>) connectedStreams.getFirstInput();
        KeyedStream<?, ?> secondKeyedInput = (KeyedStream<?, ?>) connectedStreams.getSecondInput();
        assertThat(firstKeyedInput.getKeyType(), equalTo(Types.INT));
        assertThat(secondKeyedInput.getKeyType(), equalTo(Types.INT));
    }

    @Test
    public void sinkKeyTest() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSink<Long> sink = env.generateSequence(1, 100).print();
        assertEquals(
                0,
                getStreamGraph(env)
                        .getStreamNode(sink.getTransformation().getId())
                        .getStatePartitioners()
                        .length);
        assertTrue(
                getStreamGraph(env)
                                .getStreamNode(sink.getTransformation().getId())
                                .getInEdges()
                                .get(0)
                                .getPartitioner()
                        instanceof ForwardPartitioner);

        KeySelector<Long, Long> key1 =
                new KeySelector<Long, Long>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Long getKey(Long value) throws Exception {
                        return (long) 0;
                    }
                };

        DataStreamSink<Long> sink2 = env.generateSequence(1, 100).keyBy(key1).print();

        assertEquals(
                1,
                getStreamGraph(env)
                        .getStreamNode(sink2.getTransformation().getId())
                        .getStatePartitioners()
                        .length);
        assertNotNull(
                getStreamGraph(env)
                        .getStreamNode(sink2.getTransformation().getId())
                        .getStateKeySerializer());
        assertNotNull(
                getStreamGraph(env)
                        .getStreamNode(sink2.getTransformation().getId())
                        .getStateKeySerializer());
        assertEquals(
                key1,
                getStreamGraph(env)
                        .getStreamNode(sink2.getTransformation().getId())
                        .getStatePartitioners()[0]);
        assertTrue(
                getStreamGraph(env)
                                .getStreamNode(sink2.getTransformation().getId())
                                .getInEdges()
                                .get(0)
                                .getPartitioner()
                        instanceof KeyGroupStreamPartitioner);

        KeySelector<Long, Long> key2 =
                new KeySelector<Long, Long>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Long getKey(Long value) throws Exception {
                        return (long) 0;
                    }
                };

        DataStreamSink<Long> sink3 = env.generateSequence(1, 100).keyBy(key2).print();

        assertEquals(
                1,
                getStreamGraph(env)
                        .getStreamNode(sink3.getTransformation().getId())
                        .getStatePartitioners()
                        .length);
        assertEquals(
                key2,
                getStreamGraph(env)
                        .getStreamNode(sink3.getTransformation().getId())
                        .getStatePartitioners()[0]);
        assertTrue(
                getStreamGraph(env)
                                .getStreamNode(sink3.getTransformation().getId())
                                .getInEdges()
                                .get(0)
                                .getPartitioner()
                        instanceof KeyGroupStreamPartitioner);
    }

    @Test
    public void testChannelSelectors() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Long> src = env.generateSequence(0, 0);

        DataStream<Long> broadcast = src.broadcast();
        DataStreamSink<Long> broadcastSink = broadcast.print();
        StreamPartitioner<?> broadcastPartitioner =
                getStreamGraph(env)
                        .getStreamEdges(src.getId(), broadcastSink.getTransformation().getId())
                        .get(0)
                        .getPartitioner();
        assertTrue(broadcastPartitioner instanceof BroadcastPartitioner);

        DataStream<Long> shuffle = src.shuffle();
        DataStreamSink<Long> shuffleSink = shuffle.print();
        StreamPartitioner<?> shufflePartitioner =
                getStreamGraph(env)
                        .getStreamEdges(src.getId(), shuffleSink.getTransformation().getId())
                        .get(0)
                        .getPartitioner();
        assertTrue(shufflePartitioner instanceof ShufflePartitioner);

        DataStream<Long> forward = src.forward();
        DataStreamSink<Long> forwardSink = forward.print();
        StreamPartitioner<?> forwardPartitioner =
                getStreamGraph(env)
                        .getStreamEdges(src.getId(), forwardSink.getTransformation().getId())
                        .get(0)
                        .getPartitioner();
        assertTrue(forwardPartitioner instanceof ForwardPartitioner);

        DataStream<Long> rebalance = src.rebalance();
        DataStreamSink<Long> rebalanceSink = rebalance.print();
        StreamPartitioner<?> rebalancePartitioner =
                getStreamGraph(env)
                        .getStreamEdges(src.getId(), rebalanceSink.getTransformation().getId())
                        .get(0)
                        .getPartitioner();
        assertTrue(rebalancePartitioner instanceof RebalancePartitioner);

        DataStream<Long> global = src.global();
        DataStreamSink<Long> globalSink = global.print();
        StreamPartitioner<?> globalPartitioner =
                getStreamGraph(env)
                        .getStreamEdges(src.getId(), globalSink.getTransformation().getId())
                        .get(0)
                        .getPartitioner();
        assertTrue(globalPartitioner instanceof GlobalPartitioner);
    }

    /////////////////////////////////////////////////////////////
    // KeyBy testing
    /////////////////////////////////////////////////////////////

    @Test
    public void testPrimitiveArrayKeyRejection() {

        KeySelector<Tuple2<Integer[], String>, int[]> keySelector =
                new KeySelector<Tuple2<Integer[], String>, int[]>() {

                    @Override
                    public int[] getKey(Tuple2<Integer[], String> value) throws Exception {
                        int[] ks = new int[value.f0.length];
                        for (int i = 0; i < ks.length; i++) {
                            ks[i] = value.f0[i];
                        }
                        return ks;
                    }
                };

        assertArrayKeyRejection(keySelector, PrimitiveArrayTypeInfo.INT_PRIMITIVE_ARRAY_TYPE_INFO);
    }

    @Test
    public void testBasicArrayKeyRejection() {

        KeySelector<Tuple2<Integer[], String>, Integer[]> keySelector =
                new KeySelector<Tuple2<Integer[], String>, Integer[]>() {

                    @Override
                    public Integer[] getKey(Tuple2<Integer[], String> value) throws Exception {
                        return value.f0;
                    }
                };

        assertArrayKeyRejection(keySelector, BasicArrayTypeInfo.INT_ARRAY_TYPE_INFO);
    }

    @Test
    public void testObjectArrayKeyRejection() {

        KeySelector<Tuple2<Integer[], String>, Object[]> keySelector =
                new KeySelector<Tuple2<Integer[], String>, Object[]>() {

                    @Override
                    public Object[] getKey(Tuple2<Integer[], String> value) throws Exception {
                        Object[] ks = new Object[value.f0.length];
                        for (int i = 0; i < ks.length; i++) {
                            ks[i] = new Object();
                        }
                        return ks;
                    }
                };

        ObjectArrayTypeInfo<Object[], Object> keyTypeInfo =
                ObjectArrayTypeInfo.getInfoFor(Object[].class, new GenericTypeInfo<>(Object.class));

        assertArrayKeyRejection(keySelector, keyTypeInfo);
    }

    private <K> void assertArrayKeyRejection(
            KeySelector<Tuple2<Integer[], String>, K> keySelector,
            TypeInformation<K> expectedKeyType) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<Integer[], String>> input =
                env.fromElements(new Tuple2<>(new Integer[] {1, 2}, "barfoo"));

        Assert.assertEquals(
                expectedKeyType, TypeExtractor.getKeySelectorTypes(keySelector, input.getType()));

        // adjust the rule
        expectedException.expect(InvalidProgramException.class);
        expectedException.expectMessage(
                new StringStartsWith("Type " + expectedKeyType + " cannot be used as key."));

        input.keyBy(keySelector);
    }

    @Test
    public void testEnumKeyRejection() {
        KeySelector<Tuple2<TestEnum, String>, TestEnum> keySelector = value -> value.f0;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<TestEnum, String>> input =
                env.fromElements(Tuple2.of(TestEnum.FOO, "Foo"), Tuple2.of(TestEnum.BAR, "Bar"));

        expectedException.expect(InvalidProgramException.class);
        expectedException.expectMessage(
                new StringStartsWith(
                        "Type " + EnumTypeInfo.of(TestEnum.class) + " cannot be used as key."));

        input.keyBy(keySelector);
    }

    ////////////////			Composite Key Tests : POJOs			////////////////

    @Test
    public void testPOJOWithNestedArrayNoHashCodeKeyRejection() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<POJOWithHashCode> input =
                env.fromElements(new POJOWithHashCode(new int[] {1, 2}));

        TypeInformation<?> expectedTypeInfo =
                new TupleTypeInfo<Tuple1<int[]>>(
                        PrimitiveArrayTypeInfo.INT_PRIMITIVE_ARRAY_TYPE_INFO);

        // adjust the rule
        expectedException.expect(InvalidProgramException.class);
        expectedException.expectMessage(
                new StringStartsWith("Type " + expectedTypeInfo + " cannot be used as key."));

        input.keyBy("id");
    }

    @Test
    public void testPOJOWithNestedArrayAndHashCodeWorkAround() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<POJOWithHashCode> input =
                env.fromElements(new POJOWithHashCode(new int[] {1, 2}));

        input.keyBy(
                        new KeySelector<POJOWithHashCode, POJOWithHashCode>() {
                            @Override
                            public POJOWithHashCode getKey(POJOWithHashCode value)
                                    throws Exception {
                                return value;
                            }
                        })
                .addSink(
                        new SinkFunction<POJOWithHashCode>() {
                            @Override
                            public void invoke(POJOWithHashCode value) throws Exception {
                                Assert.assertEquals(value.getId(), new int[] {1, 2});
                            }
                        });
    }

    @Test
    public void testPOJOnoHashCodeKeyRejection() {

        KeySelector<POJOWithoutHashCode, POJOWithoutHashCode> keySelector =
                new KeySelector<POJOWithoutHashCode, POJOWithoutHashCode>() {
                    @Override
                    public POJOWithoutHashCode getKey(POJOWithoutHashCode value) throws Exception {
                        return value;
                    }
                };

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<POJOWithoutHashCode> input =
                env.fromElements(new POJOWithoutHashCode(new int[] {1, 2}));

        // adjust the rule
        expectedException.expect(InvalidProgramException.class);

        input.keyBy(keySelector);
    }

    ////////////////			Composite Key Tests : Tuples			////////////////

    @Test
    public void testTupleNestedArrayKeyRejection() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<Integer[], String>> input =
                env.fromElements(new Tuple2<>(new Integer[] {1, 2}, "test-test"));

        TypeInformation<?> expectedTypeInfo =
                new TupleTypeInfo<Tuple2<Integer[], String>>(
                        BasicArrayTypeInfo.INT_ARRAY_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);

        // adjust the rule
        expectedException.expect(InvalidProgramException.class);
        expectedException.expectMessage(
                new StringStartsWith("Type " + expectedTypeInfo + " cannot be used as key."));

        input.keyBy(
                new KeySelector<Tuple2<Integer[], String>, Tuple2<Integer[], String>>() {
                    @Override
                    public Tuple2<Integer[], String> getKey(Tuple2<Integer[], String> value)
                            throws Exception {
                        return value;
                    }
                });
    }

    @Test
    public void testPrimitiveKeyAcceptance() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setMaxParallelism(1);

        DataStream<Integer> input = env.fromElements(new Integer(10000));

        KeyedStream<Integer, Object> keyedStream =
                input.keyBy(
                        new KeySelector<Integer, Object>() {
                            @Override
                            public Object getKey(Integer value) throws Exception {
                                return value;
                            }
                        });

        keyedStream.addSink(
                new SinkFunction<Integer>() {
                    @Override
                    public void invoke(Integer value) throws Exception {
                        Assert.assertEquals(10000L, (long) value);
                    }
                });
    }

    /** POJO without hashCode. */
    public static class POJOWithoutHashCode {

        private int[] id;

        public POJOWithoutHashCode() {}

        public POJOWithoutHashCode(int[] id) {
            this.id = id;
        }

        public int[] getId() {
            return id;
        }

        public void setId(int[] id) {
            this.id = id;
        }
    }

    /** POJO with hashCode. */
    public static class POJOWithHashCode extends POJOWithoutHashCode {

        public POJOWithHashCode() {}

        public POJOWithHashCode(int[] id) {
            super(id);
        }

        @Override
        public int hashCode() {
            int hash = 31;
            for (int i : getId()) {
                hash = 37 * hash + i;
            }
            return hash;
        }
    }

    /////////////////////////////////////////////////////////////
    // Utilities
    /////////////////////////////////////////////////////////////

    private static StreamOperator<?> getOperatorForDataStream(DataStream<?> dataStream) {
        StreamExecutionEnvironment env = dataStream.getExecutionEnvironment();
        StreamGraph streamGraph = getStreamGraph(env);
        return streamGraph.getStreamNode(dataStream.getId()).getOperator();
    }

    private static Function getFunctionForDataStream(DataStream<?> dataStream) {
        AbstractUdfStreamOperator<?, ?> operator =
                (AbstractUdfStreamOperator<?, ?>) getOperatorForDataStream(dataStream);
        return operator.getUserFunction();
    }

    /** Returns the StreamGraph without clearing the transformations. */
    private static StreamGraph getStreamGraph(StreamExecutionEnvironment sEnv) {
        return sEnv.getStreamGraph(false);
    }

    private static Integer createDownStreamId(DataStream<?> dataStream) {
        return dataStream.print().getTransformation().getId();
    }

    private static boolean isKeyed(DataStream<?> dataStream) {
        return dataStream instanceof KeyedStream;
    }

    @SuppressWarnings("rawtypes,unchecked")
    private static Integer createDownStreamId(ConnectedStreams dataStream) {
        SingleOutputStreamOperator<?> coMap =
                dataStream.map(
                        new CoMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Object>() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public Object map1(Tuple2<Long, Long> value) {
                                return null;
                            }

                            @Override
                            public Object map2(Tuple2<Long, Long> value) {
                                return null;
                            }
                        });
        coMap.addSink(new DiscardingSink());
        return coMap.getId();
    }

    private static boolean isKeyed(ConnectedStreams<?, ?> dataStream) {
        return (dataStream.getFirstInput() instanceof KeyedStream
                && dataStream.getSecondInput() instanceof KeyedStream);
    }

    private static boolean isPartitioned(List<StreamEdge> edges) {
        boolean result = true;
        for (StreamEdge edge : edges) {
            if (!(edge.getPartitioner() instanceof KeyGroupStreamPartitioner)) {
                result = false;
            }
        }
        return result;
    }

    private static boolean isCustomPartitioned(List<StreamEdge> edges) {
        boolean result = true;
        for (StreamEdge edge : edges) {
            if (!(edge.getPartitioner() instanceof CustomPartitionerWrapper)) {
                result = false;
            }
        }
        return result;
    }

    private static class FirstSelector implements KeySelector<Tuple2<Long, Long>, Long> {
        private static final long serialVersionUID = 1L;

        @Override
        public Long getKey(Tuple2<Long, Long> value) throws Exception {
            return value.f0;
        }
    }

    private static class IdentityKeySelector<T> implements KeySelector<T, T> {
        private static final long serialVersionUID = 1L;

        @Override
        public T getKey(T value) throws Exception {
            return value;
        }
    }

    private static class CustomPOJO {
        private String s;
        private int i;

        public CustomPOJO() {}

        public void setS(String s) {
            this.s = s;
        }

        public void setI(int i) {
            this.i = i;
        }

        public String getS() {
            return s;
        }

        public int getI() {
            return i;
        }
    }

    private enum TestEnum {
        FOO,
        BAR
    }
}
