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
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.test.streaming.runtime.util.TestListResultSink;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

/** Integration test for streaming programs using side outputs. */
public class SideOutputITCase extends AbstractTestBase implements Serializable {

    @Rule public transient ExpectedException expectedException = ExpectedException.none();

    static List<Integer> elements = new ArrayList<>();

    static {
        elements.add(1);
        elements.add(2);
        elements.add(5);
        elements.add(3);
        elements.add(4);
    }

    /** Verify that watermarks are forwarded to all side outputs. */
    @Test
    public void testWatermarkForwarding() throws Exception {
        final OutputTag<String> sideOutputTag1 = new OutputTag<String>("side") {};
        final OutputTag<String> sideOutputTag2 = new OutputTag<String>("other-side") {};

        TestListResultSink<String> sideOutputResultSink1 = new TestListResultSink<>();
        TestListResultSink<String> sideOutputResultSink2 = new TestListResultSink<>();
        TestListResultSink<String> resultSink = new TestListResultSink<>();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        DataStream<Integer> dataStream =
                env.addSource(
                        new SourceFunction<Integer>() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public void run(SourceContext<Integer> ctx) throws Exception {
                                ctx.collectWithTimestamp(1, 0);
                                ctx.emitWatermark(new Watermark(0));
                                ctx.collectWithTimestamp(2, 1);
                                ctx.collectWithTimestamp(5, 2);
                                ctx.emitWatermark(new Watermark(2));
                                ctx.collectWithTimestamp(3, 3);
                                ctx.collectWithTimestamp(4, 4);
                            }

                            @Override
                            public void cancel() {}
                        });

        SingleOutputStreamOperator<Integer> passThroughtStream =
                dataStream.process(
                        new ProcessFunction<Integer, Integer>() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public void processElement(
                                    Integer value, Context ctx, Collector<Integer> out)
                                    throws Exception {
                                out.collect(value);
                                ctx.output(sideOutputTag1, "sideout-" + String.valueOf(value));
                            }
                        });

        class WatermarkReifier extends AbstractStreamOperator<String>
                implements OneInputStreamOperator<String, String> {
            private static final long serialVersionUID = 1L;

            @Override
            public void processElement(StreamRecord<String> element) throws Exception {
                output.collect(new StreamRecord<>("E:" + element.getValue()));
            }

            @Override
            public void processWatermark(Watermark mark) throws Exception {
                super.processWatermark(mark);
                output.collect(new StreamRecord<>("WM:" + mark.getTimestamp()));
            }
        }

        passThroughtStream
                .getSideOutput(sideOutputTag1)
                .transform(
                        "ReifyWatermarks", BasicTypeInfo.STRING_TYPE_INFO, new WatermarkReifier())
                .addSink(sideOutputResultSink1);

        passThroughtStream
                .getSideOutput(sideOutputTag2)
                .transform(
                        "ReifyWatermarks", BasicTypeInfo.STRING_TYPE_INFO, new WatermarkReifier())
                .addSink(sideOutputResultSink2);

        passThroughtStream
                .map(
                        new MapFunction<Integer, String>() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public String map(Integer value) throws Exception {
                                return value.toString();
                            }
                        })
                .transform(
                        "ReifyWatermarks", BasicTypeInfo.STRING_TYPE_INFO, new WatermarkReifier())
                .addSink(resultSink);

        env.execute();

        assertEquals(
                Arrays.asList(
                        "E:sideout-1",
                        "E:sideout-2",
                        "E:sideout-3",
                        "E:sideout-4",
                        "E:sideout-5",
                        "WM:0",
                        "WM:0",
                        "WM:0",
                        "WM:2",
                        "WM:2",
                        "WM:2",
                        "WM:" + Long.MAX_VALUE,
                        "WM:" + Long.MAX_VALUE,
                        "WM:" + Long.MAX_VALUE),
                sideOutputResultSink1.getSortedResult());

        assertEquals(
                Arrays.asList(
                        "E:sideout-1",
                        "E:sideout-2",
                        "E:sideout-3",
                        "E:sideout-4",
                        "E:sideout-5",
                        "WM:0",
                        "WM:0",
                        "WM:0",
                        "WM:2",
                        "WM:2",
                        "WM:2",
                        "WM:" + Long.MAX_VALUE,
                        "WM:" + Long.MAX_VALUE,
                        "WM:" + Long.MAX_VALUE),
                sideOutputResultSink1.getSortedResult());

        assertEquals(
                Arrays.asList(
                        "E:1",
                        "E:2",
                        "E:3",
                        "E:4",
                        "E:5",
                        "WM:0",
                        "WM:0",
                        "WM:0",
                        "WM:2",
                        "WM:2",
                        "WM:2",
                        "WM:" + Long.MAX_VALUE,
                        "WM:" + Long.MAX_VALUE,
                        "WM:" + Long.MAX_VALUE),
                resultSink.getSortedResult());
    }

    @Test
    public void testSideOutputWithMultipleConsumers() throws Exception {
        final OutputTag<String> sideOutputTag = new OutputTag<String>("side") {};

        TestListResultSink<String> sideOutputResultSink1 = new TestListResultSink<>();
        TestListResultSink<String> sideOutputResultSink2 = new TestListResultSink<>();
        TestListResultSink<Integer> resultSink = new TestListResultSink<>();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        DataStream<Integer> dataStream = env.fromCollection(elements);

        SingleOutputStreamOperator<Integer> passThroughtStream =
                dataStream.process(
                        new ProcessFunction<Integer, Integer>() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public void processElement(
                                    Integer value, Context ctx, Collector<Integer> out)
                                    throws Exception {
                                out.collect(value);
                                ctx.output(sideOutputTag, "sideout-" + String.valueOf(value));
                            }
                        });

        passThroughtStream.getSideOutput(sideOutputTag).addSink(sideOutputResultSink1);
        passThroughtStream.getSideOutput(sideOutputTag).addSink(sideOutputResultSink2);
        passThroughtStream.addSink(resultSink);
        env.execute();

        assertEquals(
                Arrays.asList("sideout-1", "sideout-2", "sideout-3", "sideout-4", "sideout-5"),
                sideOutputResultSink1.getSortedResult());
        assertEquals(
                Arrays.asList("sideout-1", "sideout-2", "sideout-3", "sideout-4", "sideout-5"),
                sideOutputResultSink2.getSortedResult());
        assertEquals(Arrays.asList(1, 2, 3, 4, 5), resultSink.getSortedResult());
    }

    @Test
    public void testSideOutputWithMultipleConsumersWithObjectReuse() throws Exception {
        final OutputTag<String> sideOutputTag = new OutputTag<String>("side") {};

        TestListResultSink<String> sideOutputResultSink1 = new TestListResultSink<>();
        TestListResultSink<String> sideOutputResultSink2 = new TestListResultSink<>();
        TestListResultSink<Integer> resultSink = new TestListResultSink<>();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();
        env.setParallelism(3);

        DataStream<Integer> dataStream = env.fromCollection(elements);

        SingleOutputStreamOperator<Integer> passThroughtStream =
                dataStream.process(
                        new ProcessFunction<Integer, Integer>() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public void processElement(
                                    Integer value, Context ctx, Collector<Integer> out)
                                    throws Exception {
                                out.collect(value);
                                ctx.output(sideOutputTag, "sideout-" + String.valueOf(value));
                            }
                        });

        passThroughtStream.getSideOutput(sideOutputTag).addSink(sideOutputResultSink1);
        passThroughtStream.getSideOutput(sideOutputTag).addSink(sideOutputResultSink2);
        passThroughtStream.addSink(resultSink);
        env.execute();

        assertEquals(
                Arrays.asList("sideout-1", "sideout-2", "sideout-3", "sideout-4", "sideout-5"),
                sideOutputResultSink1.getSortedResult());
        assertEquals(
                Arrays.asList("sideout-1", "sideout-2", "sideout-3", "sideout-4", "sideout-5"),
                sideOutputResultSink2.getSortedResult());
        assertEquals(Arrays.asList(1, 2, 3, 4, 5), resultSink.getSortedResult());
    }

    @Test
    public void testDifferentSideOutputTypes() throws Exception {
        final OutputTag<String> sideOutputTag1 = new OutputTag<String>("string") {};
        final OutputTag<Integer> sideOutputTag2 = new OutputTag<Integer>("int") {};

        TestListResultSink<String> sideOutputResultSink1 = new TestListResultSink<>();
        TestListResultSink<Integer> sideOutputResultSink2 = new TestListResultSink<>();
        TestListResultSink<Integer> resultSink = new TestListResultSink<>();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();
        env.setParallelism(3);

        DataStream<Integer> dataStream = env.fromCollection(elements);

        SingleOutputStreamOperator<Integer> passThroughtStream =
                dataStream.process(
                        new ProcessFunction<Integer, Integer>() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public void processElement(
                                    Integer value, Context ctx, Collector<Integer> out)
                                    throws Exception {
                                out.collect(value);
                                ctx.output(sideOutputTag1, "sideout-" + String.valueOf(value));
                                ctx.output(sideOutputTag2, 13);
                            }
                        });

        passThroughtStream.getSideOutput(sideOutputTag1).addSink(sideOutputResultSink1);
        passThroughtStream.getSideOutput(sideOutputTag2).addSink(sideOutputResultSink2);
        passThroughtStream.addSink(resultSink);
        env.execute();

        assertEquals(
                Arrays.asList("sideout-1", "sideout-2", "sideout-3", "sideout-4", "sideout-5"),
                sideOutputResultSink1.getSortedResult());
        assertEquals(Arrays.asList(13, 13, 13, 13, 13), sideOutputResultSink2.getSortedResult());
        assertEquals(Arrays.asList(1, 2, 3, 4, 5), resultSink.getSortedResult());
    }

    @Test
    public void testSideOutputNameClash() throws Exception {
        final OutputTag<String> sideOutputTag1 = new OutputTag<String>("side") {};
        final OutputTag<Integer> sideOutputTag2 = new OutputTag<Integer>("side") {};

        TestListResultSink<String> sideOutputResultSink1 = new TestListResultSink<>();
        TestListResultSink<Integer> sideOutputResultSink2 = new TestListResultSink<>();

        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.setParallelism(3);

        DataStream<Integer> dataStream = see.fromCollection(elements);

        SingleOutputStreamOperator<Integer> passThroughtStream =
                dataStream.process(
                        new ProcessFunction<Integer, Integer>() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public void processElement(
                                    Integer value, Context ctx, Collector<Integer> out)
                                    throws Exception {
                                out.collect(value);
                                ctx.output(sideOutputTag1, "sideout-" + String.valueOf(value));
                                ctx.output(sideOutputTag2, 13);
                            }
                        });

        passThroughtStream.getSideOutput(sideOutputTag1).addSink(sideOutputResultSink1);

        expectedException.expect(UnsupportedOperationException.class);
        passThroughtStream.getSideOutput(sideOutputTag2).addSink(sideOutputResultSink2);
    }

    /** Test ProcessFunction side output. */
    @Test
    public void testProcessFunctionSideOutput() throws Exception {
        final OutputTag<String> sideOutputTag = new OutputTag<String>("side") {};

        TestListResultSink<String> sideOutputResultSink = new TestListResultSink<>();
        TestListResultSink<Integer> resultSink = new TestListResultSink<>();

        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.setParallelism(3);

        DataStream<Integer> dataStream = see.fromCollection(elements);

        SingleOutputStreamOperator<Integer> passThroughtStream =
                dataStream.process(
                        new ProcessFunction<Integer, Integer>() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public void processElement(
                                    Integer value, Context ctx, Collector<Integer> out)
                                    throws Exception {
                                out.collect(value);
                                ctx.output(sideOutputTag, "sideout-" + String.valueOf(value));
                            }
                        });

        passThroughtStream.getSideOutput(sideOutputTag).addSink(sideOutputResultSink);
        passThroughtStream.addSink(resultSink);
        see.execute();

        assertEquals(
                Arrays.asList("sideout-1", "sideout-2", "sideout-3", "sideout-4", "sideout-5"),
                sideOutputResultSink.getSortedResult());
        assertEquals(Arrays.asList(1, 2, 3, 4, 5), resultSink.getSortedResult());
    }

    /** Test CoProcessFunction side output. */
    @Test
    public void testCoProcessFunctionSideOutput() throws Exception {
        final OutputTag<String> sideOutputTag = new OutputTag<String>("side") {};

        TestListResultSink<String> sideOutputResultSink = new TestListResultSink<>();
        TestListResultSink<Integer> resultSink = new TestListResultSink<>();

        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.setParallelism(3);

        DataStream<Integer> ds1 = see.fromCollection(elements);
        DataStream<Integer> ds2 = see.fromCollection(elements);

        SingleOutputStreamOperator<Integer> passThroughtStream =
                ds1.connect(ds2)
                        .process(
                                new CoProcessFunction<Integer, Integer, Integer>() {
                                    @Override
                                    public void processElement1(
                                            Integer value, Context ctx, Collector<Integer> out)
                                            throws Exception {
                                        if (value < 3) {
                                            out.collect(value);
                                            ctx.output(
                                                    sideOutputTag,
                                                    "sideout1-" + String.valueOf(value));
                                        }
                                    }

                                    @Override
                                    public void processElement2(
                                            Integer value, Context ctx, Collector<Integer> out)
                                            throws Exception {
                                        if (value >= 3) {
                                            out.collect(value);
                                            ctx.output(
                                                    sideOutputTag,
                                                    "sideout2-" + String.valueOf(value));
                                        }
                                    }
                                });

        passThroughtStream.getSideOutput(sideOutputTag).addSink(sideOutputResultSink);
        passThroughtStream.addSink(resultSink);
        see.execute();

        assertEquals(
                Arrays.asList("sideout1-1", "sideout1-2", "sideout2-3", "sideout2-4", "sideout2-5"),
                sideOutputResultSink.getSortedResult());
        assertEquals(Arrays.asList(1, 2, 3, 4, 5), resultSink.getSortedResult());
    }

    /** Test CoProcessFunction side output with multiple consumers. */
    @Test
    public void testCoProcessFunctionSideOutputWithMultipleConsumers() throws Exception {
        final OutputTag<String> sideOutputTag1 = new OutputTag<String>("side1") {};
        final OutputTag<String> sideOutputTag2 = new OutputTag<String>("side2") {};

        TestListResultSink<String> sideOutputResultSink1 = new TestListResultSink<>();
        TestListResultSink<String> sideOutputResultSink2 = new TestListResultSink<>();
        TestListResultSink<Integer> resultSink = new TestListResultSink<>();

        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.setParallelism(3);

        DataStream<Integer> ds1 = see.fromCollection(elements);
        DataStream<Integer> ds2 = see.fromCollection(elements);

        SingleOutputStreamOperator<Integer> passThroughtStream =
                ds1.connect(ds2)
                        .process(
                                new CoProcessFunction<Integer, Integer, Integer>() {
                                    @Override
                                    public void processElement1(
                                            Integer value, Context ctx, Collector<Integer> out)
                                            throws Exception {
                                        if (value < 4) {
                                            out.collect(value);
                                            ctx.output(
                                                    sideOutputTag1,
                                                    "sideout1-" + String.valueOf(value));
                                        }
                                    }

                                    @Override
                                    public void processElement2(
                                            Integer value, Context ctx, Collector<Integer> out)
                                            throws Exception {
                                        if (value >= 4) {
                                            out.collect(value);
                                            ctx.output(
                                                    sideOutputTag2,
                                                    "sideout2-" + String.valueOf(value));
                                        }
                                    }
                                });

        passThroughtStream.getSideOutput(sideOutputTag1).addSink(sideOutputResultSink1);
        passThroughtStream.getSideOutput(sideOutputTag2).addSink(sideOutputResultSink2);
        passThroughtStream.addSink(resultSink);
        see.execute();

        assertEquals(
                Arrays.asList("sideout1-1", "sideout1-2", "sideout1-3"),
                sideOutputResultSink1.getSortedResult());
        assertEquals(
                Arrays.asList("sideout2-4", "sideout2-5"), sideOutputResultSink2.getSortedResult());
        assertEquals(Arrays.asList(1, 2, 3, 4, 5), resultSink.getSortedResult());
    }

    /** Test keyed ProcessFunction side output. */
    @Test
    public void testKeyedProcessFunctionSideOutput() throws Exception {
        final OutputTag<String> sideOutputTag = new OutputTag<String>("side") {};

        TestListResultSink<String> sideOutputResultSink = new TestListResultSink<>();
        TestListResultSink<Integer> resultSink = new TestListResultSink<>();

        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.setParallelism(3);

        DataStream<Integer> dataStream = see.fromCollection(elements);

        SingleOutputStreamOperator<Integer> passThroughtStream =
                dataStream
                        .keyBy(
                                new KeySelector<Integer, Integer>() {
                                    private static final long serialVersionUID = 1L;

                                    @Override
                                    public Integer getKey(Integer value) throws Exception {
                                        return value;
                                    }
                                })
                        .process(
                                new ProcessFunction<Integer, Integer>() {
                                    private static final long serialVersionUID = 1L;

                                    @Override
                                    public void processElement(
                                            Integer value, Context ctx, Collector<Integer> out)
                                            throws Exception {
                                        out.collect(value);
                                        ctx.output(
                                                sideOutputTag, "sideout-" + String.valueOf(value));
                                    }
                                });

        passThroughtStream.getSideOutput(sideOutputTag).addSink(sideOutputResultSink);
        passThroughtStream.addSink(resultSink);
        see.execute();

        assertEquals(
                Arrays.asList("sideout-1", "sideout-2", "sideout-3", "sideout-4", "sideout-5"),
                sideOutputResultSink.getSortedResult());
        assertEquals(Arrays.asList(1, 2, 3, 4, 5), resultSink.getSortedResult());
    }

    /** Test keyed CoProcessFunction side output. */
    @Test
    public void testLegacyKeyedCoProcessFunctionSideOutput() throws Exception {
        final OutputTag<String> sideOutputTag = new OutputTag<String>("side") {};

        TestListResultSink<String> sideOutputResultSink = new TestListResultSink<>();
        TestListResultSink<Integer> resultSink = new TestListResultSink<>();

        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.setParallelism(3);

        DataStream<Integer> ds1 = see.fromCollection(elements);
        DataStream<Integer> ds2 = see.fromCollection(elements);

        SingleOutputStreamOperator<Integer> passThroughtStream =
                ds1.keyBy(i -> i)
                        .connect(ds2.keyBy(i -> i))
                        .process(
                                new CoProcessFunction<Integer, Integer, Integer>() {
                                    @Override
                                    public void processElement1(
                                            Integer value, Context ctx, Collector<Integer> out)
                                            throws Exception {
                                        if (value < 3) {
                                            out.collect(value);
                                            ctx.output(
                                                    sideOutputTag,
                                                    "sideout1-" + String.valueOf(value));
                                        }
                                    }

                                    @Override
                                    public void processElement2(
                                            Integer value, Context ctx, Collector<Integer> out)
                                            throws Exception {
                                        if (value >= 3) {
                                            out.collect(value);
                                            ctx.output(
                                                    sideOutputTag,
                                                    "sideout2-" + String.valueOf(value));
                                        }
                                    }
                                });

        passThroughtStream.getSideOutput(sideOutputTag).addSink(sideOutputResultSink);
        passThroughtStream.addSink(resultSink);
        see.execute();

        assertEquals(
                Arrays.asList("sideout1-1", "sideout1-2", "sideout2-3", "sideout2-4", "sideout2-5"),
                sideOutputResultSink.getSortedResult());
        assertEquals(Arrays.asList(1, 2, 3, 4, 5), resultSink.getSortedResult());
    }

    /** Test keyed KeyedCoProcessFunction side output. */
    @Test
    public void testKeyedCoProcessFunctionSideOutput() throws Exception {
        final OutputTag<String> sideOutputTag = new OutputTag<String>("side") {};

        TestListResultSink<String> sideOutputResultSink = new TestListResultSink<>();
        TestListResultSink<Integer> resultSink = new TestListResultSink<>();

        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.setParallelism(3);

        DataStream<Integer> ds1 = see.fromCollection(elements);
        DataStream<Integer> ds2 = see.fromCollection(elements);

        SingleOutputStreamOperator<Integer> passThroughtStream =
                ds1.keyBy(i -> i)
                        .connect(ds2.keyBy(i -> i))
                        .process(
                                new KeyedCoProcessFunction<Integer, Integer, Integer, Integer>() {
                                    @Override
                                    public void processElement1(
                                            Integer value, Context ctx, Collector<Integer> out)
                                            throws Exception {
                                        if (value < 3) {
                                            out.collect(value);
                                            ctx.output(
                                                    sideOutputTag,
                                                    "sideout1-"
                                                            + ctx.getCurrentKey()
                                                            + "-"
                                                            + String.valueOf(value));
                                        }
                                    }

                                    @Override
                                    public void processElement2(
                                            Integer value, Context ctx, Collector<Integer> out)
                                            throws Exception {
                                        if (value >= 3) {
                                            out.collect(value);
                                            ctx.output(
                                                    sideOutputTag,
                                                    "sideout2-"
                                                            + ctx.getCurrentKey()
                                                            + "-"
                                                            + String.valueOf(value));
                                        }
                                    }
                                });

        passThroughtStream.getSideOutput(sideOutputTag).addSink(sideOutputResultSink);
        passThroughtStream.addSink(resultSink);
        see.execute();

        assertEquals(
                Arrays.asList(
                        "sideout1-1-1",
                        "sideout1-2-2",
                        "sideout2-3-3",
                        "sideout2-4-4",
                        "sideout2-5-5"),
                sideOutputResultSink.getSortedResult());
        assertEquals(Arrays.asList(1, 2, 3, 4, 5), resultSink.getSortedResult());
    }

    /** Test keyed CoProcessFunction side output with multiple consumers. */
    @Test
    public void testLegacyKeyedCoProcessFunctionSideOutputWithMultipleConsumers() throws Exception {
        final OutputTag<String> sideOutputTag1 = new OutputTag<String>("side1") {};
        final OutputTag<String> sideOutputTag2 = new OutputTag<String>("side2") {};

        TestListResultSink<String> sideOutputResultSink1 = new TestListResultSink<>();
        TestListResultSink<String> sideOutputResultSink2 = new TestListResultSink<>();
        TestListResultSink<Integer> resultSink = new TestListResultSink<>();

        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.setParallelism(3);

        DataStream<Integer> ds1 = see.fromCollection(elements);
        DataStream<Integer> ds2 = see.fromCollection(elements);

        SingleOutputStreamOperator<Integer> passThroughtStream =
                ds1.keyBy(i -> i)
                        .connect(ds2.keyBy(i -> i))
                        .process(
                                new CoProcessFunction<Integer, Integer, Integer>() {
                                    @Override
                                    public void processElement1(
                                            Integer value, Context ctx, Collector<Integer> out)
                                            throws Exception {
                                        if (value < 4) {
                                            out.collect(value);
                                            ctx.output(
                                                    sideOutputTag1,
                                                    "sideout1-" + String.valueOf(value));
                                        }
                                    }

                                    @Override
                                    public void processElement2(
                                            Integer value, Context ctx, Collector<Integer> out)
                                            throws Exception {
                                        if (value >= 4) {
                                            out.collect(value);
                                            ctx.output(
                                                    sideOutputTag2,
                                                    "sideout2-" + String.valueOf(value));
                                        }
                                    }
                                });

        passThroughtStream.getSideOutput(sideOutputTag1).addSink(sideOutputResultSink1);
        passThroughtStream.getSideOutput(sideOutputTag2).addSink(sideOutputResultSink2);
        passThroughtStream.addSink(resultSink);
        see.execute();

        assertEquals(
                Arrays.asList("sideout1-1", "sideout1-2", "sideout1-3"),
                sideOutputResultSink1.getSortedResult());
        assertEquals(
                Arrays.asList("sideout2-4", "sideout2-5"), sideOutputResultSink2.getSortedResult());
        assertEquals(Arrays.asList(1, 2, 3, 4, 5), resultSink.getSortedResult());
    }

    /** Test keyed KeyedCoProcessFunction side output with multiple consumers. */
    @Test
    public void testKeyedCoProcessFunctionSideOutputWithMultipleConsumers() throws Exception {
        final OutputTag<String> sideOutputTag1 = new OutputTag<String>("side1") {};
        final OutputTag<String> sideOutputTag2 = new OutputTag<String>("side2") {};

        TestListResultSink<String> sideOutputResultSink1 = new TestListResultSink<>();
        TestListResultSink<String> sideOutputResultSink2 = new TestListResultSink<>();
        TestListResultSink<Integer> resultSink = new TestListResultSink<>();

        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.setParallelism(3);

        DataStream<Integer> ds1 = see.fromCollection(elements);
        DataStream<Integer> ds2 = see.fromCollection(elements);

        SingleOutputStreamOperator<Integer> passThroughtStream =
                ds1.keyBy(i -> i)
                        .connect(ds2.keyBy(i -> i))
                        .process(
                                new KeyedCoProcessFunction<Integer, Integer, Integer, Integer>() {
                                    @Override
                                    public void processElement1(
                                            Integer value, Context ctx, Collector<Integer> out)
                                            throws Exception {
                                        if (value < 4) {
                                            out.collect(value);
                                            ctx.output(
                                                    sideOutputTag1,
                                                    "sideout1-"
                                                            + ctx.getCurrentKey()
                                                            + "-"
                                                            + String.valueOf(value));
                                        }
                                    }

                                    @Override
                                    public void processElement2(
                                            Integer value, Context ctx, Collector<Integer> out)
                                            throws Exception {
                                        if (value >= 4) {
                                            out.collect(value);
                                            ctx.output(
                                                    sideOutputTag2,
                                                    "sideout2-"
                                                            + ctx.getCurrentKey()
                                                            + "-"
                                                            + String.valueOf(value));
                                        }
                                    }
                                });

        passThroughtStream.getSideOutput(sideOutputTag1).addSink(sideOutputResultSink1);
        passThroughtStream.getSideOutput(sideOutputTag2).addSink(sideOutputResultSink2);
        passThroughtStream.addSink(resultSink);
        see.execute();

        assertEquals(
                Arrays.asList("sideout1-1-1", "sideout1-2-2", "sideout1-3-3"),
                sideOutputResultSink1.getSortedResult());
        assertEquals(
                Arrays.asList("sideout2-4-4", "sideout2-5-5"),
                sideOutputResultSink2.getSortedResult());
        assertEquals(Arrays.asList(1, 2, 3, 4, 5), resultSink.getSortedResult());
    }

    /** Test ProcessFunction side outputs with wrong {@code OutputTag}. */
    @Test
    public void testProcessFunctionSideOutputWithWrongTag() throws Exception {
        final OutputTag<String> sideOutputTag1 = new OutputTag<String>("side") {};
        final OutputTag<String> sideOutputTag2 = new OutputTag<String>("other-side") {};

        TestListResultSink<String> sideOutputResultSink = new TestListResultSink<>();

        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.setParallelism(3);

        DataStream<Integer> dataStream = see.fromCollection(elements);

        dataStream
                .process(
                        new ProcessFunction<Integer, Integer>() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public void processElement(
                                    Integer value, Context ctx, Collector<Integer> out)
                                    throws Exception {
                                out.collect(value);
                                ctx.output(sideOutputTag2, "sideout-" + String.valueOf(value));
                            }
                        })
                .getSideOutput(sideOutputTag1)
                .addSink(sideOutputResultSink);

        see.execute();

        assertEquals(Arrays.asList(), sideOutputResultSink.getSortedResult());
    }

    private static class TestWatermarkAssigner
            implements AssignerWithPunctuatedWatermarks<Integer> {
        private static final long serialVersionUID = 1L;

        @Nullable
        @Override
        public Watermark checkAndGetNextWatermark(Integer lastElement, long extractedTimestamp) {
            return new Watermark(extractedTimestamp);
        }

        @Override
        public long extractTimestamp(Integer element, long previousElementTimestamp) {
            return Long.valueOf(element);
        }
    }

    private static class TestKeySelector implements KeySelector<Integer, Integer> {
        private static final long serialVersionUID = 1L;

        @Override
        public Integer getKey(Integer value) throws Exception {
            return value;
        }
    }

    /** Test window late arriving events stream. */
    @Test
    public void testAllWindowLateArrivingEvents() throws Exception {
        TestListResultSink<String> sideOutputResultSink = new TestListResultSink<>();

        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.setParallelism(1);

        DataStream<Integer> dataStream = see.fromCollection(elements);

        OutputTag<Integer> lateDataTag = new OutputTag<Integer>("late") {};

        SingleOutputStreamOperator<Integer> windowOperator =
                dataStream
                        .assignTimestampsAndWatermarks(new TestWatermarkAssigner())
                        .windowAll(
                                SlidingEventTimeWindows.of(
                                        Time.milliseconds(1), Time.milliseconds(1)))
                        .sideOutputLateData(lateDataTag)
                        .apply(
                                new AllWindowFunction<Integer, Integer, TimeWindow>() {
                                    private static final long serialVersionUID = 1L;

                                    @Override
                                    public void apply(
                                            TimeWindow window,
                                            Iterable<Integer> values,
                                            Collector<Integer> out)
                                            throws Exception {
                                        for (Integer val : values) {
                                            out.collect(val);
                                        }
                                    }
                                });

        windowOperator
                .getSideOutput(lateDataTag)
                .flatMap(
                        new FlatMapFunction<Integer, String>() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public void flatMap(Integer value, Collector<String> out)
                                    throws Exception {
                                out.collect("late-" + String.valueOf(value));
                            }
                        })
                .addSink(sideOutputResultSink);

        see.execute();
        assertEquals(sideOutputResultSink.getSortedResult(), Arrays.asList("late-3", "late-4"));
    }

    @Test
    public void testKeyedWindowLateArrivingEvents() throws Exception {
        TestListResultSink<String> resultSink = new TestListResultSink<>();
        TestListResultSink<Integer> lateResultSink = new TestListResultSink<>();

        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.setParallelism(3);

        DataStream<Integer> dataStream = see.fromCollection(elements);

        OutputTag<Integer> lateDataTag = new OutputTag<Integer>("late") {};

        SingleOutputStreamOperator<String> windowOperator =
                dataStream
                        .assignTimestampsAndWatermarks(new TestWatermarkAssigner())
                        .keyBy(new TestKeySelector())
                        .window(
                                SlidingEventTimeWindows.of(
                                        Time.milliseconds(1), Time.milliseconds(1)))
                        .allowedLateness(Time.milliseconds(2))
                        .sideOutputLateData(lateDataTag)
                        .apply(
                                new WindowFunction<Integer, String, Integer, TimeWindow>() {
                                    private static final long serialVersionUID = 1L;

                                    @Override
                                    public void apply(
                                            Integer key,
                                            TimeWindow window,
                                            Iterable<Integer> input,
                                            Collector<String> out)
                                            throws Exception {
                                        for (Integer val : input) {
                                            out.collect(
                                                    String.valueOf(key)
                                                            + "-"
                                                            + String.valueOf(val));
                                        }
                                    }
                                });

        windowOperator.addSink(resultSink);

        windowOperator.getSideOutput(lateDataTag).addSink(lateResultSink);

        see.execute();
        assertEquals(Arrays.asList("1-1", "2-2", "4-4", "5-5"), resultSink.getSortedResult());
        assertEquals(Collections.singletonList(3), lateResultSink.getSortedResult());
    }

    @Test
    public void testProcessdWindowFunctionSideOutput() throws Exception {
        TestListResultSink<Integer> resultSink = new TestListResultSink<>();
        TestListResultSink<String> sideOutputResultSink = new TestListResultSink<>();

        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.setParallelism(3);

        DataStream<Integer> dataStream = see.fromCollection(elements);

        OutputTag<String> sideOutputTag = new OutputTag<String>("side") {};

        SingleOutputStreamOperator<Integer> windowOperator =
                dataStream
                        .assignTimestampsAndWatermarks(new TestWatermarkAssigner())
                        .keyBy(new TestKeySelector())
                        .window(
                                SlidingEventTimeWindows.of(
                                        Time.milliseconds(1), Time.milliseconds(1)))
                        .process(
                                new ProcessWindowFunction<Integer, Integer, Integer, TimeWindow>() {
                                    private static final long serialVersionUID = 1L;

                                    @Override
                                    public void process(
                                            Integer integer,
                                            Context context,
                                            Iterable<Integer> elements,
                                            Collector<Integer> out)
                                            throws Exception {
                                        out.collect(integer);
                                        context.output(
                                                sideOutputTag,
                                                "sideout-" + String.valueOf(integer));
                                    }
                                });

        windowOperator.getSideOutput(sideOutputTag).addSink(sideOutputResultSink);
        windowOperator.addSink(resultSink);
        see.execute();

        assertEquals(
                Arrays.asList("sideout-1", "sideout-2", "sideout-5"),
                sideOutputResultSink.getSortedResult());
        assertEquals(Arrays.asList(1, 2, 5), resultSink.getSortedResult());
    }

    @Test
    public void testProcessAllWindowFunctionSideOutput() throws Exception {
        TestListResultSink<Integer> resultSink = new TestListResultSink<>();
        TestListResultSink<String> sideOutputResultSink = new TestListResultSink<>();

        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.setParallelism(1);

        DataStream<Integer> dataStream = see.fromCollection(elements);

        OutputTag<String> sideOutputTag = new OutputTag<String>("side") {};

        SingleOutputStreamOperator<Integer> windowOperator =
                dataStream
                        .assignTimestampsAndWatermarks(new TestWatermarkAssigner())
                        .windowAll(
                                SlidingEventTimeWindows.of(
                                        Time.milliseconds(1), Time.milliseconds(1)))
                        .process(
                                new ProcessAllWindowFunction<Integer, Integer, TimeWindow>() {
                                    private static final long serialVersionUID = 1L;

                                    @Override
                                    public void process(
                                            Context context,
                                            Iterable<Integer> elements,
                                            Collector<Integer> out)
                                            throws Exception {
                                        for (Integer e : elements) {
                                            out.collect(e);
                                            context.output(
                                                    sideOutputTag, "sideout-" + String.valueOf(e));
                                        }
                                    }
                                });

        windowOperator.getSideOutput(sideOutputTag).addSink(sideOutputResultSink);
        windowOperator.addSink(resultSink);
        see.execute();

        assertEquals(
                Arrays.asList("sideout-1", "sideout-2", "sideout-5"),
                sideOutputResultSink.getSortedResult());
        assertEquals(Arrays.asList(1, 2, 5), resultSink.getSortedResult());
    }

    @Test
    public void testUnionOfTwoSideOutputs() throws Exception {
        TestListResultSink<Integer> evensResultSink = new TestListResultSink<>();
        TestListResultSink<Integer> oddsResultSink = new TestListResultSink<>();
        TestListResultSink<Integer> oddsUEvensResultSink = new TestListResultSink<>();
        TestListResultSink<Integer> evensUOddsResultSink = new TestListResultSink<>();
        TestListResultSink<Integer> oddsUOddsResultSink = new TestListResultSink<>();
        TestListResultSink<Integer> evensUEvensResultSink = new TestListResultSink<>();
        TestListResultSink<Integer> oddsUEvensExternalResultSink = new TestListResultSink<>();
        TestListResultSink<Integer> resultSink = new TestListResultSink<>();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        DataStream<Integer> input = env.fromElements(1, 2, 3, 4);

        OutputTag<Integer> oddTag = new OutputTag<Integer>("odds") {};
        OutputTag<Integer> evenTag = new OutputTag<Integer>("even") {};

        SingleOutputStreamOperator<Integer> passThroughStream =
                input.process(
                        new ProcessFunction<Integer, Integer>() {
                            @Override
                            public void processElement(
                                    Integer value, Context ctx, Collector<Integer> out)
                                    throws Exception {
                                if (value % 2 != 0) {
                                    ctx.output(oddTag, value);
                                } else {
                                    ctx.output(evenTag, value);
                                }
                                out.collect(value);
                            }
                        });

        DataStream<Integer> evens = passThroughStream.getSideOutput(evenTag);
        DataStream<Integer> odds = passThroughStream.getSideOutput(oddTag);

        evens.addSink(evensResultSink);
        odds.addSink(oddsResultSink);
        passThroughStream.addSink(resultSink);

        odds.union(evens).addSink(oddsUEvensResultSink);
        evens.union(odds).addSink(evensUOddsResultSink);

        odds.union(odds).addSink(oddsUOddsResultSink);
        evens.union(evens).addSink(evensUEvensResultSink);

        odds.union(env.fromElements(2, 4)).addSink(oddsUEvensExternalResultSink);

        env.execute();

        assertEquals(Arrays.asList(1, 3), oddsResultSink.getSortedResult());

        assertEquals(Arrays.asList(2, 4), evensResultSink.getSortedResult());

        assertEquals(Arrays.asList(1, 2, 3, 4), resultSink.getSortedResult());

        assertEquals(Arrays.asList(1, 2, 3, 4), oddsUEvensResultSink.getSortedResult());

        assertEquals(Arrays.asList(1, 2, 3, 4), evensUOddsResultSink.getSortedResult());

        assertEquals(Arrays.asList(1, 1, 3, 3), oddsUOddsResultSink.getSortedResult());

        assertEquals(Arrays.asList(2, 2, 4, 4), evensUEvensResultSink.getSortedResult());

        assertEquals(Arrays.asList(1, 2, 3, 4), oddsUEvensExternalResultSink.getSortedResult());
    }
}
