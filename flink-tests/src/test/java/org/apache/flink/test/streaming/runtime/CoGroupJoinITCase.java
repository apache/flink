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

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.CoGroupedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.util.Collector;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** Integration tests for windowed join / coGroup operators. */
@SuppressWarnings("serial")
public class CoGroupJoinITCase extends AbstractTestBase {

    private static List<String> testResults;

    @Test
    public void testCoGroup() throws Exception {

        testResults = new ArrayList<>();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Tuple2<String, Integer>> source1 =
                env.addSource(
                                new SourceFunction<Tuple2<String, Integer>>() {
                                    private static final long serialVersionUID = 1L;

                                    @Override
                                    public void run(SourceContext<Tuple2<String, Integer>> ctx)
                                            throws Exception {
                                        ctx.collect(Tuple2.of("a", 0));
                                        ctx.collect(Tuple2.of("a", 1));
                                        ctx.collect(Tuple2.of("a", 2));

                                        ctx.collect(Tuple2.of("b", 3));
                                        ctx.collect(Tuple2.of("b", 4));
                                        ctx.collect(Tuple2.of("b", 5));

                                        ctx.collect(Tuple2.of("a", 6));
                                        ctx.collect(Tuple2.of("a", 7));
                                        ctx.collect(Tuple2.of("a", 8));

                                        // source is finite, so it will have an implicit MAX
                                        // watermark when it finishes
                                    }

                                    @Override
                                    public void cancel() {}
                                })
                        .assignTimestampsAndWatermarks(new Tuple2TimestampExtractor());

        DataStream<Tuple2<String, Integer>> source2 =
                env.addSource(
                                new SourceFunction<Tuple2<String, Integer>>() {

                                    @Override
                                    public void run(SourceContext<Tuple2<String, Integer>> ctx)
                                            throws Exception {
                                        ctx.collect(Tuple2.of("a", 0));
                                        ctx.collect(Tuple2.of("a", 1));

                                        ctx.collect(Tuple2.of("b", 3));

                                        ctx.collect(Tuple2.of("c", 6));
                                        ctx.collect(Tuple2.of("c", 7));
                                        ctx.collect(Tuple2.of("c", 8));

                                        // source is finite, so it will have an implicit MAX
                                        // watermark when it finishes
                                    }

                                    @Override
                                    public void cancel() {}
                                })
                        .assignTimestampsAndWatermarks(new Tuple2TimestampExtractor());

        source1.coGroup(source2)
                .where(new Tuple2KeyExtractor())
                .equalTo(new Tuple2KeyExtractor())
                .window(TumblingEventTimeWindows.of(Time.of(3, TimeUnit.MILLISECONDS)))
                .apply(
                        new CoGroupFunction<
                                Tuple2<String, Integer>, Tuple2<String, Integer>, String>() {
                            @Override
                            public void coGroup(
                                    Iterable<Tuple2<String, Integer>> first,
                                    Iterable<Tuple2<String, Integer>> second,
                                    Collector<String> out)
                                    throws Exception {
                                StringBuilder result = new StringBuilder();
                                result.append("F:");
                                for (Tuple2<String, Integer> t : first) {
                                    result.append(t.toString());
                                }
                                result.append(" S:");
                                for (Tuple2<String, Integer> t : second) {
                                    result.append(t.toString());
                                }
                                out.collect(result.toString());
                            }
                        })
                .addSink(
                        new SinkFunction<String>() {
                            @Override
                            public void invoke(String value) throws Exception {
                                testResults.add(value);
                            }
                        });

        env.execute("CoGroup Test");

        List<String> expectedResult =
                Arrays.asList(
                        "F:(a,0)(a,1)(a,2) S:(a,0)(a,1)",
                        "F:(b,3)(b,4)(b,5) S:(b,3)",
                        "F:(a,6)(a,7)(a,8) S:",
                        "F: S:(c,6)(c,7)(c,8)");

        Collections.sort(expectedResult);
        Collections.sort(testResults);

        Assert.assertEquals(expectedResult, testResults);
    }

    @Test
    public void testJoin() throws Exception {

        testResults = new ArrayList<>();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Tuple3<String, String, Integer>> source1 =
                env.addSource(
                                new SourceFunction<Tuple3<String, String, Integer>>() {

                                    @Override
                                    public void run(
                                            SourceContext<Tuple3<String, String, Integer>> ctx)
                                            throws Exception {
                                        ctx.collect(Tuple3.of("a", "x", 0));
                                        ctx.collect(Tuple3.of("a", "y", 1));
                                        ctx.collect(Tuple3.of("a", "z", 2));

                                        ctx.collect(Tuple3.of("b", "u", 3));
                                        ctx.collect(Tuple3.of("b", "w", 5));

                                        ctx.collect(Tuple3.of("a", "i", 6));
                                        ctx.collect(Tuple3.of("a", "j", 7));
                                        ctx.collect(Tuple3.of("a", "k", 8));

                                        // source is finite, so it will have an implicit MAX
                                        // watermark when it finishes
                                    }

                                    @Override
                                    public void cancel() {}
                                })
                        .assignTimestampsAndWatermarks(new Tuple3TimestampExtractor());

        DataStream<Tuple3<String, String, Integer>> source2 =
                env.addSource(
                                new SourceFunction<Tuple3<String, String, Integer>>() {

                                    @Override
                                    public void run(
                                            SourceContext<Tuple3<String, String, Integer>> ctx)
                                            throws Exception {
                                        ctx.collect(Tuple3.of("a", "u", 0));
                                        ctx.collect(Tuple3.of("a", "w", 1));

                                        ctx.collect(Tuple3.of("b", "i", 3));
                                        ctx.collect(Tuple3.of("b", "k", 5));

                                        ctx.collect(Tuple3.of("a", "x", 6));
                                        ctx.collect(Tuple3.of("a", "z", 8));

                                        // source is finite, so it will have an implicit MAX
                                        // watermark when it finishes
                                    }

                                    @Override
                                    public void cancel() {}
                                })
                        .assignTimestampsAndWatermarks(new Tuple3TimestampExtractor());

        source1.join(source2)
                .where(new Tuple3KeyExtractor())
                .equalTo(new Tuple3KeyExtractor())
                .window(TumblingEventTimeWindows.of(Time.of(3, TimeUnit.MILLISECONDS)))
                .apply(
                        new JoinFunction<
                                Tuple3<String, String, Integer>,
                                Tuple3<String, String, Integer>,
                                String>() {
                            @Override
                            public String join(
                                    Tuple3<String, String, Integer> first,
                                    Tuple3<String, String, Integer> second)
                                    throws Exception {
                                return first + ":" + second;
                            }
                        })
                .addSink(
                        new SinkFunction<String>() {
                            @Override
                            public void invoke(String value) throws Exception {
                                testResults.add(value);
                            }
                        });

        env.execute("Join Test");

        List<String> expectedResult =
                Arrays.asList(
                        "(a,x,0):(a,u,0)",
                        "(a,x,0):(a,w,1)",
                        "(a,y,1):(a,u,0)",
                        "(a,y,1):(a,w,1)",
                        "(a,z,2):(a,u,0)",
                        "(a,z,2):(a,w,1)",
                        "(b,u,3):(b,i,3)",
                        "(b,u,3):(b,k,5)",
                        "(b,w,5):(b,i,3)",
                        "(b,w,5):(b,k,5)",
                        "(a,i,6):(a,x,6)",
                        "(a,i,6):(a,z,8)",
                        "(a,j,7):(a,x,6)",
                        "(a,j,7):(a,z,8)",
                        "(a,k,8):(a,x,6)",
                        "(a,k,8):(a,z,8)");

        Collections.sort(expectedResult);
        Collections.sort(testResults);

        Assert.assertEquals(expectedResult, testResults);
    }

    @Test
    public void testSelfJoin() throws Exception {

        testResults = new ArrayList<>();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Tuple3<String, String, Integer>> source1 =
                env.addSource(
                                new SourceFunction<Tuple3<String, String, Integer>>() {
                                    private static final long serialVersionUID = 1L;

                                    @Override
                                    public void run(
                                            SourceContext<Tuple3<String, String, Integer>> ctx)
                                            throws Exception {
                                        ctx.collect(Tuple3.of("a", "x", 0));
                                        ctx.collect(Tuple3.of("a", "y", 1));
                                        ctx.collect(Tuple3.of("a", "z", 2));

                                        ctx.collect(Tuple3.of("b", "u", 3));
                                        ctx.collect(Tuple3.of("b", "w", 5));

                                        ctx.collect(Tuple3.of("a", "i", 6));
                                        ctx.collect(Tuple3.of("a", "j", 7));
                                        ctx.collect(Tuple3.of("a", "k", 8));

                                        // source is finite, so it will have an implicit MAX
                                        // watermark when it finishes
                                    }

                                    @Override
                                    public void cancel() {}
                                })
                        .assignTimestampsAndWatermarks(new Tuple3TimestampExtractor());

        source1.join(source1)
                .where(new Tuple3KeyExtractor())
                .equalTo(new Tuple3KeyExtractor())
                .window(TumblingEventTimeWindows.of(Time.of(3, TimeUnit.MILLISECONDS)))
                .apply(
                        new JoinFunction<
                                Tuple3<String, String, Integer>,
                                Tuple3<String, String, Integer>,
                                String>() {
                            @Override
                            public String join(
                                    Tuple3<String, String, Integer> first,
                                    Tuple3<String, String, Integer> second)
                                    throws Exception {
                                return first + ":" + second;
                            }
                        })
                .addSink(
                        new SinkFunction<String>() {
                            @Override
                            public void invoke(String value) throws Exception {
                                testResults.add(value);
                            }
                        });

        env.execute("Self-Join Test");

        List<String> expectedResult =
                Arrays.asList(
                        "(a,x,0):(a,x,0)",
                        "(a,x,0):(a,y,1)",
                        "(a,x,0):(a,z,2)",
                        "(a,y,1):(a,x,0)",
                        "(a,y,1):(a,y,1)",
                        "(a,y,1):(a,z,2)",
                        "(a,z,2):(a,x,0)",
                        "(a,z,2):(a,y,1)",
                        "(a,z,2):(a,z,2)",
                        "(b,u,3):(b,u,3)",
                        "(b,u,3):(b,w,5)",
                        "(b,w,5):(b,u,3)",
                        "(b,w,5):(b,w,5)",
                        "(a,i,6):(a,i,6)",
                        "(a,i,6):(a,j,7)",
                        "(a,i,6):(a,k,8)",
                        "(a,j,7):(a,i,6)",
                        "(a,j,7):(a,j,7)",
                        "(a,j,7):(a,k,8)",
                        "(a,k,8):(a,i,6)",
                        "(a,k,8):(a,j,7)",
                        "(a,k,8):(a,k,8)");

        Collections.sort(expectedResult);
        Collections.sort(testResults);

        Assert.assertEquals(expectedResult, testResults);
    }

    /**
     * Verifies that pipelines including {@link CoGroupedStreams} can be checkpointed properly,
     * which includes snapshotting configurations of any involved serializers.
     *
     * @see <a href="https://issues.apache.org/jira/browse/FLINK-6808">FLINK-6808</a>
     */
    @Test
    public void testCoGroupOperatorWithCheckpoint() throws Exception {

        // generate an operator for the co-group operation
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Tuple2<String, Integer>> source1 =
                env.fromElements(Tuple2.of("a", 0), Tuple2.of("b", 3));
        DataStream<Tuple2<String, Integer>> source2 =
                env.fromElements(Tuple2.of("a", 1), Tuple2.of("b", 6));

        DataStream<String> coGroupWindow =
                source1.coGroup(source2)
                        .where(new Tuple2KeyExtractor())
                        .equalTo(new Tuple2KeyExtractor())
                        .window(TumblingEventTimeWindows.of(Time.of(3, TimeUnit.MILLISECONDS)))
                        .apply(
                                new CoGroupFunction<
                                        Tuple2<String, Integer>,
                                        Tuple2<String, Integer>,
                                        String>() {
                                    @Override
                                    public void coGroup(
                                            Iterable<Tuple2<String, Integer>> first,
                                            Iterable<Tuple2<String, Integer>> second,
                                            Collector<String> out)
                                            throws Exception {
                                        out.collect(first + ":" + second);
                                    }
                                });

        OneInputTransformation<Tuple2<String, Integer>, String> transform =
                (OneInputTransformation<Tuple2<String, Integer>, String>)
                        coGroupWindow.getTransformation();
        OneInputStreamOperator<Tuple2<String, Integer>, String> operator = transform.getOperator();

        // wrap the operator in the test harness, and perform a snapshot
        OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, String> testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        operator, new Tuple2KeyExtractor(), BasicTypeInfo.STRING_TYPE_INFO);

        testHarness.open();
        testHarness.snapshot(0L, 0L);
    }

    private static class Tuple2TimestampExtractor
            implements AssignerWithPunctuatedWatermarks<Tuple2<String, Integer>> {

        @Override
        public long extractTimestamp(Tuple2<String, Integer> element, long previousTimestamp) {
            return element.f1;
        }

        @Override
        public Watermark checkAndGetNextWatermark(
                Tuple2<String, Integer> element, long extractedTimestamp) {
            return new Watermark(extractedTimestamp - 1);
        }
    }

    private static class Tuple3TimestampExtractor
            implements AssignerWithPunctuatedWatermarks<Tuple3<String, String, Integer>> {

        @Override
        public long extractTimestamp(
                Tuple3<String, String, Integer> element, long previousTimestamp) {
            return element.f2;
        }

        @Override
        public Watermark checkAndGetNextWatermark(
                Tuple3<String, String, Integer> lastElement, long extractedTimestamp) {
            return new Watermark(lastElement.f2 - 1);
        }
    }

    private static class Tuple2KeyExtractor
            implements KeySelector<Tuple2<String, Integer>, String> {

        @Override
        public String getKey(Tuple2<String, Integer> value) throws Exception {
            return value.f0;
        }
    }

    private static class Tuple3KeyExtractor
            implements KeySelector<Tuple3<String, String, Integer>, String> {

        @Override
        public String getKey(Tuple3<String, String, Integer> value) throws Exception {
            return value.f0;
        }
    }
}
