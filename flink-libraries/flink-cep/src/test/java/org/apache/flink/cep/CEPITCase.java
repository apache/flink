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

package org.apache.flink.cep;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.configuration.CEPCacheOptions;
import org.apache.flink.cep.nfa.NFA;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.WithinType;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.RichIterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Either;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/** End to end tests of both CEP operators and {@link NFA}. */
@SuppressWarnings("serial")
@RunWith(Parameterized.class)
public class CEPITCase extends AbstractTestBase {

    @Parameterized.Parameter public Configuration envConfiguration;

    @Parameterized.Parameters
    public static Collection<Configuration> prepareSharedBufferCacheConfig() {
        Configuration miniCacheConfig = new Configuration();
        miniCacheConfig.set(CEPCacheOptions.CEP_CACHE_STATISTICS_INTERVAL, Duration.ofSeconds(1));
        miniCacheConfig.set(CEPCacheOptions.CEP_SHARED_BUFFER_ENTRY_CACHE_SLOTS, 1);
        miniCacheConfig.set(CEPCacheOptions.CEP_SHARED_BUFFER_EVENT_CACHE_SLOTS, 1);

        Configuration bigCacheConfig = new Configuration();
        miniCacheConfig.set(CEPCacheOptions.CEP_CACHE_STATISTICS_INTERVAL, Duration.ofSeconds(1));

        return Arrays.asList(miniCacheConfig, bigCacheConfig);
    }

    /**
     * Checks that a certain event sequence is recognized.
     *
     * @throws Exception
     */
    @Test
    public void testSimplePatternCEP() throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(envConfiguration);

        DataStream<Event> input =
                env.fromElements(
                        new Event(1, "barfoo", 1.0),
                        new Event(2, "start", 2.0),
                        new Event(3, "foobar", 3.0),
                        new SubEvent(4, "foo", 4.0, 1.0),
                        new Event(5, "middle", 5.0),
                        new SubEvent(6, "middle", 6.0, 2.0),
                        new SubEvent(7, "bar", 3.0, 3.0),
                        new Event(42, "42", 42.0),
                        new Event(8, "end", 1.0));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("start")))
                        .followedByAny("middle")
                        .subtype(SubEvent.class)
                        .where(SimpleCondition.of(value -> value.getName().equals("middle")))
                        .followedByAny("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("end")));

        DataStream<String> result =
                CEP.pattern(input, pattern)
                        .inProcessingTime()
                        .flatSelect(
                                (p, o) -> {
                                    StringBuilder builder = new StringBuilder();

                                    builder.append(p.get("start").get(0).getId())
                                            .append(",")
                                            .append(p.get("middle").get(0).getId())
                                            .append(",")
                                            .append(p.get("end").get(0).getId());

                                    o.collect(builder.toString());
                                },
                                Types.STRING);

        List<String> resultList = new ArrayList<>();

        DataStreamUtils.collect(result).forEachRemaining(resultList::add);

        assertEquals(Arrays.asList("2,6,8"), resultList);
    }

    @Test
    public void testSimpleKeyedPatternCEP() throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(envConfiguration);
        env.setParallelism(2);

        DataStream<Event> input =
                env.fromElements(
                                new Event(1, "barfoo", 1.0),
                                new Event(2, "start", 2.0),
                                new Event(3, "start", 2.1),
                                new Event(3, "foobar", 3.0),
                                new SubEvent(4, "foo", 4.0, 1.0),
                                new SubEvent(3, "middle", 3.2, 1.0),
                                new Event(42, "start", 3.1),
                                new SubEvent(42, "middle", 3.3, 1.2),
                                new Event(5, "middle", 5.0),
                                new SubEvent(2, "middle", 6.0, 2.0),
                                new SubEvent(7, "bar", 3.0, 3.0),
                                new Event(42, "42", 42.0),
                                new Event(3, "end", 2.0),
                                new Event(2, "end", 1.0),
                                new Event(42, "end", 42.0))
                        .keyBy(
                                new KeySelector<Event, Integer>() {

                                    @Override
                                    public Integer getKey(Event value) throws Exception {
                                        return value.getId();
                                    }
                                });

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("start")))
                        .followedByAny("middle")
                        .subtype(SubEvent.class)
                        .where(SimpleCondition.of(value -> value.getName().equals("middle")))
                        .followedByAny("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("end")));

        DataStream<String> result =
                CEP.pattern(input, pattern)
                        .inProcessingTime()
                        .select(
                                p -> {
                                    StringBuilder builder = new StringBuilder();

                                    builder.append(p.get("start").get(0).getId())
                                            .append(",")
                                            .append(p.get("middle").get(0).getId())
                                            .append(",")
                                            .append(p.get("end").get(0).getId());

                                    return builder.toString();
                                });

        List<String> resultList = new ArrayList<>();

        DataStreamUtils.collect(result).forEachRemaining(resultList::add);

        resultList.sort(String::compareTo);

        assertEquals(Arrays.asList("2,2,2", "3,3,3", "42,42,42"), resultList);
    }

    @Test
    public void testSimplePatternEventTime() throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(envConfiguration);

        // (Event, timestamp)
        DataStream<Event> input =
                env.fromElements(
                                Tuple2.of(new Event(1, "start", 1.0), 5L),
                                Tuple2.of(new Event(2, "middle", 2.0), 1L),
                                Tuple2.of(new Event(3, "end", 3.0), 3L),
                                Tuple2.of(new Event(4, "end", 4.0), 10L),
                                Tuple2.of(new Event(5, "middle", 5.0), 7L),
                                // last element for high final watermark
                                Tuple2.of(new Event(5, "middle", 5.0), 100L))
                        .assignTimestampsAndWatermarks(
                                new AssignerWithPunctuatedWatermarks<Tuple2<Event, Long>>() {

                                    @Override
                                    public long extractTimestamp(
                                            Tuple2<Event, Long> element, long previousTimestamp) {
                                        return element.f1;
                                    }

                                    @Override
                                    public Watermark checkAndGetNextWatermark(
                                            Tuple2<Event, Long> lastElement,
                                            long extractedTimestamp) {
                                        return new Watermark(lastElement.f1 - 5);
                                    }
                                })
                        .map(
                                new MapFunction<Tuple2<Event, Long>, Event>() {

                                    @Override
                                    public Event map(Tuple2<Event, Long> value) throws Exception {
                                        return value.f0;
                                    }
                                });

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("start")))
                        .followedByAny("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("middle")))
                        .followedByAny("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("end")));

        DataStream<String> result =
                CEP.pattern(input, pattern)
                        .select(
                                new PatternSelectFunction<Event, String>() {

                                    @Override
                                    public String select(Map<String, List<Event>> pattern) {
                                        StringBuilder builder = new StringBuilder();

                                        builder.append(pattern.get("start").get(0).getId())
                                                .append(",")
                                                .append(pattern.get("middle").get(0).getId())
                                                .append(",")
                                                .append(pattern.get("end").get(0).getId());

                                        return builder.toString();
                                    }
                                });

        List<String> resultList = new ArrayList<>();

        DataStreamUtils.collect(result).forEachRemaining(resultList::add);

        resultList.sort(String::compareTo);

        assertEquals(Arrays.asList("1,5,4"), resultList);
    }

    @Test
    public void testSimpleKeyedPatternEventTime() throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(envConfiguration);
        env.setParallelism(2);

        // (Event, timestamp)
        DataStream<Event> input =
                env.fromElements(
                                Tuple2.of(new Event(1, "start", 1.0), 5L),
                                Tuple2.of(new Event(1, "middle", 2.0), 1L),
                                Tuple2.of(new Event(2, "middle", 2.0), 4L),
                                Tuple2.of(new Event(2, "start", 2.0), 3L),
                                Tuple2.of(new Event(1, "end", 3.0), 3L),
                                Tuple2.of(new Event(3, "start", 4.1), 5L),
                                Tuple2.of(new Event(1, "end", 4.0), 10L),
                                Tuple2.of(new Event(2, "end", 2.0), 8L),
                                Tuple2.of(new Event(1, "middle", 5.0), 7L),
                                Tuple2.of(new Event(3, "middle", 6.0), 9L),
                                Tuple2.of(new Event(3, "end", 7.0), 7L))
                        .assignTimestampsAndWatermarks(
                                new AssignerWithPunctuatedWatermarks<Tuple2<Event, Long>>() {

                                    @Override
                                    public long extractTimestamp(
                                            Tuple2<Event, Long> element, long currentTimestamp) {
                                        return element.f1;
                                    }

                                    @Override
                                    public Watermark checkAndGetNextWatermark(
                                            Tuple2<Event, Long> lastElement,
                                            long extractedTimestamp) {
                                        return new Watermark(lastElement.f1 - 5);
                                    }
                                })
                        .map(
                                new MapFunction<Tuple2<Event, Long>, Event>() {

                                    @Override
                                    public Event map(Tuple2<Event, Long> value) throws Exception {
                                        return value.f0;
                                    }
                                })
                        .keyBy(
                                new KeySelector<Event, Integer>() {

                                    @Override
                                    public Integer getKey(Event value) throws Exception {
                                        return value.getId();
                                    }
                                });

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("start")))
                        .followedByAny("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("middle")))
                        .followedByAny("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("end")));

        DataStream<String> result =
                CEP.pattern(input, pattern)
                        .select(
                                new PatternSelectFunction<Event, String>() {

                                    @Override
                                    public String select(Map<String, List<Event>> pattern) {
                                        StringBuilder builder = new StringBuilder();

                                        builder.append(pattern.get("start").get(0).getId())
                                                .append(",")
                                                .append(pattern.get("middle").get(0).getId())
                                                .append(",")
                                                .append(pattern.get("end").get(0).getId());

                                        return builder.toString();
                                    }
                                });

        List<String> resultList = new ArrayList<>();

        DataStreamUtils.collect(result).forEachRemaining(resultList::add);

        resultList.sort(String::compareTo);

        assertEquals(Arrays.asList("1,1,1", "2,2,2"), resultList);
    }

    @Test
    public void testSimplePatternWithSingleState() throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(envConfiguration);

        DataStream<Tuple2<Integer, Integer>> input =
                env.fromElements(new Tuple2<>(0, 1), new Tuple2<>(0, 2));

        Pattern<Tuple2<Integer, Integer>, ?> pattern =
                Pattern.<Tuple2<Integer, Integer>>begin("start")
                        .where(SimpleCondition.of(rec -> rec.f1 == 1));

        PatternStream<Tuple2<Integer, Integer>> pStream =
                CEP.pattern(input, pattern).inProcessingTime();

        DataStream<Tuple2<Integer, Integer>> result =
                pStream.select(
                        new PatternSelectFunction<
                                Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
                            @Override
                            public Tuple2<Integer, Integer> select(
                                    Map<String, List<Tuple2<Integer, Integer>>> pattern)
                                    throws Exception {
                                return pattern.get("start").get(0);
                            }
                        });

        List<Tuple2<Integer, Integer>> resultList = new ArrayList<>();

        DataStreamUtils.collect(result).forEachRemaining(resultList::add);

        assertEquals(Arrays.asList(new Tuple2<>(0, 1)), resultList);
    }

    @Test
    public void testProcessingTimeWithinBetweenFirstAndLast() throws Exception {
        testProcessingTimeWithWindow(WithinType.FIRST_AND_LAST);
    }

    @Test
    public void testProcessingTimeWithinPreviousAndCurrent() throws Exception {
        testProcessingTimeWithWindow(WithinType.PREVIOUS_AND_CURRENT);
    }

    private void testProcessingTimeWithWindow(WithinType withinType) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(envConfiguration);
        env.setParallelism(1);

        DataStream<Integer> input = env.fromElements(1, 2);

        Pattern<Integer, ?> pattern =
                Pattern.<Integer>begin("start")
                        .followedByAny("end")
                        .within(Time.days(1), withinType);

        DataStream<Integer> result =
                CEP.pattern(input, pattern)
                        .inProcessingTime()
                        .select(
                                new PatternSelectFunction<Integer, Integer>() {
                                    @Override
                                    public Integer select(Map<String, List<Integer>> pattern)
                                            throws Exception {
                                        return pattern.get("start").get(0)
                                                + pattern.get("end").get(0);
                                    }
                                });

        List<Integer> resultList = new ArrayList<>();

        DataStreamUtils.collect(result).forEachRemaining(resultList::add);

        assertEquals(Arrays.asList(3), resultList);
    }

    @Test
    public void testTimeoutHandlingWithinFirstAndLast() throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(envConfiguration);
        env.setParallelism(1);

        // (Event, timestamp)
        DataStream<Event> input =
                env.fromElements(
                                Tuple2.of(new Event(1, "start", 1.0), 1L),
                                Tuple2.of(new Event(1, "middle", 2.0), 5L),
                                Tuple2.of(new Event(1, "start", 2.0), 4L),
                                Tuple2.of(new Event(1, "end", 2.0), 6L))
                        .assignTimestampsAndWatermarks(
                                new AssignerWithPunctuatedWatermarks<Tuple2<Event, Long>>() {

                                    @Override
                                    public long extractTimestamp(
                                            Tuple2<Event, Long> element, long currentTimestamp) {
                                        return element.f1;
                                    }

                                    @Override
                                    public Watermark checkAndGetNextWatermark(
                                            Tuple2<Event, Long> lastElement,
                                            long extractedTimestamp) {
                                        return new Watermark(lastElement.f1 - 5);
                                    }
                                })
                        .map(
                                new MapFunction<Tuple2<Event, Long>, Event>() {

                                    @Override
                                    public Event map(Tuple2<Event, Long> value) throws Exception {
                                        return value.f0;
                                    }
                                });

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("start")))
                        .followedByAny("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("middle")))
                        .followedByAny("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("end")))
                        .within(Time.milliseconds(3));

        DataStream<Either<String, String>> result =
                CEP.pattern(input, pattern)
                        .select(
                                new PatternTimeoutFunction<Event, String>() {
                                    @Override
                                    public String timeout(
                                            Map<String, List<Event>> pattern, long timeoutTimestamp)
                                            throws Exception {
                                        return pattern.get("start").get(0).getPrice() + "";
                                    }
                                },
                                new PatternSelectFunction<Event, String>() {

                                    @Override
                                    public String select(Map<String, List<Event>> pattern) {
                                        StringBuilder builder = new StringBuilder();

                                        builder.append(pattern.get("start").get(0).getPrice())
                                                .append(",")
                                                .append(pattern.get("middle").get(0).getPrice())
                                                .append(",")
                                                .append(pattern.get("end").get(0).getPrice());

                                        return builder.toString();
                                    }
                                });

        List<Either<String, String>> resultList = new ArrayList<>();

        DataStreamUtils.collect(result).forEachRemaining(resultList::add);

        resultList.sort(Comparator.comparing(Object::toString));

        List<Either<String, String>> expected =
                Arrays.asList(
                        Either.Left.of("1.0"),
                        Either.Left.of("2.0"),
                        Either.Left.of("2.0"),
                        Either.Right.of("2.0,2.0,2.0"));

        assertEquals(expected, resultList);
    }

    @Test
    public void testTimeoutHandlingWithinPreviousAndCurrent() throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(envConfiguration);
        env.setParallelism(1);

        // (Event, timestamp)
        DataStream<Event> input =
                env.fromElements(
                                Tuple2.of(new Event(1, "start", 1.0), 1L),
                                Tuple2.of(new Event(1, "middle", 2.0), 5L),
                                Tuple2.of(new Event(1, "start", 2.0), 4L),
                                Tuple2.of(new Event(1, "end", 2.0), 6L))
                        .assignTimestampsAndWatermarks(
                                new AssignerWithPunctuatedWatermarks<Tuple2<Event, Long>>() {

                                    @Override
                                    public long extractTimestamp(
                                            Tuple2<Event, Long> element, long currentTimestamp) {
                                        return element.f1;
                                    }

                                    @Override
                                    public Watermark checkAndGetNextWatermark(
                                            Tuple2<Event, Long> lastElement,
                                            long extractedTimestamp) {
                                        return new Watermark(lastElement.f1 - 5);
                                    }
                                })
                        .map(
                                new MapFunction<Tuple2<Event, Long>, Event>() {

                                    @Override
                                    public Event map(Tuple2<Event, Long> value) throws Exception {
                                        return value.f0;
                                    }
                                });

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("start")))
                        .followedByAny("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("middle")))
                        .followedByAny("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("end")))
                        .within(Time.milliseconds(3), WithinType.PREVIOUS_AND_CURRENT);

        DataStream<Either<String, String>> result =
                CEP.pattern(input, pattern)
                        .select(
                                new PatternTimeoutFunction<Event, String>() {
                                    @Override
                                    public String timeout(
                                            Map<String, List<Event>> pattern, long timeoutTimestamp)
                                            throws Exception {
                                        return pattern.get("start").get(0).getPrice() + "";
                                    }
                                },
                                new PatternSelectFunction<Event, String>() {

                                    @Override
                                    public String select(Map<String, List<Event>> pattern) {
                                        StringBuilder builder = new StringBuilder();

                                        builder.append(pattern.get("start").get(0).getPrice())
                                                .append(",")
                                                .append(pattern.get("middle").get(0).getPrice())
                                                .append(",")
                                                .append(pattern.get("end").get(0).getPrice());

                                        return builder.toString();
                                    }
                                });

        List<Either<String, String>> resultList = new ArrayList<>();

        DataStreamUtils.collect(result).forEachRemaining(resultList::add);

        resultList.sort(Comparator.comparing(Object::toString));

        List<Either<String, String>> expected =
                Arrays.asList(
                        Either.Left.of("1.0"),
                        Either.Left.of("2.0"),
                        Either.Right.of("1.0,2.0,2.0"),
                        Either.Right.of("2.0,2.0,2.0"));

        assertEquals(expected, resultList);
    }

    /**
     * Checks that a certain event sequence is recognized with an OR filter.
     *
     * @throws Exception
     */
    @Test
    public void testSimpleOrFilterPatternCEP() throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(envConfiguration);

        DataStream<Event> input =
                env.fromElements(
                        new Event(1, "start", 1.0),
                        new Event(2, "middle", 2.0),
                        new Event(3, "end", 3.0),
                        new Event(4, "start", 4.0),
                        new Event(5, "middle", 5.0),
                        new Event(6, "end", 6.0));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("start")))
                        .followedByAny("middle")
                        .where(SimpleCondition.of(value -> value.getPrice() == 2.0))
                        .or(SimpleCondition.of(value -> value.getPrice() == 5.0))
                        .followedByAny("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("end")));

        DataStream<String> result =
                CEP.pattern(input, pattern)
                        .inProcessingTime()
                        .select(
                                new PatternSelectFunction<Event, String>() {

                                    @Override
                                    public String select(Map<String, List<Event>> pattern) {
                                        StringBuilder builder = new StringBuilder();

                                        builder.append(pattern.get("start").get(0).getId())
                                                .append(",")
                                                .append(pattern.get("middle").get(0).getId())
                                                .append(",")
                                                .append(pattern.get("end").get(0).getId());

                                        return builder.toString();
                                    }
                                });

        List<String> resultList = new ArrayList<>();

        DataStreamUtils.collect(result).forEachRemaining(resultList::add);

        List<String> expected = Arrays.asList("1,5,6", "1,2,3", "4,5,6", "1,2,6");

        expected.sort(String::compareTo);

        resultList.sort(String::compareTo);

        assertEquals(expected, resultList);
    }

    /**
     * Checks that a certain event sequence is recognized.
     *
     * @throws Exception
     */
    @Test
    public void testSimplePatternEventTimeWithComparator() throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(envConfiguration);

        // (Event, timestamp)
        DataStream<Event> input =
                env.fromElements(
                                Tuple2.of(new Event(1, "start", 1.0), 5L),
                                Tuple2.of(new Event(2, "middle", 2.0), 1L),
                                Tuple2.of(new Event(3, "end", 3.0), 3L),
                                Tuple2.of(new Event(4, "end", 4.0), 10L),
                                Tuple2.of(new Event(5, "middle", 6.0), 7L),
                                Tuple2.of(new Event(6, "middle", 5.0), 7L),
                                // last element for high final watermark
                                Tuple2.of(new Event(7, "middle", 5.0), 100L))
                        .assignTimestampsAndWatermarks(
                                new AssignerWithPunctuatedWatermarks<Tuple2<Event, Long>>() {

                                    @Override
                                    public long extractTimestamp(
                                            Tuple2<Event, Long> element, long previousTimestamp) {
                                        return element.f1;
                                    }

                                    @Override
                                    public Watermark checkAndGetNextWatermark(
                                            Tuple2<Event, Long> lastElement,
                                            long extractedTimestamp) {
                                        return new Watermark(lastElement.f1 - 5);
                                    }
                                })
                        .map(
                                new MapFunction<Tuple2<Event, Long>, Event>() {

                                    @Override
                                    public Event map(Tuple2<Event, Long> value) throws Exception {
                                        return value.f0;
                                    }
                                });

        EventComparator<Event> comparator = new CustomEventComparator();

        Pattern<Event, ? extends Event> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("start")))
                        .followedByAny("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("middle")))
                        .followedByAny("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("end")));

        DataStream<String> result =
                CEP.pattern(input, pattern, comparator)
                        .select(
                                new PatternSelectFunction<Event, String>() {

                                    @Override
                                    public String select(Map<String, List<Event>> pattern) {
                                        StringBuilder builder = new StringBuilder();

                                        builder.append(pattern.get("start").get(0).getId())
                                                .append(",")
                                                .append(pattern.get("middle").get(0).getId())
                                                .append(",")
                                                .append(pattern.get("end").get(0).getId());

                                        return builder.toString();
                                    }
                                });

        List<String> resultList = new ArrayList<>();

        DataStreamUtils.collect(result).forEachRemaining(resultList::add);

        List<String> expected = Arrays.asList("1,6,4", "1,5,4");

        expected.sort(String::compareTo);

        resultList.sort(String::compareTo);

        assertEquals(expected, resultList);
    }

    private static class CustomEventComparator implements EventComparator<Event> {
        @Override
        public int compare(Event o1, Event o2) {
            return Double.compare(o1.getPrice(), o2.getPrice());
        }
    }

    @Test
    public void testSimpleAfterMatchSkip() throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(envConfiguration);

        DataStream<Tuple2<Integer, String>> input =
                env.fromElements(
                        new Tuple2<>(1, "a"),
                        new Tuple2<>(2, "a"),
                        new Tuple2<>(3, "a"),
                        new Tuple2<>(4, "a"));

        Pattern<Tuple2<Integer, String>, ?> pattern =
                Pattern.<Tuple2<Integer, String>>begin(
                                "start", AfterMatchSkipStrategy.skipPastLastEvent())
                        .where(SimpleCondition.of(rec -> rec.f1.equals("a")))
                        .times(2);

        PatternStream<Tuple2<Integer, String>> pStream =
                CEP.pattern(input, pattern).inProcessingTime();

        DataStream<Tuple2<Integer, String>> result =
                pStream.select(
                        new PatternSelectFunction<
                                Tuple2<Integer, String>, Tuple2<Integer, String>>() {
                            @Override
                            public Tuple2<Integer, String> select(
                                    Map<String, List<Tuple2<Integer, String>>> pattern)
                                    throws Exception {
                                return pattern.get("start").get(0);
                            }
                        });

        List<Tuple2<Integer, String>> resultList = new ArrayList<>();

        DataStreamUtils.collect(result).forEachRemaining(resultList::add);

        resultList.sort(Comparator.comparing(tuple2 -> tuple2.toString()));

        List<Tuple2<Integer, String>> expected =
                Arrays.asList(Tuple2.of(1, "a"), Tuple2.of(3, "a"));

        assertEquals(expected, resultList);
    }

    @Test
    public void testRichPatternFlatSelectFunction() throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(envConfiguration);

        DataStream<Event> input =
                env.fromElements(
                        new Event(1, "barfoo", 1.0),
                        new Event(2, "start", 2.0),
                        new Event(3, "foobar", 3.0),
                        new SubEvent(4, "foo", 4.0, 1.0),
                        new Event(5, "middle", 5.0),
                        new SubEvent(6, "middle", 6.0, 2.0),
                        new SubEvent(7, "bar", 3.0, 3.0),
                        new Event(42, "42", 42.0),
                        new Event(8, "end", 1.0));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(
                                new RichIterativeCondition<Event>() {

                                    @Override
                                    public boolean filter(Event value, Context<Event> ctx)
                                            throws Exception {
                                        return value.getName().equals("start");
                                    }
                                })
                        .followedByAny("middle")
                        .subtype(SubEvent.class)
                        .where(SimpleCondition.of(value -> value.getName().equals("middle")))
                        .followedByAny("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("end")));

        DataStream<String> result =
                CEP.pattern(input, pattern)
                        .inProcessingTime()
                        .flatSelect(
                                new RichPatternFlatSelectFunction<Event, String>() {

                                    @Override
                                    public void open(OpenContext openContext) {
                                        try {
                                            getRuntimeContext()
                                                    .getMapState(
                                                            new MapStateDescriptor<>(
                                                                    "test",
                                                                    LongSerializer.INSTANCE,
                                                                    LongSerializer.INSTANCE));
                                            throw new RuntimeException(
                                                    "Expected getMapState to fail with unsupported operation exception.");
                                        } catch (UnsupportedOperationException e) {
                                            // ignore, expected
                                        }

                                        getRuntimeContext().getUserCodeClassLoader();
                                    }

                                    @Override
                                    public void flatSelect(
                                            Map<String, List<Event>> p, Collector<String> o)
                                            throws Exception {
                                        StringBuilder builder = new StringBuilder();

                                        builder.append(p.get("start").get(0).getId())
                                                .append(",")
                                                .append(p.get("middle").get(0).getId())
                                                .append(",")
                                                .append(p.get("end").get(0).getId());

                                        o.collect(builder.toString());
                                    }
                                },
                                Types.STRING);

        List<String> resultList = new ArrayList<>();

        DataStreamUtils.collect(result).forEachRemaining(resultList::add);

        assertEquals(Arrays.asList("2,6,8"), resultList);
    }

    @Test
    public void testRichPatternSelectFunction() throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(envConfiguration);
        env.setParallelism(2);

        DataStream<Event> input =
                env.fromElements(
                                new Event(1, "barfoo", 1.0),
                                new Event(2, "start", 2.0),
                                new Event(3, "start", 2.1),
                                new Event(3, "foobar", 3.0),
                                new SubEvent(4, "foo", 4.0, 1.0),
                                new SubEvent(3, "middle", 3.2, 1.0),
                                new Event(42, "start", 3.1),
                                new SubEvent(42, "middle", 3.3, 1.2),
                                new Event(5, "middle", 5.0),
                                new SubEvent(2, "middle", 6.0, 2.0),
                                new SubEvent(7, "bar", 3.0, 3.0),
                                new Event(42, "42", 42.0),
                                new Event(3, "end", 2.0),
                                new Event(2, "end", 1.0),
                                new Event(42, "end", 42.0))
                        .keyBy(
                                new KeySelector<Event, Integer>() {

                                    @Override
                                    public Integer getKey(Event value) throws Exception {
                                        return value.getId();
                                    }
                                });

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(
                                new RichIterativeCondition<Event>() {

                                    @Override
                                    public boolean filter(Event value, Context<Event> ctx)
                                            throws Exception {
                                        return value.getName().equals("start");
                                    }
                                })
                        .followedByAny("middle")
                        .subtype(SubEvent.class)
                        .where(SimpleCondition.of(value -> value.getName().equals("middle")))
                        .followedByAny("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("end")));

        DataStream<String> result =
                CEP.pattern(input, pattern)
                        .inProcessingTime()
                        .select(
                                new RichPatternSelectFunction<Event, String>() {
                                    @Override
                                    public void open(OpenContext openContext) {
                                        try {
                                            getRuntimeContext()
                                                    .getMapState(
                                                            new MapStateDescriptor<>(
                                                                    "test",
                                                                    LongSerializer.INSTANCE,
                                                                    LongSerializer.INSTANCE));
                                            throw new RuntimeException(
                                                    "Expected getMapState to fail with unsupported operation exception.");
                                        } catch (UnsupportedOperationException e) {
                                            // ignore, expected
                                        }

                                        getRuntimeContext().getUserCodeClassLoader();
                                    }

                                    @Override
                                    public String select(Map<String, List<Event>> p)
                                            throws Exception {
                                        StringBuilder builder = new StringBuilder();

                                        builder.append(p.get("start").get(0).getId())
                                                .append(",")
                                                .append(p.get("middle").get(0).getId())
                                                .append(",")
                                                .append(p.get("end").get(0).getId());

                                        return builder.toString();
                                    }
                                });

        List<String> resultList = new ArrayList<>();

        DataStreamUtils.collect(result).forEachRemaining(resultList::add);

        resultList.sort(String::compareTo);

        assertEquals(Arrays.asList("2,2,2", "3,3,3", "42,42,42"), resultList);
    }

    @Test
    public void testFlatSelectSerializationWithAnonymousClass() throws Exception {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(envConfiguration);

        DataStreamSource<Integer> elements = env.fromElements(1, 2, 3);
        OutputTag<Integer> outputTag = new OutputTag<Integer>("AAA") {};
        CEP.pattern(elements, Pattern.begin("A"))
                .inProcessingTime()
                .flatSelect(
                        outputTag,
                        new PatternFlatTimeoutFunction<Integer, Integer>() {
                            @Override
                            public void timeout(
                                    Map<String, List<Integer>> pattern,
                                    long timeoutTimestamp,
                                    Collector<Integer> out)
                                    throws Exception {}
                        },
                        new PatternFlatSelectFunction<Integer, Object>() {
                            @Override
                            public void flatSelect(
                                    Map<String, List<Integer>> pattern, Collector<Object> out)
                                    throws Exception {}
                        });

        env.execute();
    }

    @Test
    public void testPartialMatchTimeoutOutputCompletedMatch() throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(envConfiguration);

        // (Event, timestamp)
        DataStream<Event> input =
                env.fromElements(
                                Tuple2.of(new Event(1, "start", 1.0), 0L),
                                Tuple2.of(new Event(2, "start", 2.0), 1L),
                                Tuple2.of(new Event(3, "start", 3.0), 2L),
                                Tuple2.of(new Event(4, "start", 4.0), 3L),
                                Tuple2.of(new Event(5, "end", 5.0), 4L))
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<Tuple2<Event, Long>>forBoundedOutOfOrderness(
                                                Duration.ofMillis(5))
                                        .withTimestampAssigner(
                                                TimestampAssignerSupplier.of(
                                                        (SerializableTimestampAssigner<
                                                                        Tuple2<Event, Long>>)
                                                                (element, recordTimestamp) ->
                                                                        element.f1)))
                        .map((MapFunction<Tuple2<Event, Long>, Event>) value -> value.f0);

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start", AfterMatchSkipStrategy.skipPastLastEvent())
                        .where(SimpleCondition.of(value -> value.getName().equals("start")))
                        .oneOrMore()
                        .consecutive()
                        .greedy()
                        .followedBy("middle")
                        .where(
                                new IterativeCondition<Event>() {
                                    @Override
                                    public boolean filter(Event value, Context<Event> ctx)
                                            throws Exception {
                                        int count = 0;
                                        for (Event ignored : ctx.getEventsForPattern("start")) {
                                            count++;
                                        }
                                        if (count > 2) {
                                            return value.getName().equals("middle");
                                        } else {
                                            return value.getName().equals("end");
                                        }
                                    }
                                })
                        .within(Time.milliseconds(100L));

        DataStream<String> result =
                CEP.pattern(input, pattern)
                        .select(
                                (PatternSelectFunction<Event, String>)
                                        pattern1 ->
                                                pattern1.get("start").get(0).getId()
                                                        + ","
                                                        + pattern1.get("middle").get(0).getId());

        List<String> resultList = new ArrayList<>();
        try (CloseableIterator<String> iterator = result.executeAndCollect()) {
            iterator.forEachRemaining(resultList::add);
        }
        resultList.sort(String::compareTo);
        assertEquals(Arrays.asList("3,5"), resultList);
    }
}
