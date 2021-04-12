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

package org.apache.flink.streaming.examples.sideoutput;

import org.apache.flink.api.common.eventtime.AscendingTimestampsWatermarks;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.examples.wordcount.util.WordCountData;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * An example that illustrates the use of side output.
 *
 * <p>This is a modified version of {@link
 * org.apache.flink.streaming.examples.windowing.WindowWordCount} that has a filter in the tokenizer
 * and only emits some words for counting while emitting the other words to a side output.
 */
public class SideOutputExample {

    /**
     * We need to create an {@link OutputTag} so that we can reference it when emitting data to a
     * side output and also to retrieve the side output stream from an operation.
     */
    private static final OutputTag<String> rejectedWordsTag = new OutputTag<String>("rejected") {};

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // get input data
        DataStream<String> text;
        if (params.has("input")) {
            // read the text file from given input path
            text = env.readTextFile(params.get("input"));
        } else {
            System.out.println("Executing WordCount example with default input data set.");
            System.out.println("Use --input to specify file input.");
            // get default test text data
            text = env.fromElements(WordCountData.WORDS);
        }

        // We assign the WatermarkStrategy after creating the source. In a real-world job you
        // should integrate the WatermarkStrategy in the source. The Kafka source allows this,
        // for example.
        text.assignTimestampsAndWatermarks(IngestionTimeWatermarkStrategy.create());

        SingleOutputStreamOperator<Tuple2<String, Integer>> tokenized =
                text.keyBy(
                                new KeySelector<String, Integer>() {
                                    private static final long serialVersionUID = 1L;

                                    @Override
                                    public Integer getKey(String value) throws Exception {
                                        return 0;
                                    }
                                })
                        .process(new Tokenizer());

        DataStream<String> rejectedWords =
                tokenized
                        .getSideOutput(rejectedWordsTag)
                        .map(
                                new MapFunction<String, String>() {
                                    private static final long serialVersionUID = 1L;

                                    @Override
                                    public String map(String value) throws Exception {
                                        return "rejected: " + value;
                                    }
                                });

        DataStream<Tuple2<String, Integer>> counts =
                tokenized
                        .keyBy(value -> value.f0)
                        .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                        // group by the tuple field "0" and sum up tuple field "1"
                        .sum(1);

        // emit result
        if (params.has("output")) {
            counts.writeAsText(params.get("output"));
            rejectedWords.writeAsText(params.get("rejected-words-output"));
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            counts.print();
            rejectedWords.print();
        }

        // execute program
        env.execute("Streaming WordCount SideOutput");
    }

    // *************************************************************************
    // USER FUNCTIONS
    // *************************************************************************

    /**
     * Implements the string tokenizer that splits sentences into words as a user-defined
     * FlatMapFunction. The function takes a line (String) and splits it into multiple pairs in the
     * form of "(word,1)" ({@code Tuple2<String, Integer>}).
     *
     * <p>This rejects words that are longer than 5 characters long.
     */
    public static final class Tokenizer extends ProcessFunction<String, Tuple2<String, Integer>> {
        private static final long serialVersionUID = 1L;

        @Override
        public void processElement(
                String value, Context ctx, Collector<Tuple2<String, Integer>> out)
                throws Exception {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 5) {
                    ctx.output(rejectedWordsTag, token);
                } else if (token.length() > 0) {
                    out.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }

    /**
     * This {@link WatermarkStrategy} assigns the current system time as the event-time timestamp.
     * In a real use case you should use proper timestamps and an appropriate {@link
     * WatermarkStrategy}.
     */
    private static class IngestionTimeWatermarkStrategy<T> implements WatermarkStrategy<T> {

        private IngestionTimeWatermarkStrategy() {}

        public static <T> IngestionTimeWatermarkStrategy<T> create() {
            return new IngestionTimeWatermarkStrategy<>();
        }

        @Override
        public WatermarkGenerator<T> createWatermarkGenerator(
                WatermarkGeneratorSupplier.Context context) {
            return new AscendingTimestampsWatermarks<>();
        }

        @Override
        public TimestampAssigner<T> createTimestampAssigner(
                TimestampAssignerSupplier.Context context) {
            return (event, timestamp) -> System.currentTimeMillis();
        }
    }
}
