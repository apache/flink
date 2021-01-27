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

package org.apache.flink.streaming.examples.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HeartbeatManagerOptions;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.examples.wordcount.util.WordCountData;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

/**
 * Implements the "WordCount" program that computes a simple word occurrence histogram over text
 * files in a streaming fashion.
 *
 * <p>The input is a plain text file with lines separated by newline characters.
 *
 * <p>Usage: <code>WordCount --input &lt;path&gt; --output &lt;path&gt;</code><br>
 * If no parameters are provided, the program is run with default data from {@link WordCountData}.
 *
 * <p>This example shows how to:
 *
 * <ul>
 *   <li>write a simple Flink Streaming program,
 *   <li>use tuple data types,
 *   <li>write and use user-defined functions.
 * </ul>
 */
public class WordCount {

    // *************************************************************************
    // PROGRAM
    // *************************************************************************

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final MultipleParameterTool params = MultipleParameterTool.fromArgs(args);

        Configuration configuration = new Configuration();
        configuration.setLong(HeartbeatManagerOptions.HEARTBEAT_TIMEOUT, 1000000L);
        configuration.setString(AkkaOptions.ASK_TIMEOUT, "1 h");

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
//        final StreamExecutionEnvironment env =
//                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);
        env.enableCheckpointing(500);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));
        // env.setStateBackend(new FsStateBackend("file://tmp/test/"));

        // get input data
        DataStream<String> text = null;
        if (params.has("input")) {
            // union all the inputs from text files
            for (String input : params.getMultiParameterRequired("input")) {
                if (text == null) {
                    text = env.readTextFile(input);
                } else {
                    text = text.union(env.readTextFile(input));
                }
            }
            Preconditions.checkNotNull(text, "Input DataStream should not be null.");
        } else {
            System.out.println("Executing WordCount example with default input data set.");
            System.out.println("Use --input to specify file input.");
            // get default test text data
            // text = env.fromElements(WordCountData.WORDS);
            text =
                    env.addSource(
                            new SourceFunction<String>() {
                                private volatile boolean running = true;

                                @Override
                                public void run(SourceContext<String> ctx) throws Exception {
                                    while (running) {
                                        for (String word : WordCountData.WORDS) {
                                            synchronized (ctx.getCheckpointLock()) {
                                                ctx.collect(word);
                                            }
                                        }
                                    }
                                }

                                @Override
                                public void cancel() {
                                    running = false;
                                }
                            });
        }

        DataStream<Tuple2<String, Integer>> counts =
                // split up the lines in pairs (2-tuples) containing: (word,1)
                text.flatMap(new Tokenizer())
                        // group by the tuple field "0" and sum up tuple field "1"
                        .keyBy(value -> value.f0)
                        .sum(1);

        // emit result
        if (params.has("output")) {
            counts.writeAsText(params.get("output"));
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            // counts.print();
            counts.addSink(
                    new SinkFunction<Tuple2<String, Integer>>() {
                        @Override
                        public void invoke(Tuple2<String, Integer> value) throws Exception {
                            Thread.sleep(1);
                            // ignore
                        }
                    });
//            counts.print();
        }
        // execute program
        env.execute("Streaming WordCount");
    }

    // *************************************************************************
    // USER FUNCTIONS
    // *************************************************************************

    /**
     * Implements the string tokenizer that splits sentences into words as a user-defined
     * FlatMapFunction. The function takes a line (String) and splits it into multiple pairs in the
     * form of "(word,1)" ({@code Tuple2<String, Integer>}).
     */
    public static final class Tokenizer
            implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }

//    private static class FailingMapperFunction
//        extends RichMapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {
//        private ValueState<Integer> valueState;
//
//        @Override
//        public void open(Configuration parameters) throws Exception {
//            super.open(parameters);
//            valueState =
//                getRuntimeContext()
//                    .getState(new ValueStateDescriptor<>("value", Integer.class));
//        }
//
//        FailingMapperFunction(int restartTimes) {
//            this.restartTimes = restartTimes;
//        }
//
//        @Override
//        public Tuple2<Integer, Integer> map(Tuple2<Integer, Integer> input) throws Exception {
//            int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
//
//            if (input.f1 > FAIL_BASE * (jobFailedCnt.get() + 1)) {
//
//                // we would let region-0 to failover first
//                if (jobFailedCnt.get() < 1 && indexOfThisSubtask == 0) {
//                    jobFailedCnt.incrementAndGet();
//                    throw new TestException();
//                }
//
//                // then let last region to failover
//                if (jobFailedCnt.get() < restartTimes && indexOfThisSubtask == NUM_OF_REGIONS - 1) {
//                    jobFailedCnt.incrementAndGet();
//                    throw new TestException();
//                }
//            }
//
//            // take input (1, 2) and (1, 3) for example, we would finally emit (1, 5) out with the
//            // usage of keyed state.
//            Integer value = valueState.value();
//            if (value == null) {
//                valueState.update(input.f1);
//                return input;
//            } else {
//                return Tuple2.of(input.f0, value + input.f1);
//            }
//        }
//    }
}
