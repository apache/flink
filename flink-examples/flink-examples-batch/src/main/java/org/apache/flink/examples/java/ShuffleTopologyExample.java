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

package org.apache.flink.examples.java;

import org.apache.flink.api.common.BatchShuffleMode;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.BatchExecutionOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/** */
public class ShuffleTopologyExample {

    private static Configuration getConfiguration() {
        Configuration configuration = new Configuration();
        configuration.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        configuration.set(NettyShuffleEnvironmentOptions.NETWORK_REQUEST_BACKOFF_MAX, 100);
        return configuration;
    }

    private static Configuration configureHybridOptions(
            Configuration configuration, boolean isSelective) {
        BatchShuffleMode shuffleMode =
                isSelective
                        ? BatchShuffleMode.ALL_EXCHANGES_HYBRID_SELECTIVE
                        : BatchShuffleMode.ALL_EXCHANGES_HYBRID_FULL;
        configuration.set(ExecutionOptions.BATCH_SHUFFLE_MODE, shuffleMode);
        configuration.set(
                NettyShuffleEnvironmentOptions.NETWORK_HYBRID_SHUFFLE_ENABLE_NEW_MODE, true);

        //        configuration.setString(TaskManagerOptions.NETWORK_MEMORY_MAX.key(), "2560k");
        configuration.setString(TaskManagerOptions.NETWORK_MEMORY_MAX.key(), "1023k");

        return configuration;
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

    private static class StringSource implements ParallelSourceFunction<Tuple2<String, Integer>> {
        private volatile boolean isRunning = true;
        private int numRecordsToSend;

        StringSource(int numRecordsToSend) {
            this.numRecordsToSend = numRecordsToSend;
        }

        @Override
        public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
            while (isRunning && numRecordsToSend-- > 0) {
                char c = (char) ('A' + numRecordsToSend % 26);
                ctx.collect(Tuple2.of(String.valueOf(c), 10000 - numRecordsToSend));
                Thread.sleep(100);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    protected static void executeJobGraph(int numRecordsToSend, Configuration configuration)
            throws Exception {
        configuration.setBoolean(BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_ENABLED, false);
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, 0L));
        env.setParallelism(4);

        DataStream<Tuple2<String, Integer>> source1 =
                new DataStreamSource<>(
                                env,
                                new TupleTypeInfo(
                                        BasicTypeInfo.STRING_TYPE_INFO,
                                        BasicTypeInfo.INT_TYPE_INFO),
                                new StreamSource<>(new StringSource(numRecordsToSend)),
                                true,
                                "source1",
                                Boundedness.BOUNDED)
                        .assignTimestampsAndWatermarks(new Tuple2TimestampExtractor());
        DataStream<Tuple2<String, Integer>> source2 =
                new DataStreamSource<>(
                                env,
                                new TupleTypeInfo(
                                        BasicTypeInfo.STRING_TYPE_INFO,
                                        BasicTypeInfo.INT_TYPE_INFO),
                                new StreamSource<>(new StringSource(numRecordsToSend)),
                                true,
                                "source2",
                                Boundedness.BOUNDED)
                        .assignTimestampsAndWatermarks(new Tuple2TimestampExtractor());
        DataStream<Tuple2<String, Integer>> source3 =
                new DataStreamSource<>(
                                env,
                                new TupleTypeInfo(
                                        BasicTypeInfo.STRING_TYPE_INFO,
                                        BasicTypeInfo.INT_TYPE_INFO),
                                new StreamSource<>(new StringSource(numRecordsToSend)),
                                true,
                                "source3",
                                Boundedness.BOUNDED)
                        .assignTimestampsAndWatermarks(new Tuple2TimestampExtractor());

        KeySelector<Tuple2<String, Integer>, String> keySelector = record -> record.f0;
        JoinFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String> joinFunction =
                (first, second) -> first.f0 + second.f0;

        source1.join(source2)
                .where(keySelector)
                .equalTo(keySelector)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(1)))
                .apply(joinFunction, BasicTypeInfo.STRING_TYPE_INFO)
                .addSink(new PrintSinkFunction<>());

        source1.join(source3)
                .where(keySelector)
                .equalTo(keySelector)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(1)))
                .apply(joinFunction, BasicTypeInfo.STRING_TYPE_INFO)
                .addSink(new PrintSinkFunction<>());

        env.execute();
    }

    public static void main(String[] args) throws Exception {

        final int numRecordsToSend = 100;
        Configuration configuration = configureHybridOptions(getConfiguration(), false);
        executeJobGraph(numRecordsToSend, configuration);
    }
}
