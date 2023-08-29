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

package org.apache.flink.streaming.tests;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import org.apache.flink.shaded.netty4.io.netty.util.internal.OutOfDirectMemoryError;

import sun.misc.Unsafe;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Test program to verify the direct memory consumption of Netty. Without zero-copy Netty may create
 * more than one chunk, thus we may encounter {@link OutOfDirectMemoryError} if we limit the total
 * direct memory to be less than two chunks. Instead, with zero-copy introduced in
 * (https://issues.apache.org/jira/browse/FLINK-10742) one chunk will be enough and the exception
 * will not occur.
 *
 * <p>Since Netty uses low level API of {@link Unsafe} to allocate direct buffer when using JDK8 and
 * these memory will not be counted in direct memory, the test is only effective when JDK11 is used.
 */
public class NettyShuffleMemoryControlTestProgram {
    private static final int RECORD_LENGTH = 2048;

    private static final ConfigOption<Integer> RUNNING_TIME_IN_SECONDS =
            ConfigOptions.key("test.running_time_in_seconds")
                    .intType()
                    .defaultValue(120)
                    .withDescription("The time to run.");

    private static final ConfigOption<Integer> MAP_PARALLELISM =
            ConfigOptions.key("test.map_parallelism")
                    .intType()
                    .defaultValue(1)
                    .withDescription("The number of map tasks.");

    private static final ConfigOption<Integer> REDUCE_PARALLELISM =
            ConfigOptions.key("test.reduce_parallelism")
                    .intType()
                    .defaultValue(1)
                    .withDescription("The number of reduce tasks.");

    public static void main(String[] args) throws Exception {
        // parse the parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        final int runningTimeInSeconds =
                params.getInt(
                        RUNNING_TIME_IN_SECONDS.key(), RUNNING_TIME_IN_SECONDS.defaultValue());
        final int mapParallelism =
                params.getInt(MAP_PARALLELISM.key(), MAP_PARALLELISM.defaultValue());
        final int reduceParallelism =
                params.getInt(REDUCE_PARALLELISM.key(), REDUCE_PARALLELISM.defaultValue());

        checkArgument(
                runningTimeInSeconds > 0,
                "The running time in seconds should be positive, but it is {}",
                runningTimeInSeconds);
        checkArgument(
                mapParallelism > 0,
                "The number of map tasks should be positive, but it is {}",
                mapParallelism);
        checkArgument(
                reduceParallelism > 0,
                "The number of reduce tasks should be positive, but it is {}",
                reduceParallelism);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new StringSourceFunction(runningTimeInSeconds))
                .setParallelism(mapParallelism)
                .slotSharingGroup("a")
                .shuffle()
                .addSink(new DummySink())
                .setParallelism(reduceParallelism)
                .slotSharingGroup("b");

        // execute program
        env.execute("Netty Shuffle Memory Control Test");
    }

    /**
     * @deprecated This class is based on the {@link
     *     org.apache.flink.streaming.api.functions.source.SourceFunction} API, which is due to be
     *     removed. Use the new {@link org.apache.flink.api.connector.source.Source} API instead.
     */
    @Deprecated
    private static class StringSourceFunction extends RichParallelSourceFunction<String> {
        private static final long serialVersionUID = 1L;

        private volatile boolean isRunning;

        private final long runningTimeInSeconds;

        private transient long stopTime;

        public StringSourceFunction(long runningTimeInSeconds) {
            this.runningTimeInSeconds = runningTimeInSeconds;
        }

        @Override
        public void open(OpenContext openContext) {
            isRunning = true;
            stopTime = System.nanoTime() + runningTimeInSeconds * 1_000_000_000L;
        }

        @Override
        public void run(SourceContext<String> ctx) {
            byte[] bytes = new byte[RECORD_LENGTH];
            for (int i = 0; i < RECORD_LENGTH; ++i) {
                bytes[i] = 'a';
            }
            String str = new String(bytes);

            while (isRunning && (System.nanoTime() < stopTime)) {
                ctx.collect(str);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    private static class DummySink extends RichSinkFunction<String> {

        @Override
        public void invoke(String value, Context context) throws Exception {
            // Do nothing.
        }
    }
}
