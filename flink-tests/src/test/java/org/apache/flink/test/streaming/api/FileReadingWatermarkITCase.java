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

package org.apache.flink.test.streaming.api;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Preconditions;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

import static org.apache.flink.util.Preconditions.checkState;
import static org.junit.Assert.assertEquals;

/**
 * Tests that watermarks are emitted while file is being read, particularly the last split.
 *
 * @see <a href="https://issues.apache.org/jira/browse/FLINK-19109">FLINK-19109</a>
 */
public class FileReadingWatermarkITCase {
    private static final String NUM_WATERMARKS_ACC_NAME = "numWatermarks";
    private static final String RUNTIME_ACC_NAME = "runtime";
    private static final int FILE_SIZE_LINES = 5_000_000;
    private static final int WATERMARK_INTERVAL_MILLIS = 10;
    private static final int MIN_EXPECTED_WATERMARKS = 5;

    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testWatermarkEmissionWithChaining() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        env.getConfig().setAutoWatermarkInterval(WATERMARK_INTERVAL_MILLIS);

        checkState(env.isChainingEnabled());

        Tuple3<File, String, String> sourceFileAndFirstLastLines = getSourceFile(FILE_SIZE_LINES);
        env.readTextFile(sourceFileAndFirstLastLines.f0.getAbsolutePath())
                .assignTimestampsAndWatermarks(getExtractorAssigner())
                .addSink(
                        getWatermarkCounter(
                                sourceFileAndFirstLastLines.f1, sourceFileAndFirstLastLines.f2));

        JobExecutionResult result = env.execute();

        int actual = result.getAccumulatorResult(NUM_WATERMARKS_ACC_NAME);
        long expected =
                result.<Long>getAccumulatorResult(RUNTIME_ACC_NAME) / WATERMARK_INTERVAL_MILLIS;
        checkState(expected > MIN_EXPECTED_WATERMARKS);

        assertEquals(
                "too few watermarks emitted in "
                        + result.<Long>getAccumulatorResult(RUNTIME_ACC_NAME)
                        + " ms",
                expected,
                actual,
                .5 * expected);
    }

    private Tuple3<File, String, String> getSourceFile(int numLines) throws IOException {
        Preconditions.checkArgument(numLines > 0);
        File file = temporaryFolder.newFile();
        String first = "0", last = null;
        try (PrintWriter printWriter = new PrintWriter(file)) {
            for (int i = 0; i < numLines; i++) {
                printWriter.println(i);
                last = Integer.toString(i);
            }
        }
        return Tuple3.of(file, first, last);
    }

    private static BoundedOutOfOrdernessTimestampExtractor<String> getExtractorAssigner() {
        return new BoundedOutOfOrdernessTimestampExtractor<String>(Time.hours(1)) {
            private final long started = System.currentTimeMillis();

            @Override
            public long extractTimestamp(String line) {
                return started + Long.parseLong(line);
            }
        };
    }

    private static SinkFunction<String> getWatermarkCounter(
            final String firstElement, final String lastElement) {
        return new RichSinkFunction<String>() {
            private long start;
            private final IntCounter numWatermarks = new IntCounter();
            private final LongCounter runtime = new LongCounter();
            private long lastWatermark = -1;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                getRuntimeContext().addAccumulator(NUM_WATERMARKS_ACC_NAME, numWatermarks);
                getRuntimeContext().addAccumulator(RUNTIME_ACC_NAME, runtime);
            }

            @Override
            public void invoke(String value, SinkFunction.Context context) {
                if (value.equals(firstElement)) {
                    start = System.nanoTime();
                }
                if (value.equals(lastElement)) {
                    runtime.add((System.nanoTime() - start) / 1_000_000);
                }
            }

            @Override
            public void writeWatermark(Watermark watermark) {
                if (watermark.getTimestamp() != lastWatermark) {
                    lastWatermark = watermark.getTimestamp();
                    numWatermarks.add(1);
                }
            }
        };
    }
}
