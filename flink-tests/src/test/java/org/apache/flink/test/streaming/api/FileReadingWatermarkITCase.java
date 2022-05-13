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

import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.util.InfiniteIntegerInputFormat;
import org.apache.flink.testutils.junit.SharedObjects;
import org.apache.flink.testutils.junit.SharedReference;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Tests that watermarks are emitted while file is being read, particularly the last split.
 *
 * @see <a href="https://issues.apache.org/jira/browse/FLINK-19109">FLINK-19109</a>
 */
public class FileReadingWatermarkITCase extends TestLogger {
    private static final Logger LOG = LoggerFactory.getLogger(FileReadingWatermarkITCase.class);

    private static final int WATERMARK_INTERVAL_MILLIS = 1_000;
    private static final int EXPECTED_WATERMARKS = 5;

    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule public final SharedObjects sharedObjects = SharedObjects.create();
    /**
     * Adds an infinite split that causes the input of {@link
     * org.apache.flink.streaming.api.functions.source.ContinuousFileReaderOperator} to instantly go
     * idle while data is still being processed.
     *
     * <p>Before FLINK-19109, watermarks would not be emitted at this point.
     */
    @Test
    public void testWatermarkEmissionWithChaining() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        env.getConfig().setAutoWatermarkInterval(WATERMARK_INTERVAL_MILLIS);
        SharedReference<CountDownLatch> latch =
                sharedObjects.add(new CountDownLatch(EXPECTED_WATERMARKS));
        checkState(env.isChainingEnabled());
        env.createInput(new InfiniteIntegerInputFormat(true))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Integer>forMonotonousTimestamps()
                                .withTimestampAssigner(context -> getExtractorAssigner()))
                .addSink(getWatermarkCounter(latch));
        env.executeAsync();
        latch.get().await();
    }

    private static TimestampAssigner<Integer> getExtractorAssigner() {
        return new TimestampAssigner<Integer>() {
            private long counter = 1;

            @Override
            public long extractTimestamp(Integer element, long recordTimestamp) {
                return counter++;
            }
        };
    }

    private static SinkFunction<Integer> getWatermarkCounter(
            final SharedReference<CountDownLatch> latch) {
        return new RichSinkFunction<Integer>() {
            @Override
            public void invoke(Integer value, SinkFunction.Context context) {
                try {
                    Thread.sleep(1000);
                    LOG.info("Sink received record");
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void writeWatermark(Watermark watermark) {
                LOG.info("Sink received watermark {}", watermark);
                latch.get().countDown();
            }
        };
    }
}
