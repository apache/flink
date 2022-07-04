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

package org.apache.flink.test.streaming.api.datastream;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Test that ensures watermarks are correctly propagating with finished sources. This test has one
 * short living source that finishes immediately. Then after 5th checkpoint job fails over, and then
 * it makes sure that the watermark emitted from the other still working source around checkpoint
 * 10, is reaching the sink. Only once this happens, the long living source is allowed to exit. If
 * the watermark is not propagated/silently swallowed (as for example in FLINK-28357), the test is
 * expected to livelock.
 */
public class FinishedSourcesWatermarkITCase extends TestLogger {

    private static final AtomicLong CHECKPOINT_10_WATERMARK =
            new AtomicLong(Watermark.MAX_WATERMARK.getTimestamp());
    private static final AtomicBoolean DOWNSTREAM_CHECKPOINT_10_WATERMARK_ACK = new AtomicBoolean();

    @Test
    public void testTwoConsecutiveFinishedTasksShouldPropagateMaxWatermark() throws Exception {
        Configuration conf = new Configuration();
        conf.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1, conf);

        // disable chaining to make sure we will have two consecutive checkpoints with Task ==
        // FINISHED
        env.disableOperatorChaining();
        // Make sure that the short living source has plenty of time to finish before the 5th
        // checkpoint
        env.enableCheckpointing(200);

        // create our sources - one that will want to run forever, and another that finishes
        // immediately
        DataStream<String> runningStreamIn =
                env.addSource(new LongRunningSource(), "Long Running Source");
        DataStream<String> emptyStream =
                env.addSource(new ShortLivedEmptySource(), "Short Lived Source");

        // pass the empty stream through a simple map() function
        DataStream<String> mappedEmptyStream = emptyStream.map(v -> v).name("Empty Stream Map");

        // join the two streams together to see what watermark is reached during startup and after a
        // recovery
        runningStreamIn
                .connect(mappedEmptyStream)
                .process(new NoopCoProcessFunction())
                .name("Join")
                .addSink(new SinkWaitingForWatermark());

        env.execute();
    }

    private static class SinkWaitingForWatermark implements SinkFunction<String> {
        @Override
        public void writeWatermark(org.apache.flink.api.common.eventtime.Watermark watermark) {
            if (watermark.getTimestamp() > CHECKPOINT_10_WATERMARK.get()) {
                DOWNSTREAM_CHECKPOINT_10_WATERMARK_ACK.set(true);
            }
        }
    }

    private static class LongRunningSource extends RichSourceFunction<String>
            implements CheckpointListener {
        private volatile boolean isRunning = true;
        private long lastEmittedWatermark;

        @Override
        public void run(SourceContext<String> sourceContext) throws Exception {
            while (isRunning && !DOWNSTREAM_CHECKPOINT_10_WATERMARK_ACK.get()) {
                synchronized (sourceContext.getCheckpointLock()) {
                    lastEmittedWatermark =
                            Math.max(System.currentTimeMillis(), lastEmittedWatermark);
                    sourceContext.emitWatermark(new Watermark(lastEmittedWatermark));
                }
                Thread.sleep(1);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            if (checkpointId == 5) {
                throw new RuntimeException("Force recovery");
            }
            if (checkpointId > 10) {
                CHECKPOINT_10_WATERMARK.set(
                        Math.min(lastEmittedWatermark, CHECKPOINT_10_WATERMARK.get()));
            }
        }
    }

    private static class ShortLivedEmptySource extends RichSourceFunction<String> {
        @Override
        public void run(SourceContext<String> sourceContext) throws Exception {}

        public void cancel() {}
    }

    private static class NoopCoProcessFunction extends CoProcessFunction<String, String, String> {
        @Override
        public void processElement1(String val, Context context, Collector<String> collector) {}

        @Override
        public void processElement2(String val, Context context, Collector<String> collector) {}
    }
}
