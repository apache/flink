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

package org.apache.flink.streaming.api.operators.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.RelativeClock;

import java.time.Duration;
import java.util.Collection;

/**
 * Basic interface for the timestamp extraction and watermark generation logic for the {@link
 * org.apache.flink.api.connector.source.SourceReader}.
 *
 * <p>Implementations of this class may or may not actually perform certain tasks, like watermark
 * generation. For example, the batch-oriented implementation typically skips all watermark
 * generation logic.
 *
 * @param <T> The type of the emitted records.
 */
@Internal
public interface TimestampsAndWatermarks<T> {

    /** Lets the owner/creator of the output know about latest emitted watermark. */
    @Internal
    interface WatermarkUpdateListener {

        /** It should be called once the idle is changed. */
        void updateIdle(boolean isIdle);

        /**
         * Update the effective watermark. If an output becomes idle, please call {@link
         * this#updateIdle} instead of update the watermark to {@link Long#MAX_VALUE}. Because the
         * output needs to distinguish between idle and real watermark.
         */
        void updateCurrentEffectiveWatermark(long watermark);

        /** Notifies about changes to per split watermarks. */
        void updateCurrentSplitWatermark(String splitId, long watermark);

        /** Notifies that split has finished. */
        void splitFinished(String splitId);
    }

    /**
     * Creates the ReaderOutput for the source reader, than internally runs the timestamp extraction
     * and watermark generation.
     */
    ReaderOutput<T> createMainOutput(
            PushingAsyncDataInput.DataOutput<T> output, WatermarkUpdateListener watermarkCallback);

    /**
     * Starts emitting periodic watermarks, if this implementation produces watermarks, and if
     * periodic watermarks are configured.
     *
     * <p>Periodic watermarks are produced by periodically calling the {@link
     * org.apache.flink.api.common.eventtime.WatermarkGenerator#onPeriodicEmit(WatermarkOutput)}
     * method of the underlying Watermark Generators.
     */
    void startPeriodicWatermarkEmits();

    /** Stops emitting periodic watermarks. */
    void stopPeriodicWatermarkEmits();

    /** Emit a watermark immediately. */
    void emitImmediateWatermark(long wallClockTimestamp);

    void pauseOrResumeSplits(Collection<String> splitsToPause, Collection<String> splitsToResume);

    // ------------------------------------------------------------------------
    //  factories
    // ------------------------------------------------------------------------

    static <E> TimestampsAndWatermarks<E> createProgressiveEventTimeLogic(
            WatermarkStrategy<E> watermarkStrategy,
            MetricGroup metrics,
            ProcessingTimeService timeService,
            long periodicWatermarkIntervalMillis,
            RelativeClock mainInputActivityClock,
            Clock clock,
            TaskIOMetricGroup taskIOMetricGroup) {

        TimestampsAndWatermarksContextProvider contextProvider =
                new TimestampsAndWatermarksContextProvider(metrics);
        TimestampAssigner<E> timestampAssigner =
                watermarkStrategy.createTimestampAssigner(
                        contextProvider.create(mainInputActivityClock));

        return new ProgressiveTimestampsAndWatermarks<>(
                timestampAssigner,
                watermarkStrategy,
                contextProvider,
                timeService,
                Duration.ofMillis(periodicWatermarkIntervalMillis),
                mainInputActivityClock,
                clock,
                taskIOMetricGroup);
    }

    static <E> TimestampsAndWatermarks<E> createNoOpEventTimeLogic(
            WatermarkStrategy<E> watermarkStrategy,
            MetricGroup metrics,
            RelativeClock inputActivityClock) {

        final TimestampsAndWatermarksContext context =
                new TimestampsAndWatermarksContext(metrics, inputActivityClock);
        final TimestampAssigner<E> timestampAssigner =
                watermarkStrategy.createTimestampAssigner(context);

        return new NoOpTimestampsAndWatermarks<>(timestampAssigner);
    }

    @Internal
    class TimestampsAndWatermarksContextProvider {
        private final MetricGroup metrics;

        public TimestampsAndWatermarksContextProvider(MetricGroup metrics) {
            this.metrics = metrics;
        }

        public TimestampsAndWatermarksContext create(RelativeClock inputActivityClock) {
            return new TimestampsAndWatermarksContext(metrics, inputActivityClock);
        }
    }
}
