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
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkOutputMultiplexer;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.streaming.api.operators.util.PausableRelativeClock;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * An implementation of {@link TimestampsAndWatermarks} that does periodic watermark emission and
 * keeps track of watermarks on a per-split basis. This should be used in execution contexts where
 * watermarks are important for efficiency/correctness, for example in STREAMING execution mode.
 *
 * @param <T> The type of the emitted records.
 */
@Internal
public class ProgressiveTimestampsAndWatermarks<T> implements TimestampsAndWatermarks<T> {

    private final TimestampAssigner<T> timestampAssigner;

    private final WatermarkGeneratorSupplier<T> watermarksFactory;

    private final TimestampsAndWatermarksContextProvider watermarksContextProvider;

    private final ProcessingTimeService timeService;

    private final long periodicWatermarkInterval;

    private final PausableRelativeClock mainInputActivityClock;

    @Nullable private SplitLocalOutputs<T> currentPerSplitOutputs;

    @Nullable private StreamingReaderOutput<T> currentMainOutput;

    @Nullable private ScheduledFuture<?> periodicEmitHandle;

    public ProgressiveTimestampsAndWatermarks(
            TimestampAssigner<T> timestampAssigner,
            WatermarkGeneratorSupplier<T> watermarksFactory,
            TimestampsAndWatermarksContextProvider watermarksContextProvider,
            ProcessingTimeService timeService,
            Duration periodicWatermarkInterval,
            PausableRelativeClock mainInputActivityClock) {

        this.timestampAssigner = timestampAssigner;
        this.watermarksFactory = watermarksFactory;
        this.watermarksContextProvider = watermarksContextProvider;
        this.timeService = timeService;
        this.mainInputActivityClock = mainInputActivityClock;

        long periodicWatermarkIntervalMillis;
        try {
            periodicWatermarkIntervalMillis = periodicWatermarkInterval.toMillis();
        } catch (ArithmeticException ignored) {
            // long integer overflow
            periodicWatermarkIntervalMillis = Long.MAX_VALUE;
        }
        this.periodicWatermarkInterval = periodicWatermarkIntervalMillis;
    }

    // ------------------------------------------------------------------------

    @Override
    public ReaderOutput<T> createMainOutput(
            PushingAsyncDataInput.DataOutput<T> output,
            WatermarkUpdateListener watermarkUpdateListener) {
        // At the moment, we assume only one output is ever created!
        // This assumption is strict, currently, because many of the classes in this implementation
        // do not
        // support re-assigning the underlying output
        checkState(
                currentMainOutput == null && currentPerSplitOutputs == null,
                "already created a main output");

        final WatermarkGenerator<T> watermarkGenerator =
                watermarksFactory.createWatermarkGenerator(
                        watermarksContextProvider.create(mainInputActivityClock));

        // Downstream operators process every emitted element synchronously on the task thread,
        // during which no split can be polled. When something measures input activity time (i.e.
        // idleness detection is configured), hide that time from the activity clocks; otherwise
        // keep the plain output to leave the hot path untouched.
        final PushingAsyncDataInput.DataOutput<T> recordOutput =
                watermarksContextProvider.isInputActivityClockRequested()
                        ? new ActivityClockPausingDataOutput<>(output, mainInputActivityClock)
                        : output;

        final WatermarkOutput watermarkOutput =
                new WatermarkToDataOutput(recordOutput, watermarkUpdateListener);
        IdlenessManager idlenessManager = new IdlenessManager(watermarkOutput);

        currentPerSplitOutputs =
                new SplitLocalOutputs<>(
                        recordOutput,
                        idlenessManager.getSplitLocalOutput(),
                        watermarkUpdateListener,
                        timestampAssigner,
                        watermarksFactory,
                        watermarksContextProvider,
                        mainInputActivityClock);

        currentMainOutput =
                new StreamingReaderOutput<>(
                        recordOutput,
                        idlenessManager.getMainOutput(),
                        timestampAssigner,
                        watermarkGenerator,
                        currentPerSplitOutputs);

        return currentMainOutput;
    }

    @Override
    public void startPeriodicWatermarkEmits() {
        checkState(periodicEmitHandle == null, "periodic emitter already started");

        if (periodicWatermarkInterval == 0) {
            // a value of zero means not activated
            return;
        }

        periodicEmitHandle =
                timeService.scheduleWithFixedDelay(
                        this::emitImmediateWatermark,
                        periodicWatermarkInterval,
                        periodicWatermarkInterval);
    }

    @Override
    public void stopPeriodicWatermarkEmits() {
        if (periodicEmitHandle != null) {
            periodicEmitHandle.cancel(false);
            periodicEmitHandle = null;
        }
    }

    @Override
    public void emitImmediateWatermark(@SuppressWarnings("unused") long wallClockTimestamp) {
        if (currentPerSplitOutputs != null) {
            currentPerSplitOutputs.emitPeriodicWatermark();
        }
        if (currentMainOutput != null) {
            currentMainOutput.emitPeriodicWatermark();
        }
    }

    @Override
    public void pauseOrResumeSplits(
            Collection<String> splitsToPause, Collection<String> splitsToResume) {
        currentPerSplitOutputs.pauseOrResumeSplits(splitsToPause, splitsToResume);
    }

    // ------------------------------------------------------------------------

    private static final class StreamingReaderOutput<T> extends SourceOutputWithWatermarks<T>
            implements ReaderOutput<T> {

        private final SplitLocalOutputs<T> splitLocalOutputs;

        StreamingReaderOutput(
                PushingAsyncDataInput.DataOutput<T> output,
                WatermarkOutput watermarkOutput,
                TimestampAssigner<T> timestampAssigner,
                WatermarkGenerator<T> watermarkGenerator,
                SplitLocalOutputs<T> splitLocalOutputs) {

            super(output, watermarkOutput, watermarkOutput, timestampAssigner, watermarkGenerator);
            this.splitLocalOutputs = splitLocalOutputs;
        }

        @Override
        public SourceOutput<T> createOutputForSplit(String splitId) {
            return splitLocalOutputs.createOutputForSplit(splitId);
        }

        @Override
        public void releaseOutputForSplit(String splitId) {
            splitLocalOutputs.releaseOutputForSplit(splitId);
        }
    }

    // ------------------------------------------------------------------------

    /**
     * A holder and factory for split-local {@link SourceOutput}s. The split-local outputs maintain
     * local watermark generators with their own state, to facilitate per-split watermarking logic.
     *
     * @param <T> The type of the emitted records.
     */
    private static final class SplitLocalOutputs<T> {

        private final WatermarkOutputMultiplexer watermarkMultiplexer;
        private final Map<String, SourceOutputWithWatermarks<T>> localOutputs;
        private final Map<String, PausableRelativeClock> inputActivityClocks = new HashMap<>();
        private final PushingAsyncDataInput.DataOutput<T> recordOutput;
        private final TimestampAssigner<T> timestampAssigner;
        private final WatermarkGeneratorSupplier<T> watermarksFactory;
        private final TimestampsAndWatermarksContextProvider watermarksContextProvider;
        private final WatermarkUpdateListener watermarkUpdateListener;
        private final PausableRelativeClock mainInputActivityClock;

        private SplitLocalOutputs(
                PushingAsyncDataInput.DataOutput<T> recordOutput,
                WatermarkOutput watermarkOutput,
                WatermarkUpdateListener watermarkUpdateListener,
                TimestampAssigner<T> timestampAssigner,
                WatermarkGeneratorSupplier<T> watermarksFactory,
                TimestampsAndWatermarksContextProvider watermarksContextProvider,
                PausableRelativeClock mainInputActivityClock) {

            this.recordOutput = recordOutput;
            this.timestampAssigner = timestampAssigner;
            this.watermarksFactory = watermarksFactory;
            this.watermarksContextProvider = watermarksContextProvider;
            this.watermarkUpdateListener = watermarkUpdateListener;
            this.mainInputActivityClock = mainInputActivityClock;

            this.watermarkMultiplexer = new WatermarkOutputMultiplexer(watermarkOutput);
            this.localOutputs =
                    new LinkedHashMap<>(); // we use a LinkedHashMap because it iterates faster
        }

        SourceOutput<T> createOutputForSplit(String splitId) {
            final SourceOutputWithWatermarks<T> previous = localOutputs.get(splitId);
            if (previous != null) {
                return previous;
            }

            PausableRelativeClock inputActivityClock = createInputActivityClock(splitId);
            watermarkMultiplexer.registerNewOutput(
                    splitId,
                    new WatermarkOutputMultiplexer.WatermarkUpdateListener() {
                        @Override
                        public void onWatermarkUpdate(long watermark) {
                            watermarkUpdateListener.updateCurrentSplitWatermark(splitId, watermark);
                        }

                        @Override
                        public void onIdleUpdate(boolean idle) {
                            watermarkUpdateListener.updateCurrentSplitIdle(splitId, idle);
                        }
                    });
            final WatermarkOutput onEventOutput = watermarkMultiplexer.getImmediateOutput(splitId);
            final WatermarkOutput periodicOutput = watermarkMultiplexer.getDeferredOutput(splitId);

            final WatermarkGenerator<T> watermarks =
                    watermarksFactory.createWatermarkGenerator(
                            watermarksContextProvider.create(inputActivityClock));

            final SourceOutputWithWatermarks<T> localOutput =
                    SourceOutputWithWatermarks.createWithSeparateOutputs(
                            recordOutput,
                            onEventOutput,
                            periodicOutput,
                            timestampAssigner,
                            watermarks);

            localOutputs.put(splitId, localOutput);
            return localOutput;
        }

        private PausableRelativeClock createInputActivityClock(String splitId) {
            // Dedicated inputActivityClock for a particular split, layered on the main input
            // activity clock: it is paused whenever the main clock is (backpressure, downstream
            // processing) and additionally when this split is paused due to watermark alignment.
            PausableRelativeClock inputActivityClock =
                    new PausableRelativeClock(mainInputActivityClock);
            inputActivityClocks.put(splitId, inputActivityClock);
            return inputActivityClock;
        }

        void releaseOutputForSplit(String splitId) {
            watermarkUpdateListener.splitFinished(splitId);
            localOutputs.remove(splitId);
            watermarkMultiplexer.unregisterOutput(splitId);
            requireNonNull(inputActivityClocks.remove(splitId));
        }

        void emitPeriodicWatermark() {
            // The call in the loop only records the next watermark candidate for each local output.
            // The call to 'watermarkMultiplexer.onPeriodicEmit()' actually merges the watermarks.
            // That way, we save inefficient repeated merging of (partially outdated) watermarks
            // before
            // all local generators have emitted their candidates.
            for (SourceOutputWithWatermarks<?> output : localOutputs.values()) {
                output.emitPeriodicWatermark();
            }
            watermarkMultiplexer.onPeriodicEmit();
        }

        public void pauseOrResumeSplits(
                Collection<String> splitsToPause, Collection<String> splitsToResume) {
            for (String splitId : splitsToPause) {
                inputActivityClocks.get(splitId).pause();
            }
            for (String splitId : splitsToResume) {
                inputActivityClocks.get(splitId).unPause();
            }
        }
    }

    /**
     * A helper class for managing idleness status of the underlying output.
     *
     * <p>This class tracks the idleness status of main and split-local output, and only marks the
     * underlying output as idle if both main and per-split output are idle.
     *
     * <p>The reason of adding this manager is that the implementation of source reader might only
     * use one of main or split-local output for emitting records and watermarks, and we could avoid
     * watermark generator on the vacant output keep marking the underlying output as idle.
     */
    private static class IdlenessManager {
        private final WatermarkOutput underlyingOutput;
        private final IdlenessAwareWatermarkOutput splitLocalOutput;
        private final IdlenessAwareWatermarkOutput mainOutput;

        IdlenessManager(WatermarkOutput underlyingOutput) {
            this.underlyingOutput = underlyingOutput;
            this.splitLocalOutput = new IdlenessAwareWatermarkOutput(underlyingOutput);
            this.mainOutput = new IdlenessAwareWatermarkOutput(underlyingOutput);
        }

        IdlenessAwareWatermarkOutput getSplitLocalOutput() {
            return splitLocalOutput;
        }

        IdlenessAwareWatermarkOutput getMainOutput() {
            return mainOutput;
        }

        void maybeMarkUnderlyingOutputAsIdle() {
            if (splitLocalOutput.isIdle && mainOutput.isIdle) {
                underlyingOutput.markIdle();
            }
        }

        private class IdlenessAwareWatermarkOutput implements WatermarkOutput {
            private final WatermarkOutput underlyingOutput;
            private boolean isIdle = true;

            private IdlenessAwareWatermarkOutput(WatermarkOutput underlyingOutput) {
                this.underlyingOutput = underlyingOutput;
            }

            @Override
            public void emitWatermark(Watermark watermark) {
                underlyingOutput.emitWatermark(watermark);
                isIdle = false;
            }

            @Override
            public void markIdle() {
                isIdle = true;
                maybeMarkUnderlyingOutputAsIdle();
            }

            @Override
            public void markActive() {
                isIdle = false;
                underlyingOutput.markActive();
            }
        }
    }
}
