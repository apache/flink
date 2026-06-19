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

package org.apache.flink.datastream.impl.extension.eventtime.functions;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.WatermarksWithIdleness.IdlenessTimer;
import org.apache.flink.api.common.watermark.WatermarkDeclaration;
import org.apache.flink.api.common.watermark.WatermarkManager;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.apache.flink.datastream.api.extension.eventtime.EventTimeExtension;
import org.apache.flink.datastream.api.extension.eventtime.strategy.EventTimeWatermarkStrategy;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkState;

/** A specialized process function designed for extracting event timestamps. */
public class ExtractEventTimeProcessFunction<IN>
        implements OneInputStreamProcessFunction<IN, IN>,
                ProcessingTimeService.ProcessingTimeCallback {

    /** User-defined watermark strategy. */
    private final EventTimeWatermarkStrategy<IN> watermarkStrategy;

    /** The maximum timestamp encountered so far. */
    private long currentMaxEventTime = Long.MIN_VALUE;

    private long lastEmittedEventTime = Long.MIN_VALUE;

    /**
     * The periodic processing timer interval; if not configured by user in {@link
     * EventTimeWatermarkStrategy}, it will default to the value specified by {@link
     * PipelineOptions#AUTO_WATERMARK_INTERVAL}.
     */
    private long periodicTimerInterval = 0;

    /**
     * Whether enable create and send {@link EventTimeExtension#IDLE_STATUS_WATERMARK_DECLARATION}.
     */
    private boolean enableIdleStatus;

    /** The {@link IdlenessTimer} is utilized to check whether the input is currently idle. */
    private IdlenessTimer idlenessTimer;

    private boolean isIdleNow = false;

    private final long maxOutOfOrderTimeInMs;

    private ProcessingTimeService processingTimeService;

    private WatermarkManager watermarkManager;

    public ExtractEventTimeProcessFunction(EventTimeWatermarkStrategy<IN> watermarkStrategy) {
        this.watermarkStrategy = watermarkStrategy;
        if (watermarkStrategy.getIdleTimeout().toMillis() > 0) {
            this.enableIdleStatus = true;
        }
        this.maxOutOfOrderTimeInMs = watermarkStrategy.getMaxOutOfOrderTime().toMillis();
    }

    public void initEventTimeExtension(
            ExecutionConfig config,
            WatermarkManager watermarkManager,
            ProcessingTimeService processingTimeService) {
        this.processingTimeService = processingTimeService;
        this.watermarkManager = watermarkManager;

        if (enableIdleStatus) {
            this.idlenessTimer =
                    new IdlenessTimer(
                            processingTimeService.getClock(), watermarkStrategy.getIdleTimeout());
        }

        // May need register timer to check whether the input is idle and periodically send event
        // time watermarks
        boolean needRegisterTimer =
                watermarkStrategy.getGenerateMode()
                                == EventTimeWatermarkStrategy.EventTimeWatermarkGenerateMode
                                        .PERIODIC
                        || enableIdleStatus;
        // set timer interval default to config option {@link
        // PipelineOptions#AUTO_WATERMARK_INTERVAL}
        this.periodicTimerInterval = config.getAutoWatermarkInterval();
        if (watermarkStrategy.getGenerateMode()
                        == EventTimeWatermarkStrategy.EventTimeWatermarkGenerateMode.PERIODIC
                && !watermarkStrategy.getPeriodicWatermarkInterval().isZero()) {
            this.periodicTimerInterval =
                    watermarkStrategy.getPeriodicWatermarkInterval().toMillis();
        }
        checkState(
                periodicTimerInterval > 0,
                "Watermark interval " + periodicTimerInterval + " should large to 0.");

        if (needRegisterTimer) {
            processingTimeService.registerTimer(
                    processingTimeService.getCurrentProcessingTime() + periodicTimerInterval, this);
        }
    }

    @Override
    public Set<? extends WatermarkDeclaration> declareWatermarks() {
        // declare EventTimeExtension.EVENT_TIME_WATERMARK_DECLARATION
        // if idle status is enabled, also declare
        // EventTimeExtension.IDLE_STATUS_WATERMARK_DECLARATION.
        Set<WatermarkDeclaration> watermarkDeclarations = new HashSet<>();
        watermarkDeclarations.add(EventTimeExtension.EVENT_TIME_WATERMARK_DECLARATION);
        if (enableIdleStatus) {
            watermarkDeclarations.add(EventTimeExtension.IDLE_STATUS_WATERMARK_DECLARATION);
        }
        return watermarkDeclarations;
    }

    @Override
    public void processRecord(IN record, Collector<IN> output, PartitionedContext<IN> ctx)
            throws Exception {
        if (enableIdleStatus) {
            if (isIdleNow) {
                watermarkManager.emitWatermark(
                        EventTimeExtension.IDLE_STATUS_WATERMARK_DECLARATION.newWatermark(false));
                isIdleNow = false;
            }

            // mark current input as active
            idlenessTimer.activity();
        }

        // extract event time from record
        long extractedEventTime =
                watermarkStrategy.getEventTimeExtractor().extractTimestamp(record);
        currentMaxEventTime = Math.max(currentMaxEventTime, extractedEventTime);
        output.collectAndOverwriteTimestamp(record, extractedEventTime);

        if (watermarkStrategy.getGenerateMode()
                == EventTimeWatermarkStrategy.EventTimeWatermarkGenerateMode.PER_EVENT) {
            // If the per event watermark is utilized, create event time watermark and send
            tryEmitEventTimeWatermark(ctx.getNonPartitionedContext().getWatermarkManager());
        }
    }

    /**
     * The processing timer has two goals: 1. check whether the input is idle 2. periodically emit
     * event time watermark
     */
    @Override
    public void onProcessingTime(long time) throws IOException, InterruptedException, Exception {
        if (enableIdleStatus && idlenessTimer.checkIfIdle()) {
            if (!isIdleNow) {
                watermarkManager.emitWatermark(
                        EventTimeExtension.IDLE_STATUS_WATERMARK_DECLARATION.newWatermark(true));
                isIdleNow = true;
            }
        } else if (watermarkStrategy.getGenerateMode()
                == EventTimeWatermarkStrategy.EventTimeWatermarkGenerateMode.PERIODIC) {
            tryEmitEventTimeWatermark(watermarkManager);
        }

        processingTimeService.registerTimer(time + periodicTimerInterval, this);
    }

    private void tryEmitEventTimeWatermark(WatermarkManager watermarkManager) {
        if (currentMaxEventTime == Long.MIN_VALUE) {
            return;
        }

        long needEmittedEventTime = currentMaxEventTime - maxOutOfOrderTimeInMs;
        if (needEmittedEventTime > lastEmittedEventTime) {
            watermarkManager.emitWatermark(
                    EventTimeExtension.EVENT_TIME_WATERMARK_DECLARATION.newWatermark(
                            needEmittedEventTime));
            lastEmittedEventTime = needEmittedEventTime;
        }
    }
}
