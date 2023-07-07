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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput;
import org.apache.flink.streaming.runtime.tasks.ExceptionInChainedOperatorException;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An adapter that exposes a {@link WatermarkOutput} based on a {@link
 * PushingAsyncDataInput.DataOutput}.
 */
@Internal
public final class WatermarkToDataOutput implements WatermarkOutput {

    private final PushingAsyncDataInput.DataOutput<?> output;
    private final TimestampsAndWatermarks.WatermarkUpdateListener watermarkEmitted;
    private long maxWatermarkSoFar;
    private boolean isIdle;

    @VisibleForTesting
    public WatermarkToDataOutput(PushingAsyncDataInput.DataOutput<?> output) {
        this(
                output,
                new TimestampsAndWatermarks.WatermarkUpdateListener() {
                    @Override
                    public void updateIdle(boolean isIdle) {}

                    @Override
                    public void updateCurrentEffectiveWatermark(long watermark) {}

                    @Override
                    public void updateCurrentSplitWatermark(String splitId, long watermark) {}
                });
    }

    /** Creates a new WatermarkOutput against the given DataOutput. */
    public WatermarkToDataOutput(
            PushingAsyncDataInput.DataOutput<?> output,
            TimestampsAndWatermarks.WatermarkUpdateListener watermarkEmitted) {
        this.output = checkNotNull(output);
        this.watermarkEmitted = checkNotNull(watermarkEmitted);
        this.maxWatermarkSoFar = Long.MIN_VALUE;
    }

    @Override
    public void emitWatermark(Watermark watermark) {
        final long newWatermark = watermark.getTimestamp();
        if (newWatermark <= maxWatermarkSoFar) {
            return;
        }

        maxWatermarkSoFar = newWatermark;
        watermarkEmitted.updateCurrentEffectiveWatermark(maxWatermarkSoFar);

        try {
            markActiveInternally();

            output.emitWatermark(
                    new org.apache.flink.streaming.api.watermark.Watermark(newWatermark));
        } catch (ExceptionInChainedOperatorException e) {
            throw e;
        } catch (Exception e) {
            throw new ExceptionInChainedOperatorException(e);
        }
    }

    @Override
    public void markIdle() {
        if (isIdle) {
            return;
        }

        try {
            output.emitWatermarkStatus(WatermarkStatus.IDLE);
            watermarkEmitted.updateIdle(true);
            isIdle = true;
        } catch (ExceptionInChainedOperatorException e) {
            throw e;
        } catch (Exception e) {
            throw new ExceptionInChainedOperatorException(e);
        }
    }

    @Override
    public void markActive() {
        try {
            markActiveInternally();
        } catch (ExceptionInChainedOperatorException e) {
            throw e;
        } catch (Exception e) {
            throw new ExceptionInChainedOperatorException(e);
        }
    }

    private boolean markActiveInternally() throws Exception {
        if (!isIdle) {
            return true;
        }

        output.emitWatermarkStatus(WatermarkStatus.ACTIVE);
        watermarkEmitted.updateIdle(false);
        isIdle = false;
        return false;
    }
}
