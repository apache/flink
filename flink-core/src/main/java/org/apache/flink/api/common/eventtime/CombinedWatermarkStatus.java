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

package org.apache.flink.api.common.eventtime;

import org.apache.flink.annotation.Internal;

import java.util.ArrayList;
import java.util.List;

/**
 * A {@link CombinedWatermarkStatus} combines the watermark (and idleness) updates of multiple
 * partitions/shards/splits into one combined watermark.
 */
@Internal
final class CombinedWatermarkStatus {

    /** List of all watermark outputs, for efficient access. */
    private final List<PartialWatermark> partialWatermarks = new ArrayList<>();

    /** The combined watermark over the per-output watermarks. */
    private long combinedWatermark = Long.MIN_VALUE;

    private boolean idle = false;

    public long getCombinedWatermark() {
        return combinedWatermark;
    }

    public boolean isIdle() {
        return idle;
    }

    public boolean remove(PartialWatermark o) {
        return partialWatermarks.remove(o);
    }

    public void add(PartialWatermark element) {
        partialWatermarks.add(element);
    }

    /**
     * Checks whether we need to update the combined watermark. It can update {@link #isIdle()}
     * status.
     *
     * <p><b>NOTE:</b>The logic here should be kept in sync with {@code StatusWatermarkValve}.
     *
     * @return true, if the combined watermark changed
     */
    public boolean updateCombinedWatermark() {
        // if we don't have any outputs, we should not emit
        if (partialWatermarks.isEmpty()) {
            return false;
        }

        long maximumOverAllOutputs = Long.MIN_VALUE;
        long minimumOverAllActiveOutputs = Long.MAX_VALUE;

        boolean allIdle = true;
        for (PartialWatermark partialWatermark : partialWatermarks) {
            final long watermark = partialWatermark.getWatermark();
            maximumOverAllOutputs = Math.max(maximumOverAllOutputs, watermark);
            if (!partialWatermark.isIdle()) {
                minimumOverAllActiveOutputs = Math.min(minimumOverAllActiveOutputs, watermark);
                allIdle = false;
            }
        }

        this.idle = allIdle;

        final long combinedWatermark;
        if (allIdle) {
            // If all splits are idle, we should flush all watermarks, which effectively
            // means emitting the maximum watermark over all outputs.
            // Otherwise, there could be a race condition between splits when idleness is triggered.
            // E.g., split 1 of 2 emits 5 and goes into idle, split 2 of 2 emits 4 and goes into
            // idle. If split 2 is idle first, watermark 5 wins. If split 1 is idle first, watermark
            // 4 wins. But if both are idle, we should conclude on 5.
            combinedWatermark = maximumOverAllOutputs;
        } else {
            // Active splits should determine the progression of the watermark. Therefore, the
            // minimum watermark across all active splits takes precedence over that of idle splits.
            combinedWatermark = minimumOverAllActiveOutputs;
        }

        if (combinedWatermark > this.combinedWatermark) {
            this.combinedWatermark = combinedWatermark;
            return true;
        }

        return false;
    }

    /** Per-output watermark state. */
    static class PartialWatermark {
        private long watermark = Long.MIN_VALUE;
        private boolean idle = false;
        private final WatermarkOutputMultiplexer.WatermarkUpdateListener onWatermarkUpdate;

        public PartialWatermark(
                WatermarkOutputMultiplexer.WatermarkUpdateListener onWatermarkUpdate) {
            this.onWatermarkUpdate = onWatermarkUpdate;
        }

        /** Returns the current watermark timestamp. */
        private long getWatermark() {
            return watermark;
        }

        /**
         * Returns true if the watermark was advanced, that is if the new watermark is larger than
         * the previous one.
         *
         * <p>Setting a watermark will clear the idleness flag.
         */
        public boolean setWatermark(long watermark) {
            setIdle(false);
            final boolean updated = watermark > this.watermark;
            if (updated) {
                this.onWatermarkUpdate.onWatermarkUpdate(watermark);
                this.watermark = Math.max(watermark, this.watermark);
            }
            return updated;
        }

        private boolean isIdle() {
            return idle;
        }

        public void setIdle(boolean idle) {
            this.idle = idle;
            this.onWatermarkUpdate.onIdleUpdate(idle);
        }
    }
}
