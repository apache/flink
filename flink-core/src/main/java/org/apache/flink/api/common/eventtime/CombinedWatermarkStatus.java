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

import static org.apache.flink.util.Preconditions.checkState;

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
     * Checks whether we need to update the combined watermark.
     *
     * <p><b>NOTE:</b>It can update {@link #isIdle()} status.
     *
     * @return true, if the combined watermark changed
     */
    public boolean updateCombinedWatermark() {
        long minimumOverAllOutputs = Long.MAX_VALUE;

        // if we don't have any outputs minimumOverAllOutputs is not valid, it's still
        // at its initial Long.MAX_VALUE state and we must not emit that
        if (partialWatermarks.isEmpty()) {
            return false;
        }

        boolean allIdle = true;
        for (PartialWatermark partialWatermark : partialWatermarks) {
            if (!partialWatermark.isIdle()) {
                minimumOverAllOutputs =
                        Math.min(minimumOverAllOutputs, partialWatermark.getWatermark());
                allIdle = false;
            }
        }

        this.idle = allIdle;

        if (!allIdle && minimumOverAllOutputs > combinedWatermark) {
            combinedWatermark = minimumOverAllOutputs;
            return true;
        }

        return false;
    }

    /** Per-output watermark state. */
    static class PartialWatermark {
        private long watermark = Long.MIN_VALUE;
        private boolean idle = false;

        /**
         * Returns the current watermark timestamp. This will throw {@link IllegalStateException} if
         * the output is currently idle.
         */
        private long getWatermark() {
            checkState(!idle, "Output is idle.");
            return watermark;
        }

        /**
         * Returns true if the watermark was advanced, that is if the new watermark is larger than
         * the previous one.
         *
         * <p>Setting a watermark will clear the idleness flag.
         */
        public boolean setWatermark(long watermark) {
            this.idle = false;
            final boolean updated = watermark > this.watermark;
            this.watermark = Math.max(watermark, this.watermark);
            return updated;
        }

        private boolean isIdle() {
            return idle;
        }

        public void setIdle(boolean idle) {
            this.idle = idle;
        }
    }
}
