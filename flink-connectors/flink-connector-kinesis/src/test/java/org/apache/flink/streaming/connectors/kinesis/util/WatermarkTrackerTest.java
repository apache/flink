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

package org.apache.flink.streaming.connectors.kinesis.util;

import org.apache.flink.streaming.util.MockStreamingRuntimeContext;

import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link WatermarkTracker}. */
public class WatermarkTrackerTest {

    WatermarkTracker.WatermarkState wm1 = new WatermarkTracker.WatermarkState();
    MutableLong clock = new MutableLong(0);

    private class TestWatermarkTracker extends WatermarkTracker {
        /** The watermarks of all sub tasks that participate in the synchronization. */
        private final Map<String, WatermarkState> watermarks = new HashMap<>();

        private long updateTimeoutCount = 0;

        @Override
        protected long getCurrentTime() {
            return clock.longValue();
        }

        @Override
        public long updateWatermark(final long localWatermark) {
            refreshWatermarkSnapshot(this.watermarks);

            long currentTime = getCurrentTime();
            String subtaskId = this.getSubtaskId();

            WatermarkState ws = watermarks.get(subtaskId);
            if (ws == null) {
                watermarks.put(subtaskId, ws = new WatermarkState());
            }
            ws.lastUpdated = currentTime;
            ws.watermark = Math.max(ws.watermark, localWatermark);
            saveWatermark(subtaskId, ws);

            long globalWatermark = ws.watermark;
            for (Map.Entry<String, WatermarkState> e : watermarks.entrySet()) {
                ws = e.getValue();
                if (ws.lastUpdated + getUpdateTimeoutMillis() < currentTime) {
                    // ignore outdated subtask
                    updateTimeoutCount++;
                    continue;
                }
                globalWatermark = Math.min(ws.watermark, globalWatermark);
            }
            return globalWatermark;
        }

        protected void refreshWatermarkSnapshot(Map<String, WatermarkState> watermarks) {
            watermarks.put("wm1", wm1);
        }

        protected void saveWatermark(String id, WatermarkState ws) {
            // do nothing
        }

        public long getUpdateTimeoutCount() {
            return updateTimeoutCount;
        }
    }

    @Test
    public void test() {
        long watermark = 0;
        TestWatermarkTracker ws = new TestWatermarkTracker();
        ws.open(new MockStreamingRuntimeContext(false, 1, 0));
        assertThat(ws.updateWatermark(Long.MIN_VALUE)).isEqualTo(Long.MIN_VALUE);
        assertThat(ws.updateWatermark(watermark)).isEqualTo(Long.MIN_VALUE);
        // timeout wm1
        clock.add(WatermarkTracker.DEFAULT_UPDATE_TIMEOUT_MILLIS + 1);
        assertThat(ws.updateWatermark(watermark)).isEqualTo(watermark);
        assertThat(ws.updateWatermark(watermark - 1)).isEqualTo(watermark);

        // min watermark
        wm1.watermark = watermark + 1;
        wm1.lastUpdated = clock.longValue();
        assertThat(ws.updateWatermark(watermark)).isEqualTo(watermark);
        assertThat(ws.updateWatermark(watermark + 1)).isEqualTo(watermark + 1);
    }
}
