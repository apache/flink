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

import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Timeout;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.hamcrest.MatcherAssert;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;

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
        Assertions.assertEquals(Long.MIN_VALUE, ws.updateWatermark(Long.MIN_VALUE));
        Assertions.assertEquals(Long.MIN_VALUE, ws.updateWatermark(watermark));
        // timeout wm1
        clock.add(WatermarkTracker.DEFAULT_UPDATE_TIMEOUT_MILLIS + 1);
        Assertions.assertEquals(watermark, ws.updateWatermark(watermark));
        Assertions.assertEquals(watermark, ws.updateWatermark(watermark - 1));

        // min watermark
        wm1.watermark = watermark + 1;
        wm1.lastUpdated = clock.longValue();
        Assertions.assertEquals(watermark, ws.updateWatermark(watermark));
        Assertions.assertEquals(watermark + 1, ws.updateWatermark(watermark + 1));
    }
}
