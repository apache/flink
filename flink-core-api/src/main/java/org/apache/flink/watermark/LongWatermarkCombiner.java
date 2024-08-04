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

package org.apache.flink.watermark;

import org.apache.flink.api.common.WatermarkOutput;
import org.apache.flink.api.common.eventtime.Watermark;

import java.util.HashMap;
import java.util.Map;

public class LongWatermarkCombiner implements InternalWatermarkDeclaration.WatermarkCombiner {
    private Map<Integer, Long> channelWatermarks = new HashMap<>();

    @Override
    public void combineWatermark(
            Watermark watermark,
            InternalWatermarkDeclaration.WatermarkCombiner.Context context,
            WatermarkOutput output)
            throws Exception {
        assert (watermark instanceof LongWatermark);

        // Get the current channel index and number of channels
        int currentChannelIndex = context.getIndexOfCurrentChannel();
        int numberOfInputChannels = context.getNumberOfInputChannels();

        // Update the watermark for the current channel
        channelWatermarks.put(currentChannelIndex, ((LongWatermark) watermark).getValue());

        // Check if we have received watermarks from all channels
        if (channelWatermarks.size() == numberOfInputChannels) {
            // Find the minimum watermark
            long minWatermark = Long.MAX_VALUE;
            for (long wm : channelWatermarks.values()) {
                minWatermark = Math.min(minWatermark, wm);
            }

            // Emit the minimum watermark
            output.emitWatermark(
                    new LongWatermark(
                            minWatermark, ((LongWatermark) watermark).getWatermarkIdentifier()));
        }
    }
}
