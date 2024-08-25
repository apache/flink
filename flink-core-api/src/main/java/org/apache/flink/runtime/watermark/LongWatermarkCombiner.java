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

package org.apache.flink.runtime.watermark;

import org.apache.flink.api.watermark.GeneralizedWatermark;
import org.apache.flink.api.watermark.LongWatermark;
import org.apache.flink.api.watermark.WatermarkSpecs;

import java.util.HashMap;
import java.util.Map;

public class LongWatermarkCombiner implements InternalWatermarkDeclaration.WatermarkCombiner {
    private final Map<Integer, Long> channelWatermarks = new HashMap<>();
    private final WatermarkSpecs.NumericWatermarkComparison comparison;

    // Constructor to choose whether to use MAX or MIN operation
    public LongWatermarkCombiner(WatermarkSpecs.NumericWatermarkComparison comparison) {
        this.comparison = comparison;
    }

    @Override
    public void combineWatermark(
            GeneralizedWatermark watermark,
            InternalWatermarkDeclaration.WatermarkCombiner.Context context,
            WatermarkOutput output)
            throws Exception {
        assert (watermark instanceof LongWatermark);

        // Get the current channel index and number of channels
        int currentChannelIndex = context.getIndexOfCurrentChannel();
        int numberOfInputChannels = context.getNumberOfInputChannels();

        // Update the watermark for the current channel
        channelWatermarks.put(currentChannelIndex, ((LongWatermark) watermark).getValue());
        boolean useMax = comparison == WatermarkSpecs.NumericWatermarkComparison.MAX;

        // Check if we have received watermarks from all channels
        if (channelWatermarks.size() == numberOfInputChannels) {
            // Calculate the combined watermark based on the useMax flag
            long combinedWatermark = useMax ? Long.MIN_VALUE : Long.MAX_VALUE;
            for (long wm : channelWatermarks.values()) {
                combinedWatermark =
                        useMax ? Math.max(combinedWatermark, wm) : Math.min(combinedWatermark, wm);
            }

            // Emit the combined watermark
            output.emitWatermark(new LongWatermark(combinedWatermark, watermark.getIdentifier()));
        }
    }
}
