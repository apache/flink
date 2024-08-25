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

import org.apache.flink.api.watermark.BoolWatermark;
import org.apache.flink.api.watermark.GeneralizedWatermark;
import org.apache.flink.api.watermark.WatermarkSpecs;

import java.util.HashMap;
import java.util.Map;

public class BoolWatermarkCombiner implements InternalWatermarkDeclaration.WatermarkCombiner {
    private static final long serialVersionUID = 1L;

    private final Map<Integer, Boolean> channelWatermarks = new HashMap<>();
    private final WatermarkSpecs.BoolWatermarkComparison comparison;

    // Constructor to choose whether to use OR or AND operation
    public BoolWatermarkCombiner(WatermarkSpecs.BoolWatermarkComparison comparison) {
        this.comparison = comparison;
    }

    @Override
    public void combineWatermark(
            GeneralizedWatermark watermark,
            InternalWatermarkDeclaration.WatermarkCombiner.Context context,
            WatermarkOutput output)
            throws Exception {
        assert (watermark instanceof BoolWatermark);

        // Get the current channel index and number of channels
        int currentChannelIndex = context.getIndexOfCurrentChannel();
        int numberOfInputChannels = context.getNumberOfInputChannels();

        // Update the watermark for the current channel
        channelWatermarks.put(currentChannelIndex, ((BoolWatermark) watermark).getValue());
        boolean useOr = comparison == WatermarkSpecs.BoolWatermarkComparison.OR;
        // Check if we have received watermarks from all channels
        if (channelWatermarks.size() == numberOfInputChannels) {
            // Calculate the combined watermark based on the useOr flag
            boolean combinedWatermark = !useOr;
            for (boolean wm : channelWatermarks.values()) {
                combinedWatermark = useOr ? (combinedWatermark || wm) : (combinedWatermark && wm);
            }

            // Emit the combined watermark
            output.emitWatermark(new BoolWatermark(combinedWatermark, watermark.getIdentifier()));
        }
    }
}
