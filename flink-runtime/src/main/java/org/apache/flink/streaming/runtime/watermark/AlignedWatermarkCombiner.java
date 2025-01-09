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

package org.apache.flink.streaming.runtime.watermark;

import org.apache.flink.api.common.watermark.Watermark;

import java.util.BitSet;
import java.util.function.Consumer;

/**
 * A {@link WatermarkCombiner} is design to align {@link Watermark}s. It will combine watermarks
 * after receiving watermarks from all upstream input channels.
 *
 * <p>The combine process will perform the following three steps: (1) send the combined watermark
 * (actually the last received watermark) to downstream, (2) clear the received watermarks, (3)
 * resume input gate.
 *
 * <p>Note that the aligned Watermarks should have the same value within a batch.
 */
public class AlignedWatermarkCombiner implements WatermarkCombiner {
    /** The number of upstream input channels. */
    private int numberOfInputChannels;

    /** A bitset to record whether the watermark has been received from each channel. */
    private final BitSet hasReceiveWatermarks;

    /** The input gate resume callback. */
    private Runnable gateResumer;

    public AlignedWatermarkCombiner(int numberOfInputChannels, Runnable gateResumer) {
        this.numberOfInputChannels = numberOfInputChannels;
        this.hasReceiveWatermarks = new BitSet(numberOfInputChannels);
        this.gateResumer = gateResumer;
    }

    @Override
    public void combineWatermark(
            Watermark watermark, int channelIndex, Consumer<Watermark> watermarkEmitter)
            throws Exception {

        // mark the channel has received the watermark
        hasReceiveWatermarks.set(channelIndex);

        // once receive all watermarks, perform combine process
        if (hasReceiveWatermarks.cardinality() == numberOfInputChannels) {
            // send the combined watermark to downstream
            watermarkEmitter.accept(watermark);
            // clear the received watermarks
            hasReceiveWatermarks.clear();
            // resume input gate
            gateResumer.run();
        }
    }
}
