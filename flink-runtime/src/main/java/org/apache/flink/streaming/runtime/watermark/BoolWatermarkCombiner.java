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

import org.apache.flink.api.common.watermark.BoolWatermark;
import org.apache.flink.api.common.watermark.Watermark;
import org.apache.flink.api.common.watermark.WatermarkCombinationFunction;
import org.apache.flink.api.common.watermark.WatermarkCombinationPolicy;

import java.util.BitSet;
import java.util.function.Consumer;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link WatermarkCombiner} for unaligned {@link BoolWatermark}s.
 *
 * <p>The combination process will perform the following steps: (1) determine if it satisfies the
 * condition of {@link WatermarkCombinationPolicy#isCombineWaitForAllChannels()}, (2) calculate the
 * combined watermark value, (3) send the combined watermark to downstream only when the value
 * differs from the previous sent one or if it's the first time sending.
 */
public class BoolWatermarkCombiner implements WatermarkCombiner {

    private final WatermarkCombinationPolicy combinationPolicy;

    /** The number of upstream input channels. */
    private final int numberOfInputChannels;

    /** A bitset to record whether the watermark has been received from each channel. */
    private final BitSet hasReceiveWatermarks;

    /** The received watermark value for each channel. */
    private final BitSet watermarkValues;

    /**
     * Send only the watermark that differs from the previous sent one, we need to record the
     * previous sent watermark value and whether is the first time sending.
     */
    private boolean previousEmitWatermarkValue = false;

    private boolean isFirstTimeEmit = true;

    public BoolWatermarkCombiner(
            WatermarkCombinationPolicy combinationPolicy, int numberOfInputChannels) {
        checkState(
                combinationPolicy.getWatermarkCombinationFunction()
                        instanceof WatermarkCombinationFunction.BoolWatermarkCombinationFunction);
        this.combinationPolicy = combinationPolicy;
        this.numberOfInputChannels = numberOfInputChannels;
        this.hasReceiveWatermarks = new BitSet(numberOfInputChannels);
        this.watermarkValues = new BitSet(numberOfInputChannels);

        if (combinationPolicy.getWatermarkCombinationFunction()
                == WatermarkCombinationFunction.BoolWatermarkCombinationFunction.OR) {
            // Initialize the watermarkValues to all false when using OR strategy
            watermarkValues.clear(0, numberOfInputChannels);
        } else if (combinationPolicy.getWatermarkCombinationFunction()
                == WatermarkCombinationFunction.BoolWatermarkCombinationFunction.AND) {
            // Initialize the watermarkValues to all true when using AND strategy
            watermarkValues.set(0, numberOfInputChannels);
        } else {
            throw new IllegalArgumentException(
                    "Illegal WatermarkCombinationFunction for BoolWatermarkCombiner: "
                            + combinationPolicy.getWatermarkCombinationFunction());
        }
    }

    @Override
    public void combineWatermark(
            Watermark watermark, int channelIndex, Consumer<Watermark> watermarkEmitter)
            throws Exception {
        checkState(watermark instanceof BoolWatermark);

        hasReceiveWatermarks.set(channelIndex);
        // Update the watermark for the current channel
        watermarkValues.set(channelIndex, ((BoolWatermark) watermark).getValue());

        if (combinationPolicy.isCombineWaitForAllChannels()
                && hasReceiveWatermarks.cardinality() < numberOfInputChannels) {
            // Not all watermarks have been received yet
            return;
        }

        // calculate the combined watermark value
        boolean combinedWatermarkValue;
        if (combinationPolicy.getWatermarkCombinationFunction()
                == WatermarkCombinationFunction.BoolWatermarkCombinationFunction.OR) {
            combinedWatermarkValue = watermarkValues.cardinality() > 0;
        } else {
            combinedWatermarkValue = watermarkValues.cardinality() == numberOfInputChannels;
        }

        if (shouldEmitWatermark(combinedWatermarkValue)) {
            // send the combined watermark to downstream
            watermarkEmitter.accept(
                    new BoolWatermark(combinedWatermarkValue, watermark.getIdentifier()));
            previousEmitWatermarkValue = combinedWatermarkValue;
        }
    }

    private boolean shouldEmitWatermark(boolean combinedWatermarkValue) {
        if (isFirstTimeEmit) {
            isFirstTimeEmit = false;
            return true;
        }
        return combinedWatermarkValue != previousEmitWatermarkValue;
    }
}
