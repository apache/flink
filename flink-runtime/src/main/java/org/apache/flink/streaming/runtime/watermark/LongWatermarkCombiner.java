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

import org.apache.flink.api.common.watermark.LongWatermark;
import org.apache.flink.api.common.watermark.Watermark;
import org.apache.flink.api.common.watermark.WatermarkCombinationFunction;
import org.apache.flink.api.common.watermark.WatermarkCombinationPolicy;
import org.apache.flink.streaming.runtime.watermarkstatus.HeapPriorityQueue;
import org.apache.flink.streaming.runtime.watermarkstatus.HeapPriorityQueue.HeapPriorityQueueElement;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link WatermarkCombiner} for unaligned {@link LongWatermark}s.
 *
 * <p>The combination process will perform the following steps: (1) determine if it satisfies the
 * condition of {@link WatermarkCombinationPolicy#isCombineWaitForAllChannels()}, (2) calculate the
 * combined watermark value, (3) send the combined watermark to downstream only when the value
 * differs from the previous sent one or if it's the first time sending.
 */
public class LongWatermarkCombiner implements WatermarkCombiner {

    private final WatermarkCombinationPolicy combinationPolicy;

    /** The number of upstream input channels. */
    private final int numberOfInputChannels;

    /** A bitset to record whether the watermark has been received from each channel. */
    private final BitSet hasReceiveWatermarks;

    /** Channel index to {@link LongWatermarkElement}. */
    private final Map<Integer, LongWatermarkElement> channelWatermarks;

    /** A heap-based priority queue to help find the minimum/maximum watermark. */
    private final HeapPriorityQueue<LongWatermarkElement> orderedChannelWatermarks;

    /** The comparator to compare the watermark value of two {@link LongWatermarkElement}s. */
    private final HeapPriorityQueue.PriorityComparator<LongWatermarkElement> watermarkComparator;

    /**
     * Send only the watermark that differs from the previous sent one, we need to record the
     * previous sent watermark value and whether is the first time sending.
     */
    private final LongWatermarkElement previousEmitWatermarkElement = new LongWatermarkElement(-1);

    private boolean isFirstTimeEmit = true;

    public LongWatermarkCombiner(
            WatermarkCombinationPolicy combinationPolicy, int numberOfInputChannels) {
        checkState(
                combinationPolicy.getWatermarkCombinationFunction()
                        instanceof
                        WatermarkCombinationFunction.NumericWatermarkCombinationFunction);
        this.combinationPolicy = combinationPolicy;
        this.numberOfInputChannels = numberOfInputChannels;
        this.hasReceiveWatermarks = new BitSet(numberOfInputChannels);
        this.channelWatermarks = new HashMap<>(numberOfInputChannels);

        // according to the combination strategy create the comparator and the {@link
        // LongWatermarkElement} initial value
        long initValue;
        if (combinationPolicy.getWatermarkCombinationFunction()
                == WatermarkCombinationFunction.NumericWatermarkCombinationFunction.MIN) {
            this.watermarkComparator =
                    (left, right) ->
                            Long.compare(left.getWatermarkValue(), right.getWatermarkValue());
            initValue = Long.MAX_VALUE;
        } else {
            this.watermarkComparator =
                    (left, right) ->
                            Long.compare(right.getWatermarkValue(), left.getWatermarkValue());
            initValue = Long.MIN_VALUE;
        }

        // init the watermark elements of {@code #channelWatermarks} and {@code
        // #orderedChannelWatermarks}
        this.orderedChannelWatermarks =
                new HeapPriorityQueue<>(watermarkComparator, numberOfInputChannels);
        for (int i = 0; i < numberOfInputChannels; i++) {
            LongWatermarkElement watermarkElement = new LongWatermarkElement(initValue);
            channelWatermarks.put(i, watermarkElement);
            orderedChannelWatermarks.add(watermarkElement);
        }
    }

    @Override
    public void combineWatermark(
            Watermark watermark, int channelIndex, Consumer<Watermark> watermarkEmitter) {
        checkState(watermark instanceof LongWatermark);

        hasReceiveWatermarks.set(channelIndex);
        // Update the watermark for the current channel
        channelWatermarks
                .get(channelIndex)
                .setWatermarkValue(((LongWatermark) watermark).getValue());
        orderedChannelWatermarks.adjustModifiedElement(channelWatermarks.get(channelIndex));

        if (combinationPolicy.isCombineWaitForAllChannels()
                && hasReceiveWatermarks.cardinality() < numberOfInputChannels) {
            // Not all watermarks have been received yet
            return;
        }

        // the combined watermark should be the first node of {@code orderedChannelWatermarks}

        if (shouldEmitWatermark(orderedChannelWatermarks.peek())) {
            // send the combined watermark to downstream
            watermarkEmitter.accept(
                    new LongWatermark(
                            orderedChannelWatermarks.peek().getWatermarkValue(),
                            watermark.getIdentifier()));
            previousEmitWatermarkElement.setWatermarkValue(
                    orderedChannelWatermarks.peek().getWatermarkValue());
        }
    }

    private boolean shouldEmitWatermark(LongWatermarkElement combinedWatermarkElement) {
        if (isFirstTimeEmit) {
            isFirstTimeEmit = false;
            return true;
        }
        return watermarkComparator.comparePriority(
                        combinedWatermarkElement, previousEmitWatermarkElement)
                != 0;
    }

    /**
     * This class implements {@link HeapPriorityQueueElement} to help find the maximum/minimum
     * {@link LongWatermark}.
     */
    protected static class LongWatermarkElement implements HeapPriorityQueueElement {
        private long watermarkValue;

        /**
         * This field holds the current physical index of this watermark when it is managed by a
         * {@link HeapPriorityQueue}.
         */
        private int heapIndex = HeapPriorityQueueElement.NOT_CONTAINED;

        public LongWatermarkElement(long watermarkValue) {
            this.watermarkValue = watermarkValue;
        }

        @Override
        public int getInternalIndex() {
            return heapIndex;
        }

        @Override
        public void setInternalIndex(int newIndex) {
            this.heapIndex = newIndex;
        }

        public void setWatermarkValue(long watermarkValue) {
            this.watermarkValue = watermarkValue;
        }

        public long getWatermarkValue() {
            return watermarkValue;
        }
    }
}
