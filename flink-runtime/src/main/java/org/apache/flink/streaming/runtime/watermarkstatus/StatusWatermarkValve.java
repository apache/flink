/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.watermarkstatus;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionIndexSet;
import org.apache.flink.streaming.api.watermark.InternalWatermark;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput.DataOutput;
import org.apache.flink.streaming.runtime.io.checkpointing.CheckpointedInputGate;
import org.apache.flink.streaming.runtime.watermarkstatus.HeapPriorityQueue.HeapPriorityQueueElement;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@code StatusWatermarkValve} embodies the logic of how {@link Watermark} and {@link
 * WatermarkStatus} are propagated to downstream outputs, given a set of one or multiple
 * subpartitions that continuously receive them. Usages of this class need to define the number of
 * subpartitions that the valve needs to handle, as well as provide a implementation of {@link
 * DataOutput}, which is called by the valve only when it determines a new watermark or watermark
 * status can be propagated.
 */
@Internal
public class StatusWatermarkValve {

    // ------------------------------------------------------------------------
    //	Runtime state for watermark & watermark status output determination
    // ------------------------------------------------------------------------

    /**
     * The current status of all subpartitions. Changes as watermarks & watermark statuses are fed
     * into the valve.
     */
    private final List<Map<Integer, SubpartitionStatus>> subpartitionStatuses;

    /**
     * The index of the subpartition consumed by an input channel, if the channel consumes only one
     * subpartition.
     */
    private final int[] subpartitionIndexes;

    /** The last watermark emitted from the valve. */
    private long lastOutputWatermark;

    /** The last watermark status emitted from the valve. */
    private WatermarkStatus lastOutputWatermarkStatus;

    /** A heap-based priority queue to help find the minimum watermark. */
    private final HeapPriorityQueue<SubpartitionStatus> alignedSubpartitionStatuses;

    /** Whether there are multiple subpartitions transmitted through the same input channel. */
    private final boolean isInputChannelShared;

    /**
     * Returns a new {@code StatusWatermarkValve}.
     *
     * @param numInputChannels the number of input channels that this valve will need to handle
     */
    @VisibleForTesting
    public StatusWatermarkValve(int numInputChannels) {
        this(getIndexSets(numInputChannels));
    }

    private static ResultSubpartitionIndexSet[] getIndexSets(int numInputChannels) {
        ResultSubpartitionIndexSet[] subpartitionIndexRanges =
                new ResultSubpartitionIndexSet[numInputChannels];
        Arrays.fill(subpartitionIndexRanges, new ResultSubpartitionIndexSet(0));
        return subpartitionIndexRanges;
    }

    public StatusWatermarkValve(CheckpointedInputGate inputGate) {
        this(getIndexSets(inputGate));
    }

    private static ResultSubpartitionIndexSet[] getIndexSets(CheckpointedInputGate inputGate) {
        ResultSubpartitionIndexSet[] subpartitionIndexSets =
                new ResultSubpartitionIndexSet[inputGate.getNumberOfInputChannels()];
        for (int i = 0; i < inputGate.getNumberOfInputChannels(); i++) {
            subpartitionIndexSets[i] = inputGate.getChannel(i).getConsumedSubpartitionIndexSet();
        }
        return subpartitionIndexSets;
    }

    public StatusWatermarkValve(ResultSubpartitionIndexSet[] subpartitionIndexSets) {
        int numSubpartitions = 0;
        for (ResultSubpartitionIndexSet subpartitionIndexSet : subpartitionIndexSets) {
            numSubpartitions += subpartitionIndexSet.size();
        }
        this.alignedSubpartitionStatuses =
                new HeapPriorityQueue<>(
                        (left, right) -> Long.compare(left.watermark, right.watermark),
                        numSubpartitions);

        this.subpartitionStatuses = new ArrayList<>(subpartitionIndexSets.length);
        this.subpartitionIndexes = new int[subpartitionIndexSets.length];
        Arrays.fill(subpartitionIndexes, -1);
        for (ResultSubpartitionIndexSet subpartitionIndexSet : subpartitionIndexSets) {
            Map<Integer, SubpartitionStatus> map = new HashMap<>();
            for (int subpartitionId : subpartitionIndexSet.values()) {
                SubpartitionStatus subpartitionStatus = new SubpartitionStatus();
                subpartitionStatus.watermark = Long.MIN_VALUE;
                subpartitionStatus.watermarkStatus = WatermarkStatus.ACTIVE;
                markWatermarkAligned(subpartitionStatus);
                map.put(subpartitionId, subpartitionStatus);
            }
            if (subpartitionIndexSet.size() == 1) {
                subpartitionIndexes[subpartitionStatuses.size()] =
                        subpartitionIndexSet.values().iterator().next();
            }
            this.subpartitionStatuses.add(map);
        }

        this.lastOutputWatermark = Long.MIN_VALUE;
        this.lastOutputWatermarkStatus = WatermarkStatus.ACTIVE;

        this.isInputChannelShared =
                Arrays.stream(subpartitionIndexSets).anyMatch(x -> x.size() > 1);
    }

    /**
     * Feed a {@link Watermark} into the valve. If the input triggers the valve to output a new
     * Watermark, {@link DataOutput#emitWatermark(Watermark)} will be called to process the new
     * Watermark.
     *
     * @param watermark the watermark to feed to the valve
     * @param channelIndex the index of the channel that the fed watermark belongs to (index
     *     starting from 0)
     */
    public void inputWatermark(Watermark watermark, int channelIndex, DataOutput<?> output)
            throws Exception {
        final SubpartitionStatus subpartitionStatus;
        if (watermark instanceof InternalWatermark) {
            int subpartitionStatusIndex = ((InternalWatermark) watermark).getSubpartitionIndex();
            subpartitionStatus =
                    subpartitionStatuses.get(channelIndex).get(subpartitionStatusIndex);
        } else {
            subpartitionStatus =
                    subpartitionStatuses.get(channelIndex).get(subpartitionIndexes[channelIndex]);
        }

        // ignore the input watermark if its subpartition, or all subpartitions are idle (i.e.
        // overall the valve is idle).
        if (lastOutputWatermarkStatus.isActive() && subpartitionStatus.watermarkStatus.isActive()) {
            long watermarkMillis = watermark.getTimestamp();

            // if the input watermark's value is less than the last received watermark for its
            // subpartition, ignore it also.
            if (watermarkMillis > subpartitionStatus.watermark) {
                subpartitionStatus.watermark = watermarkMillis;

                if (subpartitionStatus.isWatermarkAligned) {
                    adjustAlignedSubpartitionStatuses(subpartitionStatus);
                } else if (watermarkMillis >= lastOutputWatermark) {
                    // previously unaligned subpartitions are now aligned if its watermark has
                    // caught up
                    markWatermarkAligned(subpartitionStatus);
                }

                // now, attempt to find a new min watermark across all aligned subpartitions
                findAndOutputNewMinWatermarkAcrossAlignedSubpartitions(output);
            }
        }
    }

    /**
     * Feed a {@link WatermarkStatus} into the valve. This may trigger the valve to output either a
     * new Watermark Status, for which {@link DataOutput#emitWatermarkStatus(WatermarkStatus)} will
     * be called, or a new Watermark, for which {@link DataOutput#emitWatermark(Watermark)} will be
     * called.
     *
     * @param watermarkStatus the watermark status to feed to the valve
     * @param channelIndex the index of the channel that the fed watermark status belongs to (index
     *     starting from 0)
     */
    public void inputWatermarkStatus(
            WatermarkStatus watermarkStatus, int channelIndex, DataOutput<?> output)
            throws Exception {
        // Shared input channel is only enabled in batch jobs, which do not have watermark status
        // events.
        Preconditions.checkState(!isInputChannelShared);
        SubpartitionStatus subpartitionStatus =
                subpartitionStatuses.get(channelIndex).get(subpartitionIndexes[channelIndex]);

        // It is supposed that WatermarkStatus will not appear in jobs where one input channel
        // consumes multiple subpartitions, so we do not need to map channelIndex into
        // subpartitionStatusIndex for now, like what is done on Watermarks.

        // only account for watermark status inputs that will result in a status change for the
        // subpartition
        if (watermarkStatus.isIdle() && subpartitionStatus.watermarkStatus.isActive()) {
            // handle active -> idle toggle for the subpartition
            subpartitionStatus.watermarkStatus = WatermarkStatus.IDLE;

            // the subpartition is now idle, therefore not aligned
            markWatermarkUnaligned(subpartitionStatus);

            // if all subpartitions of the valve are now idle, we need to output an idle stream
            // status from the valve (this also marks the valve as idle)
            if (!SubpartitionStatus.hasActiveSubpartitions(subpartitionStatuses)) {

                // now that all subpartitions are idle and no subpartitions will continue to advance
                // its
                // watermark,
                // we should "flush" all watermarks across all subpartitions; effectively, this
                // means
                // emitting
                // the max watermark across all subpartitions as the new watermark. Also, since we
                // already try to advance
                // the min watermark as subpartitions individually become IDLE, here we only need to
                // perform the flush
                // if the watermark of the last active subpartition that just became idle is the
                // current
                // min watermark.
                if (subpartitionStatus.watermark == lastOutputWatermark) {
                    findAndOutputMaxWatermarkAcrossAllSubpartitions(output);
                }

                lastOutputWatermarkStatus = WatermarkStatus.IDLE;
                output.emitWatermarkStatus(lastOutputWatermarkStatus);
            } else if (subpartitionStatus.watermark == lastOutputWatermark) {
                // if the watermark of the subpartition that just became idle equals the last output
                // watermark (the previous overall min watermark), we may be able to find a new
                // min watermark from the remaining aligned subpartitions
                findAndOutputNewMinWatermarkAcrossAlignedSubpartitions(output);
            }
        } else if (watermarkStatus.isActive() && subpartitionStatus.watermarkStatus.isIdle()) {
            // handle idle -> active toggle for the subpartition
            subpartitionStatus.watermarkStatus = WatermarkStatus.ACTIVE;

            // if the last watermark of the subpartition, before it was marked idle, is still
            // larger than
            // the overall last output watermark of the valve, then we can set the subpartition to
            // be
            // aligned already.
            if (subpartitionStatus.watermark >= lastOutputWatermark) {
                markWatermarkAligned(subpartitionStatus);
            }

            // if the valve was previously marked to be idle, mark it as active and output an active
            // stream
            // status because at least one of the subpartitions is now active
            if (lastOutputWatermarkStatus.isIdle()) {
                lastOutputWatermarkStatus = WatermarkStatus.ACTIVE;
                output.emitWatermarkStatus(lastOutputWatermarkStatus);
            }
        }
    }

    private void findAndOutputNewMinWatermarkAcrossAlignedSubpartitions(DataOutput<?> output)
            throws Exception {
        boolean hasAlignedSubpartitions = !alignedSubpartitionStatuses.isEmpty();

        // we acknowledge and output the new overall watermark if it really is aggregated
        // from some remaining aligned subpartition, and is also larger than the last output
        // watermark
        if (hasAlignedSubpartitions
                && alignedSubpartitionStatuses.peek().watermark > lastOutputWatermark) {
            lastOutputWatermark = alignedSubpartitionStatuses.peek().watermark;
            output.emitWatermark(new Watermark(lastOutputWatermark));
        }
    }

    /**
     * Mark the {@link SubpartitionStatus} as watermark-aligned and add it to the {@link
     * #alignedSubpartitionStatuses}.
     *
     * @param subpartitionStatus the subpartition status to be marked
     */
    private void markWatermarkAligned(SubpartitionStatus subpartitionStatus) {
        if (!subpartitionStatus.isWatermarkAligned) {
            subpartitionStatus.isWatermarkAligned = true;
            subpartitionStatus.addTo(alignedSubpartitionStatuses);
        }
    }

    /**
     * Mark the {@link SubpartitionStatus} as watermark-unaligned and remove it from the {@link
     * #alignedSubpartitionStatuses}.
     *
     * @param subpartitionStatus the subpartition status to be marked
     */
    private void markWatermarkUnaligned(SubpartitionStatus subpartitionStatus) {
        if (subpartitionStatus.isWatermarkAligned) {
            subpartitionStatus.isWatermarkAligned = false;
            subpartitionStatus.removeFrom(alignedSubpartitionStatuses);
        }
    }

    /**
     * Adjust the {@link #alignedSubpartitionStatuses} when an element({@link SubpartitionStatus})
     * in it was modified. The {@link #alignedSubpartitionStatuses} is a priority queue, when an
     * element in it was modified, we need to adjust the element's position to ensure its priority
     * order.
     *
     * @param subpartitionStatus the modified subpartition status
     */
    private void adjustAlignedSubpartitionStatuses(SubpartitionStatus subpartitionStatus) {
        alignedSubpartitionStatuses.adjustModifiedElement(subpartitionStatus);
    }

    private void findAndOutputMaxWatermarkAcrossAllSubpartitions(DataOutput<?> output)
            throws Exception {
        long maxWatermark = Long.MIN_VALUE;

        for (Map<Integer, SubpartitionStatus> map : subpartitionStatuses) {
            for (SubpartitionStatus subpartitionStatus : map.values()) {
                maxWatermark = Math.max(subpartitionStatus.watermark, maxWatermark);
            }
        }

        if (maxWatermark > lastOutputWatermark) {
            lastOutputWatermark = maxWatermark;
            output.emitWatermark(new Watermark(lastOutputWatermark));
        }
    }

    /**
     * An {@code SubpartitionStatus} keeps track of a subpartition's last watermark, stream status,
     * and whether or not the subpartition's current watermark is aligned with the overall watermark
     * output from the valve.
     *
     * <p>There are 2 situations where a subpartition's watermark is not considered aligned:
     *
     * <ul>
     *   <li>the current watermark status of the subpartition is idle
     *   <li>the watermark status has resumed to be active, but the watermark of the subpartition
     *       hasn't caught up to the last output watermark from the valve yet.
     * </ul>
     *
     * <p>NOTE: This class implements {@link HeapPriorityQueueElement} to be managed by {@link
     * #alignedSubpartitionStatuses} to help find minimum watermark.
     */
    @VisibleForTesting
    protected static class SubpartitionStatus implements HeapPriorityQueueElement {
        protected long watermark;
        protected WatermarkStatus watermarkStatus;
        protected boolean isWatermarkAligned;

        /**
         * This field holds the current physical index of this subpartition status when it is
         * managed by a {@link HeapPriorityQueue}.
         */
        private int heapIndex = HeapPriorityQueueElement.NOT_CONTAINED;

        /**
         * Utility to check if at least one subpartition in a given array of subpartitions is
         * active.
         */
        private static boolean hasActiveSubpartitions(
                List<Map<Integer, SubpartitionStatus>> subpartitionStatuses) {
            for (Map<Integer, SubpartitionStatus> map : subpartitionStatuses) {
                for (SubpartitionStatus status : map.values()) {
                    if (status.watermarkStatus.isActive()) {
                        return true;
                    }
                }
            }
            return false;
        }

        @Override
        public int getInternalIndex() {
            return heapIndex;
        }

        @Override
        public void setInternalIndex(int newIndex) {
            this.heapIndex = newIndex;
        }

        private void removeFrom(HeapPriorityQueue<SubpartitionStatus> queue) {
            checkState(heapIndex != HeapPriorityQueueElement.NOT_CONTAINED);
            queue.remove(this);
            setInternalIndex(HeapPriorityQueueElement.NOT_CONTAINED);
        }

        private void addTo(HeapPriorityQueue<SubpartitionStatus> queue) {
            checkState(heapIndex == HeapPriorityQueueElement.NOT_CONTAINED);
            queue.add(this);
        }
    }

    @VisibleForTesting
    protected SubpartitionStatus getSubpartitionStatus(int subpartitionIndex) {
        for (Map<Integer, SubpartitionStatus> map : subpartitionStatuses) {
            Preconditions.checkState(
                    map.size() == 1,
                    "Cannot trigger this method when an input channel consumes multiple subpartition.");
        }

        Preconditions.checkArgument(
                subpartitionIndex >= 0 && subpartitionIndex < subpartitionStatuses.size(),
                "Invalid subpartition index. Number of subpartitions: "
                        + subpartitionStatuses.size());

        return subpartitionStatuses.get(subpartitionIndex).get(0);
    }
}
