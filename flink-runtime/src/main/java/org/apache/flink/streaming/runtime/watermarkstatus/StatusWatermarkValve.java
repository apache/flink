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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final Logger LOG = LoggerFactory.getLogger(StatusWatermarkValve.class);

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
        final int subpartitionIndex;
        if (watermark instanceof InternalWatermark) {
            subpartitionIndex = ((InternalWatermark) watermark).getSubpartitionIndex();
            subpartitionStatus = subpartitionStatuses.get(channelIndex).get(subpartitionIndex);
        } else {
            subpartitionIndex = subpartitionIndexes[channelIndex];
            subpartitionStatus = subpartitionStatuses.get(channelIndex).get(subpartitionIndex);
        }

        WatermarkStatus currentSubpartitionStatus = subpartitionStatus.watermarkStatus;

        // FINISHED subpartitions can only accept Long.MAX_VALUE from upstream to preserve message
        // ordering
        if (currentSubpartitionStatus.isFinished()) {
            if (watermark.getTimestamp() == Long.MAX_VALUE) {
                subpartitionStatus.watermark = Long.MAX_VALUE;
                tryEmitNewWatermark(output);
            } else {
                // Ignore non-MAX_VALUE watermarks
                LOG.error(
                        "Channel {} subpartition {} in FINISHED state received a non-MAX watermark ({})."
                                + " Ignoring it - only MAX_WATERMARK is expected.",
                        channelIndex,
                        subpartitionIndex,
                        watermark.getTimestamp());
            }
        } else if (currentSubpartitionStatus.isActive()) {
            long watermarkMillis = watermark.getTimestamp();

            // if the input watermark's value is less than the last received watermark for its
            // subpartition, ignore it.
            if (watermarkMillis > subpartitionStatus.watermark) {
                subpartitionStatus.watermark = watermarkMillis;

                if (subpartitionStatus.isWatermarkAligned) {
                    adjustAlignedSubpartitionStatuses(subpartitionStatus);
                } else if (watermarkMillis >= lastOutputWatermark) {
                    // previously unaligned subpartitions are now aligned if its watermark has
                    // caught up
                    markWatermarkAligned(subpartitionStatus);
                }

                tryEmitNewWatermark(output);
            }
        } else if (currentSubpartitionStatus.isIdle()) {
            // Ignore watermark if subpartition is IDLE
            LOG.debug(
                    "Channel {} subpartition {} is IDLE. Ignoring received watermark ({}).",
                    channelIndex,
                    subpartitionIndex,
                    watermark.getTimestamp());
        } else {
            throw new IllegalStateException(
                    String.format(
                            "Unknown watermark status for channel %d subpartition %d: %s",
                            channelIndex, subpartitionIndex, currentSubpartitionStatus));
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

        WatermarkStatus currentSubpartitionStatus = subpartitionStatus.watermarkStatus;

        // Ignore if no change
        if (watermarkStatus.equals(currentSubpartitionStatus)) {
            return;
        }

        // Handle all valid status transitions
        if (currentSubpartitionStatus.isActive()) {
            // Check if this subpartition contributes to current output watermark
            boolean shouldUpdateWatermark = subpartitionStatus.watermark == lastOutputWatermark;

            if (watermarkStatus.isIdle()) {
                subpartitionStatus.watermarkStatus = WatermarkStatus.IDLE;
                markWatermarkUnaligned(subpartitionStatus);

                // Emit watermark first (final progression before going idle)
                if (shouldUpdateWatermark) {
                    tryEmitNewWatermark(output);
                }
                // Then update and emit global watermark status
                tryEmitNewGlobalWatermarkStatus(output);
            } else if (watermarkStatus.isFinished()) {
                subpartitionStatus.watermarkStatus = WatermarkStatus.FINISHED;
                markWatermarkUnaligned(subpartitionStatus);

                // Emit watermark first if this subpartition was contributing
                if (shouldUpdateWatermark) {
                    tryEmitNewWatermark(output);
                }
                // Then update and emit global watermark status
                tryEmitNewGlobalWatermarkStatus(output);
            } else {
                throw new IllegalStateException(
                        "Invalid ACTIVE -> "
                                + watermarkStatus
                                + " transition for channel "
                                + channelIndex
                                + " subpartition "
                                + subpartitionIndexes[channelIndex]);
            }
        } else if (currentSubpartitionStatus.isIdle()) {
            if (watermarkStatus.isActive()) {
                subpartitionStatus.watermarkStatus = WatermarkStatus.ACTIVE;
                // Check if watermark has caught up
                if (subpartitionStatus.watermark >= lastOutputWatermark) {
                    markWatermarkAligned(subpartitionStatus);
                }

                // No watermark emission needed - IDLE subpartitions don't contribute to watermark
                // Update and emit global watermark status
                tryEmitNewGlobalWatermarkStatus(output);
            } else if (watermarkStatus.isFinished()) {
                subpartitionStatus.watermarkStatus = WatermarkStatus.FINISHED;
                subpartitionStatus.watermark = Long.MAX_VALUE;
                markWatermarkUnaligned(subpartitionStatus);

                // No watermark emission needed - subpartition was IDLE (not contributing) before
                // transition
                // Update and emit global watermark status.
                tryEmitNewGlobalWatermarkStatus(output);
            } else {
                throw new IllegalStateException(
                        "Invalid IDLE -> "
                                + watermarkStatus
                                + " transition for channel "
                                + channelIndex
                                + " subpartition "
                                + subpartitionIndexes[channelIndex]);
            }
        } else if (currentSubpartitionStatus.isFinished()) {
            LOG.debug(
                    "Channel {} subpartition {} is in FINISHED state. Ignoring transition to {}.",
                    channelIndex,
                    subpartitionIndexes[channelIndex],
                    watermarkStatus);
        } else {
            throw new IllegalStateException(
                    "Invalid status transition for channel "
                            + channelIndex
                            + " subpartition "
                            + subpartitionIndexes[channelIndex]
                            + ": currentStatus="
                            + currentSubpartitionStatus
                            + ", newStatus="
                            + watermarkStatus);
        }
    }

    // Helper to calculate and emit new watermark if it progresses.
    private void tryEmitNewWatermark(DataOutput<?> output) throws Exception {
        Long newWatermark = calculateWatermarkByAggregationRules();
        if (newWatermark != null && newWatermark > lastOutputWatermark) {
            lastOutputWatermark = newWatermark;
            output.emitWatermark(new Watermark(lastOutputWatermark));
        }
    }

    // Calculate and emit new operator-level (global) status
    private void tryEmitNewGlobalWatermarkStatus(DataOutput<?> output) throws Exception {
        WatermarkStatus newGlobalStatus = calculateGlobalWatermarkStatus();
        if (!newGlobalStatus.equals(lastOutputWatermarkStatus)) {
            lastOutputWatermarkStatus = newGlobalStatus;
            output.emitWatermarkStatus(newGlobalStatus);
        }
    }

    /**
     * Calculates watermark based on the clear aggregation rules.
     *
     * <ol>
     *   <li>If there are ACTIVE subpartitions: watermark = min(active_subpartitions)
     *   <li>Else if there are IDLE subpartitions: watermark = max(idle_subpartitions)
     *   <li>Else (all subpartitions FINISHED): watermark = Long.MAX_VALUE if all have received it
     * </ol>
     */
    private Long calculateWatermarkByAggregationRules() {
        if (SubpartitionStatus.hasActiveSubpartitions(subpartitionStatuses)) {
            // Rule 1: ACTIVE subpartitions exist -> min(active_subpartitions)
            return calculateMinWatermarkFromAlignedChannels();
        } else if (SubpartitionStatus.hasIdleSubpartitions(subpartitionStatuses)) {
            // Rule 2: Only IDLE subpartitions (no active) -> max(idle_subpartitions)
            return calculateMaxWatermarkFromNonFinishedChannels();
        } else {
            // Rule 3: All subpartitions FINISHED -> emit Long.MAX_VALUE only when all have received
            // it
            if (allChannelsFinishedWithMaxValue()) {
                return Long.MAX_VALUE;
            } else {
                return null; // Wait for all subpartitions to receive Long.MAX_VALUE from upstream
            }
        }
    }

    /**
     * Calculates operator-level (global) watermark status by aggregating subpartition states.
     *
     * <ol>
     *   <li>If there are ACTIVE subpartitions: status = ACTIVE
     *   <li>Else if there are IDLE subpartitions: status = IDLE
     *   <li>Else (all subpartitions FINISHED): status = FINISHED
     * </ol>
     */
    private WatermarkStatus calculateGlobalWatermarkStatus() {
        if (SubpartitionStatus.hasActiveSubpartitions(subpartitionStatuses)) {
            // Rule 1: At least one ACTIVE subpartition -> operator is ACTIVE
            return WatermarkStatus.ACTIVE;
        } else if (SubpartitionStatus.hasIdleSubpartitions(subpartitionStatuses)) {
            // Rule 2: No ACTIVE, at least one IDLE subpartition -> operator is IDLE
            return WatermarkStatus.IDLE;
        } else {
            // Rule 3: All subpartitions FINISHED -> operator is FINISHED
            // (Implicitly validated: if not ACTIVE and not IDLE, must be FINISHED)
            return WatermarkStatus.FINISHED;
        }
    }

    // Calculates minimum watermark from aligned (active) subpartitions.
    private Long calculateMinWatermarkFromAlignedChannels() {
        boolean hasAlignedSubpartitions = !alignedSubpartitionStatuses.isEmpty();
        return hasAlignedSubpartitions ? alignedSubpartitionStatuses.peek().watermark : null;
    }

    // Calculates maximum watermark from non-finished (idle) subpartitions.
    private Long calculateMaxWatermarkFromNonFinishedChannels() {
        long maxWatermark = Long.MIN_VALUE;
        boolean hasNonFinishedChannels = false;

        for (Map<Integer, SubpartitionStatus> map : subpartitionStatuses) {
            for (SubpartitionStatus subpartitionStatus : map.values()) {
                if (!subpartitionStatus.watermarkStatus.isFinished()) {
                    hasNonFinishedChannels = true;
                    maxWatermark = Math.max(subpartitionStatus.watermark, maxWatermark);
                }
            }
        }

        return hasNonFinishedChannels ? maxWatermark : null;
    }

    /**
     * Checks if all subpartitions are FINISHED and have received Long.MAX_VALUE watermark. This is
     * the condition for emitting Long.MAX_VALUE downstream.
     */
    private boolean allChannelsFinishedWithMaxValue() {
        for (Map<Integer, SubpartitionStatus> map : subpartitionStatuses) {
            for (SubpartitionStatus status : map.values()) {
                if (!status.watermarkStatus.isFinished() || status.watermark != Long.MAX_VALUE) {
                    return false;
                }
            }
        }
        return true;
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

        /**
         * Utility to check if at least one subpartition in a given array of subpartitions is idle.
         */
        private static boolean hasIdleSubpartitions(
                List<Map<Integer, SubpartitionStatus>> subpartitionStatuses) {
            for (Map<Integer, SubpartitionStatus> map : subpartitionStatuses) {
                for (SubpartitionStatus status : map.values()) {
                    if (status.watermarkStatus.isIdle()) {
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
