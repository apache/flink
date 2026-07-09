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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.io.network.api.writer.SubtaskStateMapper;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.IntStream;

/**
 * Topology computation utilities for POINTWISE edge unaligned-checkpoint rescaling. Shared by JM
 * (subtask assignment / descriptor generation) and TM (channel routing at recovery time).
 *
 * <h3>Atomic primitives</h3>
 *
 * <p>All business methods are compositions of six atomic primitives:
 *
 * <ul>
 *   <li>{@link #consumersOf} — topology edge: upstream → downstream subtask set
 *   <li>{@link #producersOf} — topology edge: downstream → upstream subtask set
 *   <li>{@link #localIndexToGlobalSubtaskIndex} — local channel/subpartition index → peer global
 *       subtask index (local-to-global)
 *   <li>{@link #globalSubtaskIndexToLocalIndex} — inverse: peer global subtask index → local
 *       channel/subpartition index (global-to-local)
 *   <li>{@link #oldSubtasksAssignedTo} — cross-generation: new subtask → inherited old subtask set
 *       (ROUND_ROBIN)
 *   <li>{@link #newSubtaskAssignedFrom} — cross-generation inverse: old subtask → inheriting new
 *       subtask (modulo)
 * </ul>
 *
 * <h3>Core chain</h3>
 *
 * <p>One chain, two directions. JM walks it forward (set-level) when assigning state to decide
 * <em>which</em> old subtasks to recover from; JM distributes buffers (downstream-recovery mode) or
 * TM reads them (upstream-recovery mode) by walking backward to decide <em>where</em> each buffer
 * goes.
 *
 * <p><b>Forward (JM, set-level)</b> — "which old upstreams does new upstream U need state from?"
 *
 * <pre>
 *   newUp ──consumersOf──→ newDown ──oldSubtasksAssignedTo──→ oldDown ──producersOf──→ oldUp
 * </pre>
 *
 * <p>Implemented by {@link #computeInheritedOldUpstreams}. Its inverse {@link
 * #computePrimaryUpstreamMapping} resolves the input side: for a given new downstream, which new
 * upstream is responsible for recovering each old upstream's data.
 *
 * <p><b>Backward (TM)</b> — "where does old buffer (oldUp, localSP) go?"
 *
 * <pre>
 *   (oldUp, localSP) ──{@link #localIndexToGlobalSubtaskIndex}──→ oldDown
 *                      ──{@link #newSubtaskAssignedFrom}──→ newDown
 *                      ──{@link #globalSubtaskIndexToLocalIndex}──→ newLocalSP
 * </pre>
 *
 * <p>Output-side local-to-global and global-to-local both use <b>modulo mapping</b> ({@code D %
 * numConsumers}), matching the subpartition numbering in {@link
 * org.apache.flink.runtime.executiongraph.VertexInputInfoComputationUtils#computeConsumedSubpartitionRange}.
 *
 * <h3>Primary upstream</h3>
 *
 * <p>When a POINTWISE edge has {@code newUpPar > newDownPar}, multiple new upstreams connect to the
 * same downstream. {@link #computeInheritedOldUpstreams} may cause multiple new upstreams to trace
 * back to the same old state. To avoid duplicates, only the first upstream ({@code
 * producersOf(D)[0]}) is responsible for recovering that downstream's state; other upstreams do not
 * recover. This behavior is consistent on the JM side (buffer distribution to downstream) and the
 * TM side (output recovery) via {@link #computeNewLocalSubpartitionIndex}.
 *
 * <h3>Identity fast path</h3>
 *
 * <p>When neither parallelism nor pattern changes (in-place recovery), each subtask recovers its
 * own state. {@link #computeInheritedOldUpstreams} satisfies {@code computeInheritedOldUpstreams(N)
 * = {N}}, producing no overlap — the primary upstream is itself, requiring no extra handling.
 *
 * <p>All arithmetic mirrors {@link
 * org.apache.flink.runtime.executiongraph.VertexInputInfoComputationUtils#computeVertexInputInfoForPointwise}
 * but operates on scalar parallelisms only (no scheduler-layer objects), so it can run on TM.
 */
@Internal
public final class PointwiseChannelMappingUtils {

    private PointwiseChannelMappingUtils() {}

    // ============================== Business methods ==============================

    /**
     * Computes the set of old upstreams whose state a new upstream inherits. Chains consumersOf →
     * oldSubtasksAssignedTo → producersOf (see class javadoc). Results may overlap — multiple new
     * upstreams may trace back to the same old upstream; only the primary upstream ({@code
     * producersOf(D)[0]}) is responsible for recovery.
     */
    public static int[] computeInheritedOldUpstreams(
            int newUpstreamSubtaskIndex, PointwiseRescaleParams params) {
        final int newUpstreamParallelism = params.getNewUpParallelism();
        final int newDownstreamParallelism = params.getNewDownParallelism();
        final int oldUpstreamParallelism = params.getOldUpParallelism();
        final int oldDownstreamParallelism = params.getOldDownParallelism();

        int[] newDownstreams;
        if (params.getNewDistributionPattern() == DistributionPattern.POINTWISE) {
            newDownstreams =
                    consumersOf(
                            newUpstreamSubtaskIndex,
                            newUpstreamParallelism,
                            newDownstreamParallelism);
        } else {
            newDownstreams = IntStream.range(0, newDownstreamParallelism).toArray();
        }

        Set<Integer> oldDownstreams = new HashSet<>();
        for (int newDownstream : newDownstreams) {
            for (int oldDownstream :
                    oldSubtasksAssignedTo(
                            newDownstream, oldDownstreamParallelism, newDownstreamParallelism)) {
                oldDownstreams.add(oldDownstream);
            }
        }

        Set<Integer> oldUpstreams = new HashSet<>();
        for (int oldDownstream : oldDownstreams) {
            if (params.getOldDistributionPattern() == DistributionPattern.ALL_TO_ALL) {
                for (int i = 0; i < oldUpstreamParallelism; i++) {
                    oldUpstreams.add(i);
                }
            } else {
                for (int oldUpstream :
                        producersOf(
                                oldDownstream, oldUpstreamParallelism, oldDownstreamParallelism)) {
                    oldUpstreams.add(oldUpstream);
                }
            }
        }

        return oldUpstreams.stream().sorted().mapToInt(Integer::intValue).toArray();
    }

    /**
     * For a given new downstream, determines which new upstream is responsible for recovering each
     * old upstream. Inverse of {@link #computeInheritedOldUpstreams} scoped to connected new
     * upstreams, using first-claim: when multiple new upstreams trace the same old upstream, only
     * the first claimant is responsible. Returns {@code result[oldUp] = newUp}, or -1 if unclaimed.
     */
    public static int[] computePrimaryUpstreamMapping(
            int newDownstreamSubtaskIndex, PointwiseRescaleParams params) {
        int[] connectedNewUpstreams;
        if (params.getNewDistributionPattern() == DistributionPattern.POINTWISE) {
            connectedNewUpstreams =
                    producersOf(
                            newDownstreamSubtaskIndex,
                            params.getNewUpParallelism(),
                            params.getNewDownParallelism());
        } else {
            connectedNewUpstreams = IntStream.range(0, params.getNewUpParallelism()).toArray();
        }

        int[] mapping = new int[params.getOldUpParallelism()];
        Arrays.fill(mapping, -1);

        for (int newUpstream : connectedNewUpstreams) {
            for (int oldUpstream : computeInheritedOldUpstreams(newUpstream, params)) {
                if (mapping[oldUpstream] < 0) {
                    mapping[oldUpstream] = newUpstream;
                }
            }
        }

        return mapping;
    }

    /**
     * Old-topology local input channel count. A2A = oldUpstreamParallelism, PW =
     * producersOf(subtask).length.
     */
    public static int getOldLocalChannelCount(
            int downstreamSubtaskIndex, PointwiseRescaleParams params) {
        if (params.getOldDistributionPattern() == DistributionPattern.ALL_TO_ALL) {
            return params.getOldUpParallelism();
        }
        return producersOf(
                        downstreamSubtaskIndex,
                        params.getOldUpParallelism(),
                        params.getOldDownParallelism())
                .length;
    }

    /**
     * New upstream subtask index → local input channel index in the new topology. Never returns -1;
     * input-side recovery only routes to upstreams confirmed by {@link
     * #computePrimaryUpstreamMapping}, so a miss throws.
     */
    public static int computeNewLocalInputChannelIndex(
            int newUpstreamSubtaskIndex,
            int newDownstreamSubtaskIndex,
            PointwiseRescaleParams params) {
        if (params.getNewDistributionPattern() == DistributionPattern.ALL_TO_ALL) {
            return newUpstreamSubtaskIndex;
        }
        int localIndex =
                globalSubtaskIndexToLocalIndex(
                        newDownstreamSubtaskIndex,
                        newUpstreamSubtaskIndex,
                        params.getNewUpParallelism(),
                        params.getNewDownParallelism(),
                        true,
                        params.getNewDistributionPattern());
        if (localIndex < 0) {
            throw new IllegalStateException(
                    "newUpstreamSubtaskIndex="
                            + newUpstreamSubtaskIndex
                            + " not found in producers of subtask "
                            + newDownstreamSubtaskIndex
                            + " with params "
                            + params);
        }
        return localIndex;
    }

    /**
     * New downstream subtask index → local subpartition index in the new topology (modulo mapping).
     * Returns -1 when this upstream is not responsible for recovering the downstream:
     *
     * <ol>
     *   <li><b>Not connected</b>: downstream not in {@code consumersOf(upstream)} — old upstreams
     *       traced by {@link #computeInheritedOldUpstreams} on JM may not all connect to this one
     *   <li><b>Not primary upstream</b>: when {@code upPar > downPar}, multiple upstreams connect
     *       to the same downstream; only {@code producersOf(D)[0]} recovers — JM and TM consistent
     * </ol>
     */
    public static int computeNewLocalSubpartitionIndex(
            int newDownstreamSubtaskIndex,
            int newUpstreamSubtaskIndex,
            PointwiseRescaleParams params) {
        if (params.getNewDistributionPattern() == DistributionPattern.ALL_TO_ALL) {
            return newDownstreamSubtaskIndex;
        }
        int localIndex =
                globalSubtaskIndexToLocalIndex(
                        newUpstreamSubtaskIndex,
                        newDownstreamSubtaskIndex,
                        params.getNewUpParallelism(),
                        params.getNewDownParallelism(),
                        false,
                        params.getNewDistributionPattern());
        if (localIndex < 0) {
            return -1;
        }
        // Primary producer dedup: only producersOf(D)[0] writes.
        int[] producers =
                producersOf(
                        newDownstreamSubtaskIndex,
                        params.getNewUpParallelism(),
                        params.getNewDownParallelism());
        if (producers[0] != newUpstreamSubtaskIndex) {
            return -1;
        }
        return localIndex;
    }

    // ============================== Atomic primitives ==============================

    /**
     * Topology edge: downstream subtask → upstream subtask indices that produce for it. Mirrors the
     * assignment algorithm in {@link
     * org.apache.flink.runtime.executiongraph.VertexInputInfoComputationUtils#computeVertexInputInfoForPointwise}.
     * Uses floor division when upPar >= downPar (each downstream connects to a contiguous range of
     * upstreams), ceiling division when upPar < downPar (each downstream connects to one upstream).
     */
    static int[] producersOf(
            int downstreamIndex, int upstreamParallelism, int downstreamParallelism) {
        Preconditions.checkArgument(
                upstreamParallelism > 0 && downstreamParallelism > 0,
                "parallelisms must be positive, got upstreamParallelism=%s, downstreamParallelism=%s",
                upstreamParallelism,
                downstreamParallelism);
        Preconditions.checkArgument(
                downstreamIndex >= 0 && downstreamIndex < downstreamParallelism,
                "index out of range: downstreamIndex=%s, downstreamParallelism=%s",
                downstreamIndex,
                downstreamParallelism);
        if (upstreamParallelism >= downstreamParallelism) {
            int start = downstreamIndex * upstreamParallelism / downstreamParallelism;
            int end = (downstreamIndex + 1) * upstreamParallelism / downstreamParallelism;
            int[] result = new int[end - start];
            for (int i = 0; i < result.length; i++) {
                result[i] = start + i;
            }
            return result;
        } else {
            for (int u = 0; u < upstreamParallelism; u++) {
                int start =
                        (u * downstreamParallelism + upstreamParallelism - 1) / upstreamParallelism;
                int end =
                        ((u + 1) * downstreamParallelism + upstreamParallelism - 1)
                                / upstreamParallelism;
                if (downstreamIndex >= start && downstreamIndex < end) {
                    return new int[] {u};
                }
            }
            throw new IllegalStateException(
                    "No producer found for downstreamIndex="
                            + downstreamIndex
                            + " (upstreamParallelism="
                            + upstreamParallelism
                            + ", downstreamParallelism="
                            + downstreamParallelism
                            + ")");
        }
    }

    /**
     * Topology edge: upstream subtask → downstream subtask indices it produces for. Dual of
     * producersOf with symmetric division: ceiling division when upPar < downPar (each upstream
     * connects to a contiguous range of downstreams), floor division when upPar >= downPar.
     */
    static int[] consumersOf(
            int upstreamIndex, int upstreamParallelism, int downstreamParallelism) {
        Preconditions.checkArgument(
                upstreamParallelism > 0 && downstreamParallelism > 0,
                "parallelisms must be positive, got upstreamParallelism=%s, downstreamParallelism=%s",
                upstreamParallelism,
                downstreamParallelism);
        Preconditions.checkArgument(
                upstreamIndex >= 0 && upstreamIndex < upstreamParallelism,
                "index out of range: upstreamIndex=%s, upstreamParallelism=%s",
                upstreamIndex,
                upstreamParallelism);
        if (upstreamParallelism < downstreamParallelism) {
            int start =
                    (upstreamIndex * downstreamParallelism + upstreamParallelism - 1)
                            / upstreamParallelism;
            int end =
                    ((upstreamIndex + 1) * downstreamParallelism + upstreamParallelism - 1)
                            / upstreamParallelism;
            int[] result = new int[end - start];
            for (int i = 0; i < result.length; i++) {
                result[i] = start + i;
            }
            return result;
        } else {
            for (int d = 0; d < downstreamParallelism; d++) {
                int start = d * upstreamParallelism / downstreamParallelism;
                int end = (d + 1) * upstreamParallelism / downstreamParallelism;
                if (upstreamIndex >= start && upstreamIndex < end) {
                    return new int[] {d};
                }
            }
            throw new IllegalStateException(
                    "No consumer found for upstreamIndex="
                            + upstreamIndex
                            + " (upstreamParallelism="
                            + upstreamParallelism
                            + ", downstreamParallelism="
                            + downstreamParallelism
                            + ")");
        }
    }

    /**
     * Cross-generation: new subtask → inherited old subtask set. Delegates to {@link
     * SubtaskStateMapper#ROUND_ROBIN}, i.e. {@code {old | old % newPar == newIdx}}.
     */
    public static int[] oldSubtasksAssignedTo(
            int newSubtaskIndex, int oldParallelism, int newParallelism) {
        return SubtaskStateMapper.ROUND_ROBIN.getOldSubtasks(
                newSubtaskIndex, oldParallelism, newParallelism);
    }

    /**
     * Cross-generation inverse: old subtask → inheriting new subtask. ROUND_ROBIN inverse: {@code
     * old % newPar}.
     */
    public static int newSubtaskAssignedFrom(int oldSubtaskIndex, int newParallelism) {
        return oldSubtaskIndex % newParallelism;
    }

    /**
     * Local-to-global: translates a local channel/subpartition index to the peer's global subtask
     * index.
     *
     * <p>Input side (downstream→upstream): {@code producersOf(subtask)[localIndex]}, sequential
     * lookup. Output side (upstream→downstream): <b>modulo mapping</b> — finds consumer D where
     * {@code D % numConsumers == localIndex}, matching the subpartition numbering in {@code
     * VertexInputInfoComputationUtils.computeConsumedSubpartitionRange} (the i-th subpartition
     * corresponds to global consumer D where D % len == i).
     *
     * <p>Under ALL_TO_ALL every subtask connects to all peers, so local index equals the global
     * subtask index.
     */
    public static int localIndexToGlobalSubtaskIndex(
            int subtaskIndex,
            int localIndex,
            int upstreamParallelism,
            int downstreamParallelism,
            boolean isInputSide,
            DistributionPattern distributionPattern) {
        if (distributionPattern == DistributionPattern.ALL_TO_ALL) {
            return localIndex;
        }
        if (isInputSide) {
            int[] producers = producersOf(subtaskIndex, upstreamParallelism, downstreamParallelism);
            return producers[localIndex];
        } else {
            int[] consumers = consumersOf(subtaskIndex, upstreamParallelism, downstreamParallelism);
            for (int c : consumers) {
                if (c % consumers.length == localIndex) {
                    return c;
                }
            }
            throw new IllegalStateException(
                    "No consumer with modulo position "
                            + localIndex
                            + " for upstream "
                            + subtaskIndex
                            + " (consumers="
                            + Arrays.toString(consumers)
                            + ")");
        }
    }

    /**
     * Global-to-local: inverse of {@link #localIndexToGlobalSubtaskIndex}.
     *
     * <p>Input side: linear search for peerSubtaskIndex in the producers array. Output side:
     * computes {@code peerSubtaskIndex % consumers.length} directly using the same modulo relation
     * as the forward mapping (no traversal needed, since forward satisfies {@code D % len ==
     * localIdx}, so inverse is {@code localIdx = peer % len}).
     *
     * @return local index, or -1 if not connected
     */
    static int globalSubtaskIndexToLocalIndex(
            int subtaskIndex,
            int peerSubtaskIndex,
            int upstreamParallelism,
            int downstreamParallelism,
            boolean isInputSide,
            DistributionPattern distributionPattern) {
        if (distributionPattern == DistributionPattern.ALL_TO_ALL) {
            return peerSubtaskIndex;
        }
        if (isInputSide) {
            int[] producers = producersOf(subtaskIndex, upstreamParallelism, downstreamParallelism);
            for (int i = 0; i < producers.length; i++) {
                if (producers[i] == peerSubtaskIndex) {
                    return i;
                }
            }
            return -1;
        } else {
            int[] consumers = consumersOf(subtaskIndex, upstreamParallelism, downstreamParallelism);
            if (!containsValue(consumers, peerSubtaskIndex)) {
                return -1;
            }
            return peerSubtaskIndex % consumers.length;
        }
    }

    // ============================== Utilities ==============================

    private static boolean containsValue(int[] arr, int v) {
        for (int x : arr) {
            if (x == v) {
                return true;
            }
        }
        return false;
    }
}
