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

import org.apache.flink.runtime.jobgraph.DistributionPattern;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link PointwiseChannelMappingUtils}. */
class PointwiseChannelMappingUtilsTest {

    private static final DistributionPattern PW = DistributionPattern.POINTWISE;
    private static final DistributionPattern A2A = DistributionPattern.ALL_TO_ALL;

    private static PointwiseRescaleParams params(int oldUp, int oldDown, int newUp, int newDown) {
        return new PointwiseRescaleParams(PW, PW, oldUp, oldDown, newUp, newDown);
    }

    private static PointwiseRescaleParams params(
            DistributionPattern oldPat,
            DistributionPattern newPat,
            int oldUp,
            int oldDown,
            int newUp,
            int newDown) {
        return new PointwiseRescaleParams(oldPat, newPat, oldUp, oldDown, newUp, newDown);
    }

    // ======================== producersOf / consumersOf ========================

    @Test
    void producersOfAndConsumersOfAreInverses() {
        for (int upPar = 1; upPar <= 8; upPar++) {
            for (int downPar = 1; downPar <= 8; downPar++) {
                for (int d = 0; d < downPar; d++) {
                    int[] producers = PointwiseChannelMappingUtils.producersOf(d, upPar, downPar);
                    for (int p : producers) {
                        int[] consumers =
                                PointwiseChannelMappingUtils.consumersOf(p, upPar, downPar);
                        assertThat(consumers)
                                .as(
                                        "up=%d down=%d: d=%d should be in consumersOf(%d)",
                                        upPar, downPar, d, p)
                                .contains(d);
                    }
                }
            }
        }
    }

    @Test
    void producersOfCoversAllUpstream() {
        for (int upPar = 1; upPar <= 8; upPar++) {
            for (int downPar = 1; downPar <= 8; downPar++) {
                Set<Integer> allProducers = new HashSet<>();
                for (int d = 0; d < downPar; d++) {
                    int[] producers = PointwiseChannelMappingUtils.producersOf(d, upPar, downPar);
                    for (int p : producers) {
                        allProducers.add(p);
                    }
                }
                assertThat(allProducers)
                        .as("up=%d down=%d: all upstream covered", upPar, downPar)
                        .hasSize(upPar);
            }
        }
    }

    @Test
    void consumersOfCoversAllDownstream() {
        for (int upPar = 1; upPar <= 8; upPar++) {
            for (int downPar = 1; downPar <= 8; downPar++) {
                Set<Integer> allConsumers = new HashSet<>();
                for (int u = 0; u < upPar; u++) {
                    int[] consumers = PointwiseChannelMappingUtils.consumersOf(u, upPar, downPar);
                    for (int c : consumers) {
                        allConsumers.add(c);
                    }
                }
                assertThat(allConsumers)
                        .as("up=%d down=%d: all downstream covered", upPar, downPar)
                        .hasSize(downPar);
            }
        }
    }

    @Test
    void producersOfConcreteValues() {
        // 2 upstream -> 4 downstream (upscale): each downstream has one producer
        assertThat(PointwiseChannelMappingUtils.producersOf(0, 2, 4)).containsExactly(0);
        assertThat(PointwiseChannelMappingUtils.producersOf(1, 2, 4)).containsExactly(0);
        assertThat(PointwiseChannelMappingUtils.producersOf(2, 2, 4)).containsExactly(1);
        assertThat(PointwiseChannelMappingUtils.producersOf(3, 2, 4)).containsExactly(1);

        // 4 upstream -> 2 downstream (downscale): each downstream merges 2 producers
        assertThat(PointwiseChannelMappingUtils.producersOf(0, 4, 2)).containsExactly(0, 1);
        assertThat(PointwiseChannelMappingUtils.producersOf(1, 4, 2)).containsExactly(2, 3);

        // 3 upstream -> 3 downstream (equal): identity
        assertThat(PointwiseChannelMappingUtils.producersOf(0, 3, 3)).containsExactly(0);
        assertThat(PointwiseChannelMappingUtils.producersOf(1, 3, 3)).containsExactly(1);
        assertThat(PointwiseChannelMappingUtils.producersOf(2, 3, 3)).containsExactly(2);
    }

    @Test
    void consumersOfConcreteValues() {
        // 2 upstream -> 4 downstream (upscale): each upstream serves 2 consumers
        assertThat(PointwiseChannelMappingUtils.consumersOf(0, 2, 4)).containsExactly(0, 1);
        assertThat(PointwiseChannelMappingUtils.consumersOf(1, 2, 4)).containsExactly(2, 3);

        // 4 upstream -> 2 downstream (downscale): each upstream feeds one consumer
        assertThat(PointwiseChannelMappingUtils.consumersOf(0, 4, 2)).containsExactly(0);
        assertThat(PointwiseChannelMappingUtils.consumersOf(1, 4, 2)).containsExactly(0);
        assertThat(PointwiseChannelMappingUtils.consumersOf(2, 4, 2)).containsExactly(1);
        assertThat(PointwiseChannelMappingUtils.consumersOf(3, 4, 2)).containsExactly(1);
    }

    // ======================== localIndexToGlobalSubtaskIndex ========================

    @Test
    void localIndexToGlobalSubtaskIndexPointwiseInputSide() {
        // 4 upstream -> 2 downstream: d=0 has producers {0,1}, d=1 has producers {2,3}
        assertThat(
                        PointwiseChannelMappingUtils.localIndexToGlobalSubtaskIndex(
                                0, 0, 4, 2, true, PW))
                .isEqualTo(0);
        assertThat(
                        PointwiseChannelMappingUtils.localIndexToGlobalSubtaskIndex(
                                0, 1, 4, 2, true, PW))
                .isEqualTo(1);
        assertThat(
                        PointwiseChannelMappingUtils.localIndexToGlobalSubtaskIndex(
                                1, 0, 4, 2, true, PW))
                .isEqualTo(2);
        assertThat(
                        PointwiseChannelMappingUtils.localIndexToGlobalSubtaskIndex(
                                1, 1, 4, 2, true, PW))
                .isEqualTo(3);
    }

    @Test
    void localIndexToGlobalSubtaskIndexPointwiseOutputSide() {
        // 2 upstream -> 4 downstream: u=0 has consumers {0,1}, u=1 has consumers {2,3}
        // consumers start at 0 and 2 (both % 2 == 0), so modulo == sequential here.
        assertThat(
                        PointwiseChannelMappingUtils.localIndexToGlobalSubtaskIndex(
                                0, 0, 2, 4, false, PW))
                .isEqualTo(0);
        assertThat(
                        PointwiseChannelMappingUtils.localIndexToGlobalSubtaskIndex(
                                0, 1, 2, 4, false, PW))
                .isEqualTo(1);
        assertThat(
                        PointwiseChannelMappingUtils.localIndexToGlobalSubtaskIndex(
                                1, 0, 2, 4, false, PW))
                .isEqualTo(2);
        assertThat(
                        PointwiseChannelMappingUtils.localIndexToGlobalSubtaskIndex(
                                1, 1, 2, 4, false, PW))
                .isEqualTo(3);

        // 4 upstream -> 6 downstream: u=2 has consumers {3,4}
        // consumers[0]=3, 3%2=1 ≠ 0, so modulo ≠ sequential:
        // sp 0 → consumer 4 (4%2=0), sp 1 → consumer 3 (3%2=1)
        assertThat(
                        PointwiseChannelMappingUtils.localIndexToGlobalSubtaskIndex(
                                2, 0, 4, 6, false, PW))
                .isEqualTo(4);
        assertThat(
                        PointwiseChannelMappingUtils.localIndexToGlobalSubtaskIndex(
                                2, 1, 4, 6, false, PW))
                .isEqualTo(3);
    }

    @Test
    void localIndexToGlobalSubtaskIndexBoundsCheck() {
        assertThatThrownBy(
                        () ->
                                PointwiseChannelMappingUtils.localIndexToGlobalSubtaskIndex(
                                        0, 5, 4, 2, true, PW))
                .isInstanceOf(ArrayIndexOutOfBoundsException.class);
    }

    // ======================== getOldLocalChannelCount ========================

    @Test
    void getOldLocalChannelCountAllToAll() {
        PointwiseRescaleParams p = new PointwiseRescaleParams(A2A, A2A, 5, 3, 5, 3);
        assertThat(PointwiseChannelMappingUtils.getOldLocalChannelCount(0, p)).isEqualTo(5);
        assertThat(PointwiseChannelMappingUtils.getOldLocalChannelCount(2, p)).isEqualTo(5);
    }

    @Test
    void getOldLocalChannelCountPointwise() {
        // 4 upstream -> 2 downstream: each downstream has 2 producers
        PointwiseRescaleParams p = params(4, 2, 4, 2);
        assertThat(PointwiseChannelMappingUtils.getOldLocalChannelCount(0, p)).isEqualTo(2);
        assertThat(PointwiseChannelMappingUtils.getOldLocalChannelCount(1, p)).isEqualTo(2);
    }

    // ======================== computeNewLocalInputChannelIndex / SubpartitionIndex ===

    @Test
    void computeNewLocalInputChannelIndexAllToAll() {
        PointwiseRescaleParams p = new PointwiseRescaleParams(PW, A2A, 4, 2, 4, 2);
        assertThat(PointwiseChannelMappingUtils.computeNewLocalInputChannelIndex(3, 0, p))
                .isEqualTo(3);
    }

    @Test
    void computeNewLocalInputChannelIndexPointwise() {
        // 4 upstream -> 2 downstream: d=0 connects to {0,1}, d=1 connects to {2,3}
        PointwiseRescaleParams p = params(4, 2, 4, 2);
        assertThat(PointwiseChannelMappingUtils.computeNewLocalInputChannelIndex(0, 0, p))
                .isEqualTo(0);
        assertThat(PointwiseChannelMappingUtils.computeNewLocalInputChannelIndex(1, 0, p))
                .isEqualTo(1);
        assertThat(PointwiseChannelMappingUtils.computeNewLocalInputChannelIndex(2, 1, p))
                .isEqualTo(0);
    }

    @Test
    void computeNewLocalSubpartitionIndexAllToAll() {
        PointwiseRescaleParams p = new PointwiseRescaleParams(PW, A2A, 2, 4, 2, 4);
        assertThat(PointwiseChannelMappingUtils.computeNewLocalSubpartitionIndex(3, 0, p))
                .isEqualTo(3);
    }

    @Test
    void computeNewLocalSubpartitionIndexPointwise() {
        // 2 upstream -> 4 downstream: u=0 connects to {0,1}, u=1 connects to {2,3}
        PointwiseRescaleParams p = params(2, 4, 2, 4);
        assertThat(PointwiseChannelMappingUtils.computeNewLocalSubpartitionIndex(0, 0, p))
                .isEqualTo(0);
        assertThat(PointwiseChannelMappingUtils.computeNewLocalSubpartitionIndex(1, 0, p))
                .isEqualTo(1);
        assertThat(PointwiseChannelMappingUtils.computeNewLocalSubpartitionIndex(2, 1, p))
                .isEqualTo(0);
    }

    // =============== oldSubtasksAssignedTo / newSubtaskAssignedFrom ===============

    @Test
    void oldSubtasksAssignedToAndNewSubtaskAssignedFromAreConsistent() {
        for (int oldPar = 1; oldPar <= 8; oldPar++) {
            for (int newPar = 1; newPar <= 8; newPar++) {
                for (int newIdx = 0; newIdx < newPar; newIdx++) {
                    int[] oldSubtasks =
                            PointwiseChannelMappingUtils.oldSubtasksAssignedTo(
                                    newIdx, oldPar, newPar);
                    for (int oldIdx : oldSubtasks) {
                        assertThat(
                                        PointwiseChannelMappingUtils.newSubtaskAssignedFrom(
                                                oldIdx, newPar))
                                .as(
                                        "old=%d,new=%d: newSubtaskAssignedFrom(%d) == %d",
                                        oldPar, newPar, oldIdx, newIdx)
                                .isEqualTo(newIdx);
                    }
                }
            }
        }
    }

    @Test
    void oldSubtasksAssignedToCoversAllOld() {
        for (int oldPar = 1; oldPar <= 8; oldPar++) {
            for (int newPar = 1; newPar <= 8; newPar++) {
                Set<Integer> covered = new HashSet<>();
                for (int newIdx = 0; newIdx < newPar; newIdx++) {
                    for (int oldIdx :
                            PointwiseChannelMappingUtils.oldSubtasksAssignedTo(
                                    newIdx, oldPar, newPar)) {
                        covered.add(oldIdx);
                    }
                }
                assertThat(covered)
                        .as("old=%d new=%d: all old subtasks assigned", oldPar, newPar)
                        .hasSize(oldPar);
            }
        }
    }

    @Test
    void newSubtaskAssignedFromIsModulo() {
        assertThat(PointwiseChannelMappingUtils.newSubtaskAssignedFrom(5, 3)).isEqualTo(2);
        assertThat(PointwiseChannelMappingUtils.newSubtaskAssignedFrom(0, 4)).isEqualTo(0);
        assertThat(PointwiseChannelMappingUtils.newSubtaskAssignedFrom(7, 3)).isEqualTo(1);
    }

    // ======================== computeInheritedOldUpstreams ========================

    @Test
    void computeInheritedOldUpstreamsPwToPw() {
        // 3 up -> 3 down (PW), rescale to 4 up -> 3 down (PW)
        // newUp=0: consumers in new topo = consumersOf(0,4,3) = {0}
        //   newDown=0: oldSubtasksAssignedTo(0,3,3) = {0}
        //     oldDown=0: producersOf(0,3,3) = {0}
        // result = {0}
        PointwiseRescaleParams p = params(3, 3, 4, 3);
        assertThat(PointwiseChannelMappingUtils.computeInheritedOldUpstreams(0, p))
                .containsExactly(0);

        // newUp=3: consumers in new topo = consumersOf(3,4,3) = {2}
        //   newDown=2: oldSubtasksAssignedTo(2,3,3) = {2}
        //     oldDown=2: producersOf(2,3,3) = {2}
        // result = {2}
        assertThat(PointwiseChannelMappingUtils.computeInheritedOldUpstreams(3, p))
                .containsExactly(2);
    }

    @Test
    void computeInheritedOldUpstreamsA2aToPw() {
        // Old A2A (3 up, 3 down) -> New PW (4 up, 3 down)
        // Any old downstream connects to all 3 old upstreams (A2A), so result = {0,1,2}
        PointwiseRescaleParams p = params(A2A, PW, 3, 3, 4, 3);
        for (int newUp = 0; newUp < 4; newUp++) {
            assertThat(PointwiseChannelMappingUtils.computeInheritedOldUpstreams(newUp, p))
                    .as("newUp=%d", newUp)
                    .containsExactly(0, 1, 2);
        }
    }

    @Test
    void computeInheritedOldUpstreamsPwToA2a() {
        // Old PW (3 up, 3 down) -> New A2A (4 up, 3 down)
        // New A2A: every new upstream serves ALL new downstreams {0,1,2}
        // oldSubtasksAssignedTo covers all 3 old downstreams → all old producers reached
        PointwiseRescaleParams p = params(PW, A2A, 3, 3, 4, 3);
        for (int newUp = 0; newUp < 4; newUp++) {
            assertThat(PointwiseChannelMappingUtils.computeInheritedOldUpstreams(newUp, p))
                    .as("newUp=%d", newUp)
                    .containsExactly(0, 1, 2);
        }
    }

    @Test
    void computeInheritedOldUpstreamsCoversAllOldUpstreams() {
        for (int oldUp = 1; oldUp <= 6; oldUp++) {
            for (int oldDown = 1; oldDown <= 6; oldDown++) {
                for (int newUp = 1; newUp <= 6; newUp++) {
                    for (int newDown = 1; newDown <= 6; newDown++) {
                        PointwiseRescaleParams p = params(oldUp, oldDown, newUp, newDown);
                        Set<Integer> allClaimed = new HashSet<>();
                        for (int u = 0; u < newUp; u++) {
                            for (int oldU :
                                    PointwiseChannelMappingUtils.computeInheritedOldUpstreams(
                                            u, p)) {
                                allClaimed.add(oldU);
                            }
                        }
                        assertThat(allClaimed)
                                .as(
                                        "PW %d:%d->%d:%d: all old upstreams covered",
                                        oldUp, oldDown, newUp, newDown)
                                .hasSize(oldUp);
                    }
                }
            }
        }
    }

    // ======================== computePrimaryUpstreamMapping ========================

    @Test
    void computePrimaryUpstreamMappingPwToPw() {
        // 3 up -> 3 down (PW), rescale to 4 up -> 3 down (PW)
        // newDown=0: connected new upstreams = producersOf(0,4,3) = {0}
        //   newUp=0: computeInheritedOldUpstreams = {0} → mapping[0] = 0
        PointwiseRescaleParams p = params(3, 3, 4, 3);
        int[] ownership = PointwiseChannelMappingUtils.computePrimaryUpstreamMapping(0, p);
        assertThat(ownership).hasSize(3);
        assertThat(ownership[0]).isEqualTo(0);
    }

    @Test
    void computePrimaryUpstreamMappingNoUnclaimedForConnectedOldUpstreams() {
        for (int oldUp = 1; oldUp <= 6; oldUp++) {
            for (int oldDown = 1; oldDown <= 6; oldDown++) {
                for (int newUp = 1; newUp <= 6; newUp++) {
                    for (int newDown = 1; newDown <= 6; newDown++) {
                        PointwiseRescaleParams p = params(oldUp, oldDown, newUp, newDown);
                        for (int newD = 0; newD < newDown; newD++) {
                            int[] ownership =
                                    PointwiseChannelMappingUtils.computePrimaryUpstreamMapping(
                                            newD, p);
                            // Every old upstream that was connected to any old downstream
                            // that maps to this new downstream must be claimed.
                            int[] oldDownstreams =
                                    PointwiseChannelMappingUtils.oldSubtasksAssignedTo(
                                            newD, oldDown, newDown);
                            for (int oldD : oldDownstreams) {
                                int[] oldProducers =
                                        PointwiseChannelMappingUtils.producersOf(
                                                oldD, oldUp, oldDown);
                                for (int oldU : oldProducers) {
                                    assertThat(ownership[oldU])
                                            .as(
                                                    "%d:%d->%d:%d newD=%d oldD=%d oldU=%d must be claimed",
                                                    oldUp, oldDown, newUp, newDown, newD, oldD,
                                                    oldU)
                                            .isGreaterThanOrEqualTo(0);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    @Test
    void computePrimaryUpstreamMappingOwnerIsConnected() {
        for (int oldUp = 1; oldUp <= 6; oldUp++) {
            for (int oldDown = 1; oldDown <= 6; oldDown++) {
                for (int newUp = 1; newUp <= 6; newUp++) {
                    for (int newDown = 1; newDown <= 6; newDown++) {
                        PointwiseRescaleParams p = params(oldUp, oldDown, newUp, newDown);
                        for (int newD = 0; newD < newDown; newD++) {
                            int[] ownership =
                                    PointwiseChannelMappingUtils.computePrimaryUpstreamMapping(
                                            newD, p);
                            int[] connectedNewUp =
                                    PointwiseChannelMappingUtils.producersOf(newD, newUp, newDown);
                            for (int oldU = 0; oldU < oldUp; oldU++) {
                                if (ownership[oldU] >= 0) {
                                    assertThat(connectedNewUp)
                                            .as(
                                                    "%d:%d->%d:%d newD=%d: owner of oldU=%d should be connected",
                                                    oldUp, oldDown, newUp, newDown, newD, oldU)
                                            .contains(ownership[oldU]);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // ======================== Primary producer dedup ========================

    @Test
    void primaryProducerDedupExactlyOneWriterPerDownstream() {
        // When newUpPar > newDownPar, multiple upstreams connect to the same downstream.
        // computeNewLocalSubpartitionIndex must return non-negative for exactly one.
        int[][] cases = {{4, 2}, {10, 3}, {11, 3}, {6, 2}, {7, 1}};
        for (int[] c : cases) {
            int newUp = c[0], newDown = c[1];
            PointwiseRescaleParams p = params(newUp, newDown, newUp, newDown);
            for (int d = 0; d < newDown; d++) {
                int writerCount = 0;
                int writerIndex = -1;
                for (int u = 0; u < newUp; u++) {
                    int sub =
                            PointwiseChannelMappingUtils.computeNewLocalSubpartitionIndex(d, u, p);
                    if (sub >= 0) {
                        writerCount++;
                        writerIndex = u;
                    }
                }
                assertThat(writerCount)
                        .as("%d:%d downstream %d: exactly one writer", newUp, newDown, d)
                        .isEqualTo(1);
                // The writer must be producersOf(d)[0] (primary producer)
                int[] producers = PointwiseChannelMappingUtils.producersOf(d, newUp, newDown);
                assertThat(writerIndex)
                        .as("%d:%d downstream %d: writer is primary producer", newUp, newDown, d)
                        .isEqualTo(producers[0]);
            }
        }
    }

    // ======================== Modulo-based subpartition/channel mapping ========================

    /**
     * The execution graph assigns subpartitions via {@code consumerSubtaskIndex % numConsumers}
     * (see {@code VertexInputInfoComputationUtils.computeConsumedSubpartitionRange}). When
     * consumers don't start at a multiple of numConsumers, this differs from sequential indexing
     * ({@code D - consumers[0]}). Both {@code localIndexToGlobalSubtaskIndex} (output side) and
     * {@code computeNewLocalSubpartitionIndex} must use modulo, not sequential offset.
     *
     * <p>Concrete example: 4 upstream → 6 downstream POINTWISE. Upstream 2 has consumers [3,4].
     * Sequential would map sp0→D3, sp1→D4. But the execution graph assigns D3→sp(3%2=1),
     * D4→sp(4%2=0). So sp0→D4, sp1→D3. Using sequential causes buffers to be routed to the wrong
     * downstream, leading to "Cannot select SubtaskConnectionDescriptor" errors at the DEMUX layer.
     */
    @Test
    void moduloMappingDiffersFromSequentialWhenOffsetNotAligned() {
        // 4 up -> 6 down: upstream 2 has consumers [3,4]
        int[] consumers = PointwiseChannelMappingUtils.consumersOf(2, 4, 6);
        assertThat(consumers).containsExactly(3, 4);

        // localIndexToGlobalSubtaskIndex output side: sp → consumer via modulo
        // sp 0 → consumer where c%2==0 → 4 (NOT sequential consumers[0]=3)
        assertThat(
                        PointwiseChannelMappingUtils.localIndexToGlobalSubtaskIndex(
                                2, 0, 4, 6, false, PW))
                .isEqualTo(4);
        // sp 1 → consumer where c%2==1 → 3 (NOT sequential consumers[1]=4)
        assertThat(
                        PointwiseChannelMappingUtils.localIndexToGlobalSubtaskIndex(
                                2, 1, 4, 6, false, PW))
                .isEqualTo(3);

        // computeNewLocalSubpartitionIndex: consumer → sp via modulo
        PointwiseRescaleParams p = params(4, 6, 4, 6);
        // D=3 → sp 3%2=1 (NOT sequential 3-3=0)
        assertThat(PointwiseChannelMappingUtils.computeNewLocalSubpartitionIndex(3, 2, p))
                .isEqualTo(1);
        // D=4 → sp 4%2=0 (NOT sequential 4-3=1)
        assertThat(PointwiseChannelMappingUtils.computeNewLocalSubpartitionIndex(4, 2, p))
                .isEqualTo(0);
    }

    @Test
    void subpartitionIndexModuloForNonZeroOffset() {
        // Subpartition assignment uses D % numConsumers (matching the execution graph),
        // NOT D - consumers[0] (sequential offset). This distinction matters when
        // consumers[0] % consumers.length != 0.

        // 3 up -> 10 down (PW): upstream 1 has consumers [4,5,6]
        // D=4 → sp 4%3=1, D=5 → sp 5%3=2, D=6 → sp 6%3=0
        PointwiseRescaleParams p = params(3, 10, 3, 10);
        int[] consumers = PointwiseChannelMappingUtils.consumersOf(1, 3, 10);
        assertThat(consumers).startsWith(4); // sanity check: non-zero offset
        for (int c : consumers) {
            assertThat(PointwiseChannelMappingUtils.computeNewLocalSubpartitionIndex(c, 1, p))
                    .as("3:10 upstream 1, D=%d → sp %d", c, c % consumers.length)
                    .isEqualTo(c % consumers.length);
        }

        // 2 up -> 5 down (PW): upstream 1 has consumers [3,4]
        // D=3 → sp 3%2=1, D=4 → sp 4%2=0
        PointwiseRescaleParams p2 = params(2, 5, 2, 5);
        int[] consumers2 = PointwiseChannelMappingUtils.consumersOf(1, 2, 5);
        assertThat(consumers2).startsWith(3);
        for (int c : consumers2) {
            assertThat(PointwiseChannelMappingUtils.computeNewLocalSubpartitionIndex(c, 1, p2))
                    .as("2:5 upstream 1, D=%d → sp %d", c, c % consumers2.length)
                    .isEqualTo(c % consumers2.length);
        }
    }

    @Test
    void subpartitionIndexIsModuloForAllParallelisms() {
        // Exhaustively verify D % numConsumers for all upPar x downPar up to 8.
        for (int upPar = 1; upPar <= 8; upPar++) {
            for (int downPar = 1; downPar <= 8; downPar++) {
                PointwiseRescaleParams p = params(upPar, downPar, upPar, downPar);
                for (int u = 0; u < upPar; u++) {
                    int[] consumers = PointwiseChannelMappingUtils.consumersOf(u, upPar, downPar);
                    for (int c : consumers) {
                        int sp =
                                PointwiseChannelMappingUtils.computeNewLocalSubpartitionIndex(
                                        c, u, p);
                        int[] producers =
                                PointwiseChannelMappingUtils.producersOf(c, upPar, downPar);
                        if (producers[0] == u) {
                            assertThat(sp)
                                    .as(
                                            "up=%d down=%d u=%d D=%d: sp should be D %% numConsumers",
                                            upPar, downPar, u, c)
                                    .isEqualTo(c % consumers.length);
                        }
                    }
                }
            }
        }
    }

    @Test
    void outputSideLocalIndexModuloForAllParallelisms() {
        // Exhaustively verify modulo mapping on the output side of localIndexToGlobalSubtaskIndex.
        for (int upPar = 1; upPar <= 8; upPar++) {
            for (int downPar = 1; downPar <= 8; downPar++) {
                for (int u = 0; u < upPar; u++) {
                    int[] consumers = PointwiseChannelMappingUtils.consumersOf(u, upPar, downPar);
                    for (int sp = 0; sp < consumers.length; sp++) {
                        int globalD =
                                PointwiseChannelMappingUtils.localIndexToGlobalSubtaskIndex(
                                        u, sp, upPar, downPar, false, PW);
                        assertThat(globalD % consumers.length)
                                .as(
                                        "up=%d down=%d u=%d sp=%d: D %% numConsumers should equal sp",
                                        upPar, downPar, u, sp)
                                .isEqualTo(sp);
                    }
                }
            }
        }
    }

    // =============== Round-trip: localIndexToGlobalSubtaskIndex ↔ subpartition index
    // ===============

    @Test
    void localIndexToGlobalSubtaskIndexAndSubpartitionIndexAreInverses() {
        for (int upPar = 1; upPar <= 6; upPar++) {
            for (int downPar = 1; downPar <= 6; downPar++) {
                PointwiseRescaleParams p = params(upPar, downPar, upPar, downPar);
                for (int u = 0; u < upPar; u++) {
                    int[] consumers = PointwiseChannelMappingUtils.consumersOf(u, upPar, downPar);
                    // Only the primary producer can round-trip (others get -1)
                    for (int localIndex = 0; localIndex < consumers.length; localIndex++) {
                        int globalD =
                                PointwiseChannelMappingUtils.localIndexToGlobalSubtaskIndex(
                                        u, localIndex, upPar, downPar, false, PW);
                        // Output side uses modulo mapping (D % numConsumers == localIndex),
                        // not sequential consumers[localIndex].
                        assertThat(consumers).contains(globalD);
                        assertThat(globalD % consumers.length)
                                .as("up=%d down=%d u=%d local=%d", upPar, downPar, u, localIndex)
                                .isEqualTo(localIndex);
                        int[] producers =
                                PointwiseChannelMappingUtils.producersOf(globalD, upPar, downPar);
                        if (producers[0] == u) {
                            int backLocal =
                                    PointwiseChannelMappingUtils.computeNewLocalSubpartitionIndex(
                                            globalD, u, p);
                            assertThat(backLocal)
                                    .as(
                                            "up=%d down=%d u=%d: round-trip localIndex=%d",
                                            upPar, downPar, u, localIndex)
                                    .isEqualTo(localIndex);
                        }
                    }
                }
            }
        }
    }

    @Test
    void localIndexToGlobalSubtaskIndexAndInputChannelIndexAreInverses() {
        for (int upPar = 1; upPar <= 6; upPar++) {
            for (int downPar = 1; downPar <= 6; downPar++) {
                PointwiseRescaleParams p = params(upPar, downPar, upPar, downPar);
                for (int d = 0; d < downPar; d++) {
                    int[] producers = PointwiseChannelMappingUtils.producersOf(d, upPar, downPar);
                    for (int localIndex = 0; localIndex < producers.length; localIndex++) {
                        int globalU =
                                PointwiseChannelMappingUtils.localIndexToGlobalSubtaskIndex(
                                        d, localIndex, upPar, downPar, true, PW);
                        assertThat(globalU).isEqualTo(producers[localIndex]);
                        int backLocal =
                                PointwiseChannelMappingUtils.computeNewLocalInputChannelIndex(
                                        globalU, d, p);
                        assertThat(backLocal)
                                .as(
                                        "up=%d down=%d d=%d: round-trip localIndex=%d",
                                        upPar, downPar, d, localIndex)
                                .isEqualTo(localIndex);
                    }
                }
            }
        }
    }
}
