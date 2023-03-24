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

package org.apache.flink.runtime.executiongraph;

import org.junit.jupiter.api.Test;

import static org.apache.flink.runtime.executiongraph.VertexInputInfoComputationUtils.computeVertexInputInfoForAllToAll;
import static org.apache.flink.runtime.executiongraph.VertexInputInfoComputationUtils.computeVertexInputInfoForPointwise;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link VertexInputInfoComputationUtils}. */
class VertexInputInfoComputationUtilsTest {

    @Test
    void testComputeConsumedSubpartitionRange3to1() {
        final IndexRange range = computeConsumedSubpartitionRange(0, 1, 3);
        assertThat(range).isEqualTo(new IndexRange(0, 2));
    }

    @Test
    void testComputeConsumedSubpartitionRange3to2() {
        final IndexRange range1 = computeConsumedSubpartitionRange(0, 2, 3);
        assertThat(range1).isEqualTo(new IndexRange(0, 0));

        final IndexRange range2 = computeConsumedSubpartitionRange(1, 2, 3);
        assertThat(range2).isEqualTo(new IndexRange(1, 2));
    }

    @Test
    void testComputeConsumedSubpartitionRange6to4() {
        final IndexRange range1 = computeConsumedSubpartitionRange(0, 4, 6);
        assertThat(range1).isEqualTo(new IndexRange(0, 0));

        final IndexRange range2 = computeConsumedSubpartitionRange(1, 4, 6);
        assertThat(range2).isEqualTo(new IndexRange(1, 2));

        final IndexRange range3 = computeConsumedSubpartitionRange(2, 4, 6);
        assertThat(range3).isEqualTo(new IndexRange(3, 3));

        final IndexRange range4 = computeConsumedSubpartitionRange(3, 4, 6);
        assertThat(range4).isEqualTo(new IndexRange(4, 5));
    }

    @Test
    void testComputeBroadcastConsumedSubpartitionRange() {
        final IndexRange range1 = computeConsumedSubpartitionRange(0, 3, 1, true, true);
        assertThat(range1).isEqualTo(new IndexRange(0, 0));

        final IndexRange range2 = computeConsumedSubpartitionRange(1, 3, 1, true, true);
        assertThat(range2).isEqualTo(new IndexRange(0, 0));

        final IndexRange range3 = computeConsumedSubpartitionRange(2, 3, 1, true, true);
        assertThat(range3).isEqualTo(new IndexRange(0, 0));
    }

    @Test
    void testComputeConsumedSubpartitionRangeForNonDynamicGraph() {
        final IndexRange range1 = computeConsumedSubpartitionRange(0, 3, -1, false, false);
        assertThat(range1).isEqualTo(new IndexRange(0, 0));

        final IndexRange range2 = computeConsumedSubpartitionRange(1, 3, -1, false, false);
        assertThat(range2).isEqualTo(new IndexRange(1, 1));

        final IndexRange range3 = computeConsumedSubpartitionRange(2, 3, -1, false, false);
        assertThat(range3).isEqualTo(new IndexRange(2, 2));
    }

    @Test
    void testComputeVertexInputInfoForAllToAllWithNonDynamicGraph() {
        final JobVertexInputInfo nonBroadcast =
                computeVertexInputInfoForAllToAll(2, 3, ignored -> 3, false, false);
        assertThat(nonBroadcast.getExecutionVertexInputInfos())
                .containsExactlyInAnyOrder(
                        new ExecutionVertexInputInfo(0, new IndexRange(0, 1), new IndexRange(0, 0)),
                        new ExecutionVertexInputInfo(1, new IndexRange(0, 1), new IndexRange(1, 1)),
                        new ExecutionVertexInputInfo(
                                2, new IndexRange(0, 1), new IndexRange(2, 2)));

        final JobVertexInputInfo broadcast =
                computeVertexInputInfoForAllToAll(2, 3, ignored -> 3, false, true);
        assertThat(broadcast.getExecutionVertexInputInfos())
                .containsExactlyInAnyOrder(
                        new ExecutionVertexInputInfo(0, new IndexRange(0, 1), new IndexRange(0, 0)),
                        new ExecutionVertexInputInfo(1, new IndexRange(0, 1), new IndexRange(1, 1)),
                        new ExecutionVertexInputInfo(
                                2, new IndexRange(0, 1), new IndexRange(2, 2)));
    }

    @Test
    void testComputeVertexInputInfoForAllToAllWithDynamicGraph() {
        final JobVertexInputInfo nonBroadcast =
                computeVertexInputInfoForAllToAll(2, 3, ignored -> 10, true, false);
        assertThat(nonBroadcast.getExecutionVertexInputInfos())
                .containsExactlyInAnyOrder(
                        new ExecutionVertexInputInfo(0, new IndexRange(0, 1), new IndexRange(0, 2)),
                        new ExecutionVertexInputInfo(1, new IndexRange(0, 1), new IndexRange(3, 5)),
                        new ExecutionVertexInputInfo(
                                2, new IndexRange(0, 1), new IndexRange(6, 9)));

        final JobVertexInputInfo broadcast =
                computeVertexInputInfoForAllToAll(2, 3, ignored -> 1, true, true);
        assertThat(broadcast.getExecutionVertexInputInfos())
                .containsExactlyInAnyOrder(
                        new ExecutionVertexInputInfo(0, new IndexRange(0, 1), new IndexRange(0, 0)),
                        new ExecutionVertexInputInfo(1, new IndexRange(0, 1), new IndexRange(0, 0)),
                        new ExecutionVertexInputInfo(
                                2, new IndexRange(0, 1), new IndexRange(0, 0)));
    }

    @Test
    void testComputeVertexInputInfoForPointwiseWithNonDynamicGraph() {
        final JobVertexInputInfo jobVertexInputInfo =
                computeVertexInputInfoForPointwise(2, 3, ignored -> 3, false);
        assertThat(jobVertexInputInfo.getExecutionVertexInputInfos())
                .containsExactlyInAnyOrder(
                        new ExecutionVertexInputInfo(0, new IndexRange(0, 0), new IndexRange(0, 0)),
                        new ExecutionVertexInputInfo(1, new IndexRange(0, 0), new IndexRange(1, 1)),
                        new ExecutionVertexInputInfo(
                                2, new IndexRange(1, 1), new IndexRange(0, 0)));
    }

    @Test
    void testComputeVertexInputInfoForPointwiseWithDynamicGraph() {
        final JobVertexInputInfo jobVertexInputInfo =
                computeVertexInputInfoForPointwise(2, 3, ignored -> 4, true);
        assertThat(jobVertexInputInfo.getExecutionVertexInputInfos())
                .containsExactlyInAnyOrder(
                        new ExecutionVertexInputInfo(0, new IndexRange(0, 0), new IndexRange(0, 1)),
                        new ExecutionVertexInputInfo(1, new IndexRange(0, 0), new IndexRange(2, 3)),
                        new ExecutionVertexInputInfo(
                                2, new IndexRange(1, 1), new IndexRange(0, 3)));
    }

    private static IndexRange computeConsumedSubpartitionRange(
            int consumerIndex, int numConsumers, int numSubpartitions) {
        return computeConsumedSubpartitionRange(
                consumerIndex, numConsumers, numSubpartitions, true, false);
    }

    private static IndexRange computeConsumedSubpartitionRange(
            int consumerIndex,
            int numConsumers,
            int numSubpartitions,
            boolean isDynamicGraph,
            boolean isBroadcast) {
        return VertexInputInfoComputationUtils.computeConsumedSubpartitionRange(
                consumerIndex, numConsumers, () -> numSubpartitions, isDynamicGraph, isBroadcast);
    }
}
