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

package org.apache.flink.runtime.jobmaster.event;

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils;
import org.apache.flink.runtime.executiongraph.IOMetrics;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.function.BiFunctionWithException;

import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link ExecutionVertexFinishedEvent}. */
class ExecutionVertexFinishedEventTest {

    @Test
    void testExecutionVertexFinishedEventSerializer() throws Exception {
        final TaskManagerLocation taskManagerLocation =
                new TaskManagerLocation(
                        new ResourceID("tm-X"), InetAddress.getLoopbackAddress(), 46);
        // operator coordinator snapshots
        final Map<OperatorID, byte[]> operatorCoordinatorSnapshots = new HashMap<>();
        byte[] bytes = new byte[2000];
        for (int i = 0; i < 3; ++i) {
            new Random(i).nextBytes(bytes);
            operatorCoordinatorSnapshots.put(new OperatorID(), bytes);
        }

        // operator coordinator snapshot futures
        final Map<OperatorID, CompletableFuture<byte[]>> operatorCoordinatorSnapshotFutures =
                new HashMap<>();
        operatorCoordinatorSnapshots.forEach(
                (key, value) ->
                        operatorCoordinatorSnapshotFutures.put(
                                key, CompletableFuture.completedFuture(value)));

        // io metrics and user accumulators
        IOMetrics ioMetrics = new IOMetrics(1, 2, 3, 4, 5, 6, 7);
        Map<String, Accumulator<?, ?>> accumulators = new HashMap<>();
        accumulators.put("acc", new IntCounter(8));

        ExecutionVertexFinishedEvent event =
                new ExecutionVertexFinishedEvent(
                        ExecutionGraphTestUtils.createExecutionAttemptId(),
                        taskManagerLocation,
                        operatorCoordinatorSnapshotFutures,
                        null,
                        ioMetrics,
                        accumulators);

        ExecutionVertexFinishedEvent.Serializer serializer =
                new ExecutionVertexFinishedEvent.Serializer();

        byte[] binaryLog = serializer.serialize(event);
        ExecutionVertexFinishedEvent deserializedEvent =
                (ExecutionVertexFinishedEvent)
                        serializer.deserialize(serializer.getVersion(), binaryLog);

        assertThat(event.getExecutionVertexId())
                .isEqualTo(deserializedEvent.getExecutionVertexId());
        assertThat(event.getAttemptNumber()).isEqualTo(deserializedEvent.getAttemptNumber());
        assertThat(event.getTaskManagerLocation())
                .isEqualTo(deserializedEvent.getTaskManagerLocation());
        assertThat(
                        snapshotsEquals(
                                event.getOperatorCoordinatorSnapshotFutures(),
                                deserializedEvent.getOperatorCoordinatorSnapshotFutures()))
                .isTrue();
        assertThat(
                        userAccumulatorsEquals(
                                event.getUserAccumulators(),
                                deserializedEvent.getUserAccumulators()))
                .isTrue();
        assertThat(ioMetricsEquals(event.getIOMetrics(), deserializedEvent.getIOMetrics()))
                .isTrue();
    }

    private static boolean ioMetricsEquals(IOMetrics ioMetricsA, IOMetrics ioMetricsB) {
        return ioMetricsA.getNumBytesIn() == ioMetricsB.getNumBytesIn()
                && ioMetricsA.getNumBytesOut() == ioMetricsB.getNumBytesOut()
                && ioMetricsA.getNumRecordsIn() == ioMetricsB.getNumRecordsIn()
                && ioMetricsA.getNumRecordsOut() == ioMetricsB.getNumRecordsOut();
    }

    private static boolean userAccumulatorsEquals(
            Map<String, Accumulator<?, ?>> userAccumulatorsA,
            Map<String, Accumulator<?, ?>> userAccumulatorsB)
            throws Exception {
        return mapEquals(
                userAccumulatorsA,
                userAccumulatorsB,
                (accumulatorA, accumulatorB) ->
                        accumulatorA.getLocalValue().equals(accumulatorB.getLocalValue()));
    }

    private static boolean snapshotsEquals(
            Map<OperatorID, CompletableFuture<byte[]>> snapshotsA,
            Map<OperatorID, CompletableFuture<byte[]>> snapshotsB)
            throws Exception {
        return mapEquals(
                snapshotsA,
                snapshotsB,
                (futureA, futureB) -> Arrays.equals(futureA.get(), futureB.get()));
    }

    private static <K, V> boolean mapEquals(
            Map<K, V> mapA,
            Map<K, V> mapB,
            BiFunctionWithException<V, V, Boolean, Exception> valueComparator)
            throws Exception {
        if (mapA.size() == mapB.size() && mapA.keySet().containsAll(mapB.keySet())) {
            for (Map.Entry<K, V> entry : mapA.entrySet()) {
                if (!valueComparator.apply(entry.getValue(), mapB.get(entry.getKey()))) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }
}
