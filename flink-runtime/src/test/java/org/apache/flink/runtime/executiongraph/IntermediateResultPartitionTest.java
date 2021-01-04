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

import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.testutils.DirectScheduledExecutorService;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.getExecutionJobVertex;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Tests for {@link IntermediateResultPartition}. */
public class IntermediateResultPartitionTest extends TestLogger {

    @Test
    public void testPipelinedPartitionConsumable() throws Exception {
        IntermediateResult result = createResult(ResultPartitionType.PIPELINED, 2);
        IntermediateResultPartition partition1 = result.getPartitions()[0];
        IntermediateResultPartition partition2 = result.getPartitions()[1];

        // Not consumable on init
        assertFalse(partition1.isConsumable());
        assertFalse(partition2.isConsumable());

        // Partition 1 consumable after data are produced
        partition1.markDataProduced();
        assertTrue(partition1.isConsumable());
        assertFalse(partition2.isConsumable());

        // Not consumable if failover happens
        result.resetForNewExecution();
        assertFalse(partition1.isConsumable());
        assertFalse(partition2.isConsumable());
    }

    @Test
    public void testBlockingPartitionConsumable() throws Exception {
        IntermediateResult result = createResult(ResultPartitionType.BLOCKING, 2);
        IntermediateResultPartition partition1 = result.getPartitions()[0];
        IntermediateResultPartition partition2 = result.getPartitions()[1];

        // Not consumable on init
        assertFalse(partition1.isConsumable());
        assertFalse(partition2.isConsumable());

        // Not consumable if only one partition is FINISHED
        partition1.markFinished();
        assertFalse(partition1.isConsumable());
        assertFalse(partition2.isConsumable());
        assertFalse(result.areAllPartitionsFinished());

        // Consumable after all partitions are FINISHED
        partition2.markFinished();
        assertTrue(partition1.isConsumable());
        assertTrue(partition2.isConsumable());
        assertTrue(result.areAllPartitionsFinished());

        // Not consumable if failover happens
        result.resetForNewExecution();
        assertFalse(partition1.isConsumable());
        assertFalse(partition2.isConsumable());
        assertFalse(result.areAllPartitionsFinished());
    }

    @Test
    public void testBlockingPartitionResetting() throws Exception {
        IntermediateResult result = createResult(ResultPartitionType.BLOCKING, 2);
        IntermediateResultPartition partition1 = result.getPartitions()[0];
        IntermediateResultPartition partition2 = result.getPartitions()[1];

        // Not consumable on init
        assertFalse(partition1.isConsumable());
        assertFalse(partition2.isConsumable());

        // Not consumable if partition1 is FINISHED
        partition1.markFinished();
        assertEquals(1, result.getNumberOfRunningProducers());
        assertFalse(partition1.isConsumable());
        assertFalse(partition2.isConsumable());
        assertFalse(result.areAllPartitionsFinished());

        // Reset the result and mark partition2 FINISHED, the result should still not be consumable
        result.resetForNewExecution();
        assertEquals(2, result.getNumberOfRunningProducers());
        partition2.markFinished();
        assertEquals(1, result.getNumberOfRunningProducers());
        assertFalse(partition1.isConsumable());
        assertFalse(partition2.isConsumable());
        assertFalse(result.areAllPartitionsFinished());

        // Consumable after all partitions are FINISHED
        partition1.markFinished();
        assertEquals(0, result.getNumberOfRunningProducers());
        assertTrue(partition1.isConsumable());
        assertTrue(partition2.isConsumable());
        assertTrue(result.areAllPartitionsFinished());

        // Not consumable again if failover happens
        result.resetForNewExecution();
        assertEquals(2, result.getNumberOfRunningProducers());
        assertFalse(partition1.isConsumable());
        assertFalse(partition2.isConsumable());
        assertFalse(result.areAllPartitionsFinished());
    }

    private static IntermediateResult createResult(
            ResultPartitionType resultPartitionType, int producerCount) throws Exception {

        JobVertex jobVertex = new JobVertex("v1");
        jobVertex.setInvokableClass(NoOpInvokable.class);
        jobVertex.setParallelism(producerCount);
        jobVertex.createAndAddResultDataSet(resultPartitionType);

        ExecutionJobVertex ejv =
                getExecutionJobVertex(
                        jobVertex,
                        new DirectScheduledExecutorService(),
                        ScheduleMode.LAZY_FROM_SOURCES);
        IntermediateResult result = ejv.getProducedDataSets()[0];

        return result;
    }
}
