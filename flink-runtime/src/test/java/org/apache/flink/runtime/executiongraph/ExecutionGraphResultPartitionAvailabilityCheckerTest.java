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

import org.apache.flink.runtime.executiongraph.failover.flip1.ResultPartitionAvailabilityChecker;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.TestingJobMasterPartitionTracker;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createExecutionAttemptId;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ExecutionGraphResultPartitionAvailabilityChecker}. */
class ExecutionGraphResultPartitionAvailabilityCheckerTest {

    @Test
    void testPartitionAvailabilityCheck() {

        final IntermediateResultPartitionID irp1ID = new IntermediateResultPartitionID();
        final IntermediateResultPartitionID irp2ID = new IntermediateResultPartitionID();
        final IntermediateResultPartitionID irp3ID = new IntermediateResultPartitionID();
        final IntermediateResultPartitionID irp4ID = new IntermediateResultPartitionID();

        final Map<IntermediateResultPartitionID, Boolean> expectedAvailability =
                new HashMap<IntermediateResultPartitionID, Boolean>() {
                    {
                        put(irp1ID, true);
                        put(irp2ID, false);
                        put(irp3ID, false);
                        put(irp4ID, true);
                    }
                };

        // let the partition tracker respect the expected availability result
        final TestingJobMasterPartitionTracker partitionTracker =
                new TestingJobMasterPartitionTracker();
        partitionTracker.setIsPartitionTrackedFunction(
                rpID -> expectedAvailability.get(rpID.getPartitionId()));

        // the execution attempt ID should make no difference in this case
        final Function<IntermediateResultPartitionID, ResultPartitionID> partitionIDMapper =
                intermediateResultPartitionID ->
                        new ResultPartitionID(
                                intermediateResultPartitionID, createExecutionAttemptId());

        final ResultPartitionAvailabilityChecker resultPartitionAvailabilityChecker =
                new ExecutionGraphResultPartitionAvailabilityChecker(
                        partitionIDMapper, partitionTracker);

        for (IntermediateResultPartitionID irpID : expectedAvailability.keySet()) {
            assertThat(expectedAvailability.get(irpID))
                    .isEqualTo(resultPartitionAvailabilityChecker.isAvailable(irpID));
        }
    }
}
