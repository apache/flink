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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.shuffle.PartitionDescriptorBuilder;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.Optional;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/** Tests for the {@link AbstractPartitionTracker}. */
public class AbstractPartitionTrackerTest extends TestLogger {

    @Test
    public void testStartStopTracking() {
        final TestPartitionTracker partitionTracker = new TestPartitionTracker();

        final ResourceID executorWithTrackedPartition = new ResourceID("tracked");
        final ResourceID executorWithoutTrackedPartition = new ResourceID("untracked");

        assertThat(
                partitionTracker.isTrackingPartitionsFor(executorWithTrackedPartition), is(false));
        assertThat(
                partitionTracker.isTrackingPartitionsFor(executorWithoutTrackedPartition),
                is(false));

        partitionTracker.startTrackingPartition(
                executorWithTrackedPartition, new ResultPartitionID());

        assertThat(
                partitionTracker.isTrackingPartitionsFor(executorWithTrackedPartition), is(true));
        assertThat(
                partitionTracker.isTrackingPartitionsFor(executorWithoutTrackedPartition),
                is(false));

        partitionTracker.stopTrackingPartitionsFor(executorWithTrackedPartition);

        assertThat(
                partitionTracker.isTrackingPartitionsFor(executorWithTrackedPartition), is(false));
        assertThat(
                partitionTracker.isTrackingPartitionsFor(executorWithoutTrackedPartition),
                is(false));
    }

    static ResultPartitionDeploymentDescriptor createResultPartitionDeploymentDescriptor(
            ResultPartitionID resultPartitionId, boolean hasLocalResources) {
        return createResultPartitionDeploymentDescriptor(
                resultPartitionId, ResultPartitionType.BLOCKING, hasLocalResources);
    }

    static ResultPartitionDeploymentDescriptor createResultPartitionDeploymentDescriptor(
            ResultPartitionID resultPartitionId,
            ResultPartitionType type,
            boolean hasLocalResources) {

        return new ResultPartitionDeploymentDescriptor(
                PartitionDescriptorBuilder.newBuilder()
                        .setPartitionId(resultPartitionId.getPartitionId())
                        .setPartitionType(type)
                        .build(),
                new ShuffleDescriptor() {
                    @Override
                    public ResultPartitionID getResultPartitionID() {
                        return resultPartitionId;
                    }

                    @Override
                    public Optional<ResourceID> storesLocalResourcesOn() {
                        return hasLocalResources
                                ? Optional.of(ResourceID.generate())
                                : Optional.empty();
                    }
                },
                1,
                true);
    }

    private static class TestPartitionTracker
            extends AbstractPartitionTracker<ResourceID, Integer> {

        public void startTrackingPartition(ResourceID key, ResultPartitionID resultPartitionID) {
            startTrackingPartition(key, resultPartitionID, 0);
        }
    }
}
