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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.taskexecutor.partition.ClusterPartitionReport;

import java.util.Collection;
import java.util.Collections;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

/** Test {@link TaskExecutorPartitionTracker} implementation. */
public class TestingTaskExecutorPartitionTracker implements TaskExecutorPartitionTracker {

    private Function<JobID, Boolean> isTrackingPartitionsForFunction = ignored -> false;
    private Function<ResultPartitionID, Boolean> isPartitionTrackedFunction = ignored -> false;
    private Function<JobID, Collection<PartitionTrackerEntry<JobID, TaskExecutorPartitionInfo>>>
            stopTrackingAllPartitionsFunction = ignored -> Collections.emptySet();
    private Consumer<JobID> stopTrackingAndReleaseAllPartitionsConsumer = ignored -> {};
    private BiConsumer<JobID, TaskExecutorPartitionInfo> startTrackingPartitionsConsumer =
            (ignoredA, ignoredB) -> {};
    private Consumer<Collection<ResultPartitionID>> stopTrackingAndReleasePartitionsConsumer =
            ignored -> {};
    private Function<
                    Collection<ResultPartitionID>,
                    Collection<PartitionTrackerEntry<JobID, TaskExecutorPartitionInfo>>>
            stopTrackingPartitionsFunction = ignored -> Collections.emptySet();
    private Runnable stopTrackingAllClusterPartitionsRunnable = () -> {};
    private Consumer<Collection<ResultPartitionID>> promotePartitionsConsumer = ignored -> {};
    private Consumer<Collection<IntermediateDataSetID>> releaseClusterPartitionsConsumer =
            ignored -> {};

    public void setStartTrackingPartitionsConsumer(
            BiConsumer<JobID, TaskExecutorPartitionInfo> startTrackingPartitionsConsumer) {
        this.startTrackingPartitionsConsumer = startTrackingPartitionsConsumer;
    }

    public void setIsTrackingPartitionsForFunction(
            Function<JobID, Boolean> isTrackingPartitionsForFunction) {
        this.isTrackingPartitionsForFunction = isTrackingPartitionsForFunction;
    }

    public void setIsPartitionTrackedFunction(
            Function<ResultPartitionID, Boolean> isPartitionTrackedFunction) {
        this.isPartitionTrackedFunction = isPartitionTrackedFunction;
    }

    public void setStopTrackingAllPartitionsFunction(
            Function<JobID, Collection<PartitionTrackerEntry<JobID, TaskExecutorPartitionInfo>>>
                    stopTrackingAllPartitionsFunction) {
        this.stopTrackingAllPartitionsFunction = stopTrackingAllPartitionsFunction;
    }

    public void setPromotePartitionsConsumer(
            Consumer<Collection<ResultPartitionID>> promotePartitionsConsumer) {
        this.promotePartitionsConsumer = promotePartitionsConsumer;
    }

    public void setStopTrackingAndReleaseAllPartitionsConsumer(
            Consumer<JobID> stopTrackingAndReleaseAllPartitionsConsumer) {
        this.stopTrackingAndReleaseAllPartitionsConsumer =
                stopTrackingAndReleaseAllPartitionsConsumer;
    }

    public void setStopTrackingAndReleasePartitionsConsumer(
            Consumer<Collection<ResultPartitionID>> stopTrackingAndReleasePartitionsConsumer) {
        this.stopTrackingAndReleasePartitionsConsumer = stopTrackingAndReleasePartitionsConsumer;
    }

    public void setStopTrackingPartitionsFunction(
            Function<
                            Collection<ResultPartitionID>,
                            Collection<PartitionTrackerEntry<JobID, TaskExecutorPartitionInfo>>>
                    stopTrackingPartitionsFunction) {
        this.stopTrackingPartitionsFunction = stopTrackingPartitionsFunction;
    }

    public void setReleaseClusterPartitionsConsumer(
            Consumer<Collection<IntermediateDataSetID>> releaseClusterPartitionsConsumer) {
        this.releaseClusterPartitionsConsumer = releaseClusterPartitionsConsumer;
    }

    @Override
    public void startTrackingPartition(
            JobID producingJobId, TaskExecutorPartitionInfo partitionInfo) {
        startTrackingPartitionsConsumer.accept(producingJobId, partitionInfo);
    }

    @Override
    public void stopTrackingAndReleaseJobPartitions(
            Collection<ResultPartitionID> resultPartitionIds) {
        stopTrackingAndReleasePartitionsConsumer.accept(resultPartitionIds);
    }

    @Override
    public void stopTrackingAndReleaseJobPartitionsFor(JobID producingJobId) {
        stopTrackingAndReleaseAllPartitionsConsumer.accept(producingJobId);
    }

    @Override
    public void promoteJobPartitions(Collection<ResultPartitionID> partitionsToPromote) {
        promotePartitionsConsumer.accept(partitionsToPromote);
    }

    @Override
    public void stopTrackingAndReleaseClusterPartitions(
            Collection<IntermediateDataSetID> dataSetsToRelease) {
        releaseClusterPartitionsConsumer.accept(dataSetsToRelease);
    }

    @Override
    public void stopTrackingAndReleaseAllClusterPartitions() {
        stopTrackingAllClusterPartitionsRunnable.run();
    }

    @Override
    public ClusterPartitionReport createClusterPartitionReport() {
        return new ClusterPartitionReport(Collections.emptyList());
    }

    @Override
    public Collection<PartitionTrackerEntry<JobID, TaskExecutorPartitionInfo>>
            stopTrackingPartitionsFor(JobID key) {
        return stopTrackingAllPartitionsFunction.apply(key);
    }

    @Override
    public Collection<PartitionTrackerEntry<JobID, TaskExecutorPartitionInfo>>
            stopTrackingPartitions(Collection<ResultPartitionID> resultPartitionIds) {
        return stopTrackingPartitionsFunction.apply(resultPartitionIds);
    }

    @Override
    public boolean isTrackingPartitionsFor(JobID key) {
        return isTrackingPartitionsForFunction.apply(key);
    }

    @Override
    public boolean isPartitionTracked(ResultPartitionID resultPartitionID) {
        return isPartitionTrackedFunction.apply(resultPartitionID);
    }
}
