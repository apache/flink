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
 * limitations under the License
 */

package org.apache.flink.runtime.scheduler.benchmark;

import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.configuration.JobManagerOptions.HybridPartitionDataConsumeConstraint;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobType;

/**
 * {@link JobConfiguration} contains the configuration of a STREAMING/BATCH job. It concludes {@link
 * DistributionPattern}, {@link ResultPartitionType}, {@link JobType}, {@link ExecutionMode}, {@link
 * HybridPartitionDataConsumeConstraint}.
 */
public enum JobConfiguration {
    STREAMING(
            DistributionPattern.ALL_TO_ALL,
            ResultPartitionType.PIPELINED,
            JobType.STREAMING,
            ExecutionMode.PIPELINED,
            4000,
            false),

    BATCH(
            DistributionPattern.ALL_TO_ALL,
            ResultPartitionType.BLOCKING,
            JobType.BATCH,
            ExecutionMode.BATCH,
            4000,
            false),

    BATCH_HYBRID_DEFAULT(
            DistributionPattern.ALL_TO_ALL,
            ResultPartitionType.HYBRID_FULL,
            JobType.BATCH,
            ExecutionMode.BATCH,
            HybridPartitionDataConsumeConstraint.UNFINISHED_PRODUCERS,
            4000,
            false),

    BATCH_HYBRID_PARTIAL_FINISHED(
            DistributionPattern.ALL_TO_ALL,
            ResultPartitionType.HYBRID_FULL,
            JobType.BATCH,
            ExecutionMode.BATCH,
            HybridPartitionDataConsumeConstraint.ONLY_FINISHED_PRODUCERS,
            4000,
            false),

    BATCH_HYBRID_ALL_FINISHED(
            DistributionPattern.ALL_TO_ALL,
            ResultPartitionType.HYBRID_FULL,
            JobType.BATCH,
            ExecutionMode.BATCH,
            HybridPartitionDataConsumeConstraint.ALL_PRODUCERS_FINISHED,
            4000,
            false),

    STREAMING_TEST(
            DistributionPattern.ALL_TO_ALL,
            ResultPartitionType.PIPELINED,
            JobType.STREAMING,
            ExecutionMode.PIPELINED,
            10,
            false),

    BATCH_TEST(
            DistributionPattern.ALL_TO_ALL,
            ResultPartitionType.BLOCKING,
            JobType.BATCH,
            ExecutionMode.BATCH,
            10,
            false),

    BATCH_HYBRID_DEFAULT_TEST(
            DistributionPattern.ALL_TO_ALL,
            ResultPartitionType.HYBRID_FULL,
            JobType.BATCH,
            ExecutionMode.BATCH,
            HybridPartitionDataConsumeConstraint.UNFINISHED_PRODUCERS,
            10,
            false),

    BATCH_HYBRID_PARTIAL_FINISHED_TEST(
            DistributionPattern.ALL_TO_ALL,
            ResultPartitionType.HYBRID_FULL,
            JobType.BATCH,
            ExecutionMode.BATCH,
            HybridPartitionDataConsumeConstraint.ONLY_FINISHED_PRODUCERS,
            10,
            false),

    BATCH_HYBRID_ALL_FINISHED_TEST(
            DistributionPattern.ALL_TO_ALL,
            ResultPartitionType.HYBRID_FULL,
            JobType.BATCH,
            ExecutionMode.BATCH,
            HybridPartitionDataConsumeConstraint.ALL_PRODUCERS_FINISHED,
            10,
            false),

    STREAMING_EVENLY(
            DistributionPattern.ALL_TO_ALL,
            ResultPartitionType.PIPELINED,
            JobType.STREAMING,
            ExecutionMode.PIPELINED,
            4000,
            true),

    BATCH_EVENLY(
            DistributionPattern.ALL_TO_ALL,
            ResultPartitionType.BLOCKING,
            JobType.BATCH,
            ExecutionMode.BATCH,
            4000,
            true),

    STREAMING_EVENLY_TEST(
            DistributionPattern.ALL_TO_ALL,
            ResultPartitionType.PIPELINED,
            JobType.STREAMING,
            ExecutionMode.PIPELINED,
            10,
            true),

    BATCH_EVENLY_TEST(
            DistributionPattern.ALL_TO_ALL,
            ResultPartitionType.BLOCKING,
            JobType.BATCH,
            ExecutionMode.BATCH,
            10,
            true);

    private final int parallelism;
    private final DistributionPattern distributionPattern;
    private final ResultPartitionType resultPartitionType;
    private final JobType jobType;
    private final ExecutionMode executionMode;
    private final boolean evenlySpreadOutSlots;
    private final HybridPartitionDataConsumeConstraint hybridPartitionDataConsumeConstraint;

    JobConfiguration(
            DistributionPattern distributionPattern,
            ResultPartitionType resultPartitionType,
            JobType jobType,
            ExecutionMode executionMode,
            int parallelism,
            boolean evenlySpreadOutSlots) {
        this(
                distributionPattern,
                resultPartitionType,
                jobType,
                executionMode,
                HybridPartitionDataConsumeConstraint.UNFINISHED_PRODUCERS,
                parallelism,
                evenlySpreadOutSlots);
    }

    JobConfiguration(
            DistributionPattern distributionPattern,
            ResultPartitionType resultPartitionType,
            JobType jobType,
            ExecutionMode executionMode,
            HybridPartitionDataConsumeConstraint hybridPartitionDataConsumeConstraint,
            int parallelism,
            boolean evenlySpreadOutSlots) {
        this.distributionPattern = distributionPattern;
        this.resultPartitionType = resultPartitionType;
        this.jobType = jobType;
        this.executionMode = executionMode;
        this.hybridPartitionDataConsumeConstraint = hybridPartitionDataConsumeConstraint;
        this.parallelism = parallelism;
        this.evenlySpreadOutSlots = evenlySpreadOutSlots;
    }

    public int getParallelism() {
        return parallelism;
    }

    public DistributionPattern getDistributionPattern() {
        return distributionPattern;
    }

    public ResultPartitionType getResultPartitionType() {
        return resultPartitionType;
    }

    public JobType getJobType() {
        return jobType;
    }

    public ExecutionMode getExecutionMode() {
        return executionMode;
    }

    public HybridPartitionDataConsumeConstraint getHybridPartitionDataConsumeConstraint() {
        return hybridPartitionDataConsumeConstraint;
    }

    public boolean isEvenlySpreadOutSlots() {
        return evenlySpreadOutSlots;
    }
}
