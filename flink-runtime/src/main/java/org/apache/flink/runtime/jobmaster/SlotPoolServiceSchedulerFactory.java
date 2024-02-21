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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.core.failure.FailureEnricher;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.blocklist.BlocklistOperations;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.executiongraph.JobStatusListener;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.slotpool.DeclarativeSlotPoolFactory;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPoolService;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.scheduler.SchedulerNG;
import org.apache.flink.runtime.shuffle.ShuffleMaster;

import org.slf4j.Logger;

import java.util.Collection;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

/** Factory for {@link SlotPoolService} and {@link SchedulerNG}. */
public interface SlotPoolServiceSchedulerFactory {

    /**
     * Creates a {@link SlotPoolService}.
     *
     * @param jid jid is the JobID to pass to the service
     * @param declarativeSlotPoolFactory the declarative slot pool factory
     * @return created SlotPoolService
     */
    SlotPoolService createSlotPoolService(
            JobID jid, DeclarativeSlotPoolFactory declarativeSlotPoolFactory);

    /**
     * Returns the scheduler type this factory is creating.
     *
     * @return the scheduler type this factory is creating.
     */
    JobManagerOptions.SchedulerType getSchedulerType();

    /**
     * Creates a {@link SchedulerNG}.
     *
     * @return created SchedulerNG
     * @throws Exception if the scheduler creation fails
     */
    SchedulerNG createScheduler(
            Logger log,
            JobGraph jobGraph,
            Executor ioExecutor,
            Configuration configuration,
            SlotPoolService slotPoolService,
            ScheduledExecutorService futureExecutor,
            ClassLoader userCodeLoader,
            CheckpointRecoveryFactory checkpointRecoveryFactory,
            Time rpcTimeout,
            BlobWriter blobWriter,
            JobManagerJobMetricGroup jobManagerJobMetricGroup,
            Time slotRequestTimeout,
            ShuffleMaster<?> shuffleMaster,
            JobMasterPartitionTracker partitionTracker,
            ExecutionDeploymentTracker executionDeploymentTracker,
            long initializationTimestamp,
            ComponentMainThreadExecutor mainThreadExecutor,
            FatalErrorHandler fatalErrorHandler,
            JobStatusListener jobStatusListener,
            Collection<FailureEnricher> failureEnrichers,
            BlocklistOperations blocklistOperations)
            throws Exception;
}
