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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.DeactivatedCheckpointCompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.DeactivatedCheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.DefaultCompletedCheckpointStoreUtils;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.executiongraph.DefaultExecutionGraphBuilder;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.state.SharedStateRegistry;

import org.slf4j.Logger;

import java.util.concurrent.Executor;

/** Utils class for Flink's scheduler implementations. */
public final class SchedulerUtils {

    private SchedulerUtils() {
        throw new UnsupportedOperationException(
                "Instantiation of SchedulerUtils is not supported.");
    }

    public static CompletedCheckpointStore createCompletedCheckpointStoreIfCheckpointingIsEnabled(
            JobGraph jobGraph,
            Configuration configuration,
            CheckpointRecoveryFactory checkpointRecoveryFactory,
            Executor ioExecutor,
            Logger log)
            throws JobExecutionException {
        final JobID jobId = jobGraph.getJobID();
        if (DefaultExecutionGraphBuilder.isCheckpointingEnabled(jobGraph)) {
            try {
                return createCompletedCheckpointStore(
                        configuration, checkpointRecoveryFactory, ioExecutor, log, jobId);
            } catch (Exception e) {
                throw new JobExecutionException(
                        jobId,
                        "Failed to initialize high-availability completed checkpoint store",
                        e);
            }
        } else {
            return DeactivatedCheckpointCompletedCheckpointStore.INSTANCE;
        }
    }

    @VisibleForTesting
    static CompletedCheckpointStore createCompletedCheckpointStore(
            Configuration jobManagerConfig,
            CheckpointRecoveryFactory recoveryFactory,
            Executor ioExecutor,
            Logger log,
            JobID jobId)
            throws Exception {
        return recoveryFactory.createRecoveredCompletedCheckpointStore(
                jobId,
                DefaultCompletedCheckpointStoreUtils.getMaximumNumberOfRetainedCheckpoints(
                        jobManagerConfig, log),
                SharedStateRegistry.DEFAULT_FACTORY,
                ioExecutor);
    }

    public static CheckpointIDCounter createCheckpointIDCounterIfCheckpointingIsEnabled(
            JobGraph jobGraph, CheckpointRecoveryFactory checkpointRecoveryFactory)
            throws JobExecutionException {
        final JobID jobId = jobGraph.getJobID();
        if (DefaultExecutionGraphBuilder.isCheckpointingEnabled(jobGraph)) {
            try {
                return createCheckpointIdCounter(checkpointRecoveryFactory, jobId);
            } catch (Exception e) {
                throw new JobExecutionException(
                        jobId, "Failed to initialize high-availability checkpoint id counter", e);
            }
        } else {
            return DeactivatedCheckpointIDCounter.INSTANCE;
        }
    }

    private static CheckpointIDCounter createCheckpointIdCounter(
            CheckpointRecoveryFactory recoveryFactory, JobID jobId) throws Exception {
        return recoveryFactory.createCheckpointIDCounter(jobId);
    }
}
