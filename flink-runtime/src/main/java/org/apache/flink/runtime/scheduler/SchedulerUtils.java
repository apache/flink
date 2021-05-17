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
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.DeactivatedCheckpointCompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.DeactivatedCheckpointIDCounter;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.executiongraph.DefaultExecutionGraphBuilder;
import org.apache.flink.runtime.jobgraph.JobGraph;

import org.slf4j.Logger;

/** Utils class for Flink's scheduler implementations. */
public final class SchedulerUtils {

    private SchedulerUtils() {
        throw new UnsupportedOperationException(
                "Instantiation of SchedulerUtils is not supported.");
    }

    public static CompletedCheckpointStore createCompletedCheckpointStoreIfCheckpointingIsEnabled(
            JobGraph jobGraph,
            Configuration configuration,
            ClassLoader userCodeLoader,
            CheckpointRecoveryFactory checkpointRecoveryFactory,
            Logger log)
            throws JobExecutionException {
        final JobID jobId = jobGraph.getJobID();
        if (DefaultExecutionGraphBuilder.isCheckpointingEnabled(jobGraph)) {
            try {
                return createCompletedCheckpointStore(
                        configuration, userCodeLoader, checkpointRecoveryFactory, log, jobId);
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
            ClassLoader classLoader,
            CheckpointRecoveryFactory recoveryFactory,
            Logger log,
            JobID jobId)
            throws Exception {
        int maxNumberOfCheckpointsToRetain =
                jobManagerConfig.getInteger(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS);

        if (maxNumberOfCheckpointsToRetain <= 0) {
            // warning and use 1 as the default value if the setting in
            // state.checkpoints.max-retained-checkpoints is not greater than 0.
            log.warn(
                    "The setting for '{} : {}' is invalid. Using default value of {}",
                    CheckpointingOptions.MAX_RETAINED_CHECKPOINTS.key(),
                    maxNumberOfCheckpointsToRetain,
                    CheckpointingOptions.MAX_RETAINED_CHECKPOINTS.defaultValue());

            maxNumberOfCheckpointsToRetain =
                    CheckpointingOptions.MAX_RETAINED_CHECKPOINTS.defaultValue();
        }

        return recoveryFactory.createCheckpointStore(
                jobId, maxNumberOfCheckpointsToRetain, classLoader);
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
