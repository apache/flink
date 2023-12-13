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

package org.apache.flink.runtime.highavailability.nonha.embedded;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.EmbeddedCompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.PerJobCheckpointRecoveryFactory;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesImpl;
import org.apache.flink.runtime.highavailability.JobResultStore;
import org.apache.flink.runtime.persistentservice.EmbeddedPersistentServices;
import org.apache.flink.runtime.state.SharedStateRegistry;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * {@link org.apache.flink.runtime.highavailability.HighAvailabilityServicesImpl} extension to
 * expose leadership granting and revoking.
 */
public class EmbeddedHaServicesWithLeadershipControl extends HighAvailabilityServicesImpl
        implements HaLeadershipControl {
    private final EmbeddedLeaderServices embeddedLeaderServices;

    public EmbeddedHaServicesWithLeadershipControl(Executor executor) {
        this(
                executor,
                new PerJobCheckpointRecoveryFactory<EmbeddedCompletedCheckpointStore>(
                        (maxCheckpoints,
                                previous,
                                stateRegistryFactory,
                                ioExecutor,
                                restoreMode) -> {
                            List<CompletedCheckpoint> checkpoints =
                                    previous != null
                                            ? previous.getAllCheckpoints()
                                            : Collections.emptyList();
                            SharedStateRegistry stateRegistry =
                                    stateRegistryFactory.create(
                                            ioExecutor, checkpoints, restoreMode);
                            if (previous != null) {
                                if (!previous.getShutdownStatus().isPresent()) {
                                    throw new IllegalStateException(
                                            "Completed checkpoint store from previous run has not yet shutdown.");
                                }
                                return new EmbeddedCompletedCheckpointStore(
                                        maxCheckpoints, checkpoints, stateRegistry);
                            }
                            return new EmbeddedCompletedCheckpointStore(
                                    maxCheckpoints, checkpoints, stateRegistry);
                        }));
    }

    @VisibleForTesting
    public EmbeddedHaServicesWithLeadershipControl(
            Executor executor, CheckpointRecoveryFactory checkpointRecoveryFactory) {
        super(
                new EmbeddedLeaderServices(executor),
                new EmbeddedPersistentServices(checkpointRecoveryFactory));
        this.embeddedLeaderServices = (EmbeddedLeaderServices) getLeaderServices();
    }

    @VisibleForTesting
    public EmbeddedHaServicesWithLeadershipControl(
            Executor executor, JobResultStore jobResultStore) {
        super(new EmbeddedLeaderServices(executor), new EmbeddedPersistentServices(jobResultStore));
        this.embeddedLeaderServices = (EmbeddedLeaderServices) getLeaderServices();
    }

    @Override
    public CompletableFuture<Void> revokeDispatcherLeadership() {
        final EmbeddedLeaderElectionService dispatcherLeaderService =
                embeddedLeaderServices.getDispatcherLeaderService();
        return dispatcherLeaderService.revokeLeadership();
    }

    @Override
    public CompletableFuture<Void> grantDispatcherLeadership() {
        final EmbeddedLeaderElectionService dispatcherLeaderService =
                embeddedLeaderServices.getDispatcherLeaderService();
        return dispatcherLeaderService.grantLeadership();
    }

    @Override
    public CompletableFuture<Void> revokeJobMasterLeadership(JobID jobId) {
        final EmbeddedLeaderElectionService jobMasterLeaderService =
                embeddedLeaderServices.getJobManagerLeaderService(jobId);
        return jobMasterLeaderService.revokeLeadership();
    }

    @Override
    public CompletableFuture<Void> grantJobMasterLeadership(JobID jobId) {
        final EmbeddedLeaderElectionService jobMasterLeaderService =
                embeddedLeaderServices.getJobManagerLeaderService(jobId);
        return jobMasterLeaderService.grantLeadership();
    }

    @Override
    public CompletableFuture<Void> revokeResourceManagerLeadership() {
        final EmbeddedLeaderElectionService resourceManagerLeaderService =
                embeddedLeaderServices.getResourceManagerLeaderService();
        return resourceManagerLeaderService.revokeLeadership();
    }

    @Override
    public CompletableFuture<Void> grantResourceManagerLeadership() {
        final EmbeddedLeaderElectionService resourceManagerLeaderService =
                embeddedLeaderServices.getResourceManagerLeaderService();
        return resourceManagerLeaderService.grantLeadership();
    }
}
