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

package org.apache.flink.runtime.leaderservice;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.leaderelection.DefaultLeaderElectionService;
import org.apache.flink.runtime.leaderelection.LeaderElection;
import org.apache.flink.runtime.leaderelection.StandaloneLeaderElection;
import org.apache.flink.runtime.leaderretrieval.DefaultLeaderRetrievalService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.leaderretrieval.StandaloneLeaderRetrievalService;
import org.apache.flink.util.ExceptionUtils;

import static org.apache.flink.runtime.highavailability.HighAvailabilityServices.DEFAULT_LEADER_ID;

/**
 * Default leader services based on distributed system(e.g. Zookeeper, Kubernetes). It will help
 * with creating all the leader election/retrieval services.
 */
public class DefaultLeaderServices implements LeaderServices {
    private final LeaderServiceMaterialGenerator leaderServiceMaterialGenerator;
    private final DefaultLeaderElectionService leaderElectionService;
    private final boolean recoverJobs;

    public DefaultLeaderServices(
            LeaderServiceMaterialGenerator leaderServiceMaterialGenerator, boolean recoverJobs)
            throws Exception {
        this.leaderServiceMaterialGenerator = leaderServiceMaterialGenerator;
        this.leaderElectionService =
                new DefaultLeaderElectionService(
                        leaderServiceMaterialGenerator.createLeaderElectionDriverFactory());
        this.recoverJobs = recoverJobs;
    }

    @Override
    public LeaderRetrievalService getRestEndpointLeaderRetriever() {
        return new DefaultLeaderRetrievalService(
                leaderServiceMaterialGenerator.createLeaderRetrievalDriverFactory(
                        leaderServiceMaterialGenerator.getLeaderPathForRestServer()));
    }

    @Override
    public LeaderElection getResourceManagerLeaderElection() {
        return leaderElectionService.createLeaderElection(
                leaderServiceMaterialGenerator.getLeaderPathForResourceManager());
    }

    @Override
    public LeaderRetrievalService getResourceManagerLeaderRetriever() {
        return new DefaultLeaderRetrievalService(
                leaderServiceMaterialGenerator.createLeaderRetrievalDriverFactory(
                        leaderServiceMaterialGenerator.getLeaderPathForResourceManager()));
    }

    @Override
    public LeaderElection getDispatcherLeaderElection() {
        return leaderElectionService.createLeaderElection(
                leaderServiceMaterialGenerator.getLeaderPathForDispatcher());
    }

    @Override
    public LeaderRetrievalService getDispatcherLeaderRetriever() {
        return new DefaultLeaderRetrievalService(
                leaderServiceMaterialGenerator.createLeaderRetrievalDriverFactory(
                        leaderServiceMaterialGenerator.getLeaderPathForDispatcher()));
    }

    @Override
    public LeaderRetrievalService getJobMasterLeaderRetriever(
            JobID jobID, String defaultJobManagerAddress) {
        if (recoverJobs) {
            return new DefaultLeaderRetrievalService(
                    leaderServiceMaterialGenerator.createLeaderRetrievalDriverFactory(
                            leaderServiceMaterialGenerator.getLeaderPathForJobManager(jobID)));
        } else {
            return new StandaloneLeaderRetrievalService(
                    defaultJobManagerAddress, DEFAULT_LEADER_ID);
        }
    }

    @Override
    public LeaderElection getJobMasterLeaderElection(JobID jobID) {
        if (recoverJobs) {
            return leaderElectionService.createLeaderElection(
                    leaderServiceMaterialGenerator.getLeaderPathForJobManager(jobID));
        } else {
            return new StandaloneLeaderElection(DEFAULT_LEADER_ID);
        }
    }

    @Override
    public LeaderElection getRestEndpointLeaderElection() {
        return leaderElectionService.createLeaderElection(
                leaderServiceMaterialGenerator.getLeaderPathForRestServer());
    }

    @Override
    public void close() throws Exception {
        Throwable exception = null;

        try {
            leaderElectionService.close();
        } catch (Throwable t) {
            exception = ExceptionUtils.firstOrSuppressed(t, exception);
        }

        try {
            leaderServiceMaterialGenerator.closeServices();
        } catch (Throwable t) {
            exception = ExceptionUtils.firstOrSuppressed(t, exception);
        }

        if (exception != null) {
            ExceptionUtils.rethrowException(
                    exception, "Could not properly close the " + getClass().getSimpleName());
        }
    }

    @Override
    public void cleanupJobData(JobID jobID) throws Exception {
        leaderServiceMaterialGenerator.cleanupJobData(jobID);
    }

    @Override
    public void cleanupServices() throws Exception {
        leaderServiceMaterialGenerator.cleanupServices();
    }
}
