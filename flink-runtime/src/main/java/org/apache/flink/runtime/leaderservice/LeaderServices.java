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
import org.apache.flink.runtime.leaderelection.LeaderElection;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;

/** The LeaderServices give access to all services needed for leader retrieval and election. */
public interface LeaderServices extends ClientLeaderServices {
    /**
     * Gets the {@link org.apache.flink.runtime.leaderelection.LeaderElection} for the cluster's
     * resource manager.
     */
    LeaderElection getResourceManagerLeaderElection();

    /** Gets the leader retriever for the cluster's resource manager. */
    LeaderRetrievalService getResourceManagerLeaderRetriever();

    /** Gets the {@link LeaderElection} for the cluster's dispatcher. */
    LeaderElection getDispatcherLeaderElection();

    /**
     * Gets the leader retriever for the dispatcher. This leader retrieval service is not always
     * accessible.
     */
    LeaderRetrievalService getDispatcherLeaderRetriever();

    /**
     * Gets the leader retriever for the job JobMaster which is responsible for the given job.
     *
     * @param jobID The identifier of the job.
     * @param defaultJobManagerAddress JobManager address which will be returned by a static leader
     *     retrieval service.
     * @return Leader retrieval service to retrieve the job manager for the given job
     */
    LeaderRetrievalService getJobMasterLeaderRetriever(
            JobID jobID, String defaultJobManagerAddress);

    /** Gets the {@link LeaderElection} for the job with the given {@link JobID}. */
    LeaderElection getJobMasterLeaderElection(JobID jobID);

    /** Gets the {@link LeaderElection} for the cluster's rest endpoint. */
    LeaderElection getRestEndpointLeaderElection();

    /**
     * Clean up the meta data in the distributed system(e.g. Zookeeper, Kubernetes ConfigMap).
     *
     * <p>If an exception occurs during internal cleanup, we will continue the cleanup and report
     * exceptions only after all cleanup steps have been attempted.
     *
     * @throws Exception when do the cleanup operation on external storage.
     */
    default void cleanupServices() throws Exception {}

    /**
     * Clean up the meta data in the distributed system(e.g. Zookeeper, Kubernetes ConfigMap) for
     * the specified Job. Method implementations need to be thread-safe.
     *
     * @param jobID The identifier of the job to cleanup.
     * @throws Exception when do the cleanup operation on external storage.
     */
    default void cleanupJobData(JobID jobID) throws Exception {}
}
