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
import org.apache.flink.runtime.leaderelection.LeaderElectionDriverFactory;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalDriverFactory;

/**
 * Generator for the materials used by the {@link
 * org.apache.flink.runtime.leaderservice.DefaultLeaderServices}. Including the leader path of each
 * component, {@link LeaderElectionDriverFactory} and {@link LeaderRetrievalDriverFactory} in the
 * specific high availability scenario, e.g. Kubernetes or Zookeeper. Please return a proper leader
 * name in the implementation of {@link #getLeaderPathForResourceManager}, {@link
 * #getLeaderPathForDispatcher}, {@link #getLeaderPathForJobManager}, {@link
 * #getLeaderPathForRestServer}. The returned leader name is the ConfigMap name in Kubernetes and
 * child path in Zookeeper.
 */
public interface LeaderServiceMaterialGenerator {
    /**
     * Get the leader path for ResourceManager.
     *
     * @return Return the ResourceManager leader name. It is ConfigMap name in Kubernetes or child
     *     node path in Zookeeper.
     */
    String getLeaderPathForResourceManager();

    /**
     * Get the leader path for Dispatcher.
     *
     * @return Return the Dispatcher leader name. It is ConfigMap name in Kubernetes or child node
     *     path in Zookeeper.
     */
    String getLeaderPathForDispatcher();

    /**
     * Get the leader path for specific JobManager.
     *
     * @param jobID job id
     * @return Return the JobManager leader name for specified job id. It is ConfigMap name in
     *     Kubernetes or child node path in Zookeeper.
     */
    String getLeaderPathForJobManager(final JobID jobID);

    /**
     * Get the leader path for RestServer.
     *
     * @return Return the RestServer leader name. It is ConfigMap name in Kubernetes or child node
     *     path in Zookeeper.
     */
    String getLeaderPathForRestServer();

    LeaderElectionDriverFactory createLeaderElectionDriverFactory() throws Exception;

    LeaderRetrievalDriverFactory createLeaderRetrievalDriverFactory(String componentId);

    /**
     * Closes the components which is used for external operations(e.g. Zookeeper Client, Kubernetes
     * Client).
     *
     * @throws Exception if the close operation failed
     */
    void closeServices() throws Exception;

    /**
     * Clean up the meta data in the distributed system(e.g. Zookeeper, Kubernetes ConfigMap).
     *
     * <p>If an exception occurs during internal cleanup, we will continue the cleanup and report
     * exceptions only after all cleanup steps have been attempted.
     *
     * @throws Exception when do the cleanup operation on external storage.
     */
    void cleanupServices() throws Exception;

    /**
     * Clean up the meta data in the distributed system(e.g. Zookeeper, Kubernetes ConfigMap) for
     * the specified Job. Method implementations need to be thread-safe.
     *
     * @param jobID The identifier of the job to cleanup.
     * @throws Exception when do the cleanup operation on external storage.
     */
    void cleanupJobData(JobID jobID) throws Exception;
}
