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

package org.apache.flink.client.deployment;

import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.util.FlinkException;

/**
 * A descriptor to deploy a cluster (e.g. Yarn) and return a Client for Cluster communication.
 *
 * @param <T> Type of the cluster id
 */
public interface ClusterDescriptor<T> extends AutoCloseable {

    /**
     * Returns a String containing details about the cluster (NodeManagers, available memory, ...).
     */
    String getClusterDescription();

    /**
     * Retrieves an existing Flink Cluster.
     *
     * @param clusterId The unique identifier of the running cluster
     * @return Client for the cluster
     * @throws ClusterRetrieveException if the cluster client could not be retrieved
     */
    ClusterClientProvider<T> retrieve(T clusterId) throws ClusterRetrieveException;

    /**
     * Triggers deployment of a cluster.
     *
     * @param clusterSpecification Cluster specification defining the cluster to deploy
     * @return Client for the cluster
     * @throws ClusterDeploymentException if the cluster could not be deployed
     */
    ClusterClientProvider<T> deploySessionCluster(ClusterSpecification clusterSpecification)
            throws ClusterDeploymentException;

    /**
     * Triggers deployment of an application cluster. This corresponds to a cluster dedicated to the
     * execution of a predefined application. The cluster will be created on application submission
     * and torn down upon application termination. In addition, the {@code main()} of the
     * application's user code will be executed on the cluster, rather than the client.
     *
     * @param clusterSpecification Cluster specification defining the cluster to deploy
     * @param applicationConfiguration Application-specific configuration parameters
     * @return Client for the cluster
     * @throws ClusterDeploymentException if the cluster could not be deployed
     */
    ClusterClientProvider<T> deployApplicationCluster(
            final ClusterSpecification clusterSpecification,
            final ApplicationConfiguration applicationConfiguration)
            throws ClusterDeploymentException;

    /**
     * Deploys a per-job cluster with the given job on the cluster.
     *
     * @param clusterSpecification Initial cluster specification with which the Flink cluster is
     *     launched
     * @param jobGraph JobGraph with which the job cluster is started
     * @param detached true if the cluster should be stopped after the job completion without
     *     serving the result, otherwise false
     * @return Cluster client to talk to the Flink cluster
     * @throws ClusterDeploymentException if the cluster could not be deployed
     */
    ClusterClientProvider<T> deployJobCluster(
            final ClusterSpecification clusterSpecification,
            final JobGraph jobGraph,
            final boolean detached)
            throws ClusterDeploymentException;

    /**
     * Terminates the cluster with the given cluster id.
     *
     * @param clusterId identifying the cluster to shut down
     * @throws FlinkException if the cluster could not be terminated
     */
    void killCluster(T clusterId) throws FlinkException;

    @Override
    void close();
}
