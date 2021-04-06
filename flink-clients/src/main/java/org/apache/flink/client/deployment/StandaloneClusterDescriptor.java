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
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

/** A deployment descriptor for an existing cluster. */
public class StandaloneClusterDescriptor implements ClusterDescriptor<StandaloneClusterId> {

    private final Configuration config;

    public StandaloneClusterDescriptor(Configuration config) {
        this.config = Preconditions.checkNotNull(config);
    }

    @Override
    public String getClusterDescription() {
        String host = config.getString(JobManagerOptions.ADDRESS, "");
        int port = config.getInteger(JobManagerOptions.PORT, -1);
        return "Standalone cluster at " + host + ":" + port;
    }

    @Override
    public ClusterClientProvider<StandaloneClusterId> retrieve(
            StandaloneClusterId standaloneClusterId) throws ClusterRetrieveException {
        return () -> {
            try {
                return new RestClusterClient<>(config, standaloneClusterId);
            } catch (Exception e) {
                throw new RuntimeException("Couldn't retrieve standalone cluster", e);
            }
        };
    }

    @Override
    public ClusterClientProvider<StandaloneClusterId> deploySessionCluster(
            ClusterSpecification clusterSpecification) {
        throw new UnsupportedOperationException("Can't deploy a standalone cluster.");
    }

    @Override
    public ClusterClientProvider<StandaloneClusterId> deployApplicationCluster(
            final ClusterSpecification clusterSpecification,
            final ApplicationConfiguration applicationConfiguration) {
        throw new UnsupportedOperationException(
                "Application Mode not supported by standalone deployments.");
    }

    @Override
    public ClusterClientProvider<StandaloneClusterId> deployJobCluster(
            ClusterSpecification clusterSpecification, JobGraph jobGraph, boolean detached) {
        throw new UnsupportedOperationException(
                "Per-Job Mode not supported by standalone deployments.");
    }

    @Override
    public void killCluster(StandaloneClusterId clusterId) throws FlinkException {
        throw new UnsupportedOperationException("Cannot terminate a standalone cluster.");
    }

    @Override
    public void close() {
        // nothing to do
    }
}
