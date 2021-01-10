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

package org.apache.flink.client.deployment.application.cli;

import org.apache.flink.annotation.Internal;
import org.apache.flink.client.cli.ApplicationDeployer;
import org.apache.flink.client.deployment.ClusterClientFactory;
import org.apache.flink.client.deployment.ClusterClientServiceLoader;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.configuration.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An entity responsible for submitting an application for execution in "Application Mode", i.e. on
 * a dedicated cluster that is created on application submission and torn down upon application
 * termination, and with its {@code main()} executed on the cluster, rather than the client.
 */
@Internal
public class ApplicationClusterDeployer implements ApplicationDeployer {

    private static final Logger LOG = LoggerFactory.getLogger(ApplicationClusterDeployer.class);

    private final ClusterClientServiceLoader clientServiceLoader;

    public ApplicationClusterDeployer(final ClusterClientServiceLoader clientServiceLoader) {
        this.clientServiceLoader = checkNotNull(clientServiceLoader);
    }

    public <ClusterID> void run(
            final Configuration configuration,
            final ApplicationConfiguration applicationConfiguration)
            throws Exception {
        checkNotNull(configuration);
        checkNotNull(applicationConfiguration);

        LOG.info("Submitting application in 'Application Mode'.");

        final ClusterClientFactory<ClusterID> clientFactory =
                clientServiceLoader.getClusterClientFactory(configuration);
        try (final ClusterDescriptor<ClusterID> clusterDescriptor =
                clientFactory.createClusterDescriptor(configuration)) {
            final ClusterSpecification clusterSpecification =
                    clientFactory.getClusterSpecification(configuration);

            clusterDescriptor.deployApplicationCluster(
                    clusterSpecification, applicationConfiguration);
        }
    }
}
