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

package org.apache.flink.kubernetes;

import org.apache.flink.annotation.Internal;
import org.apache.flink.client.deployment.AbstractContainerizedClusterClientFactory;
import org.apache.flink.client.deployment.ClusterClientFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.KubernetesDeploymentTarget;
import org.apache.flink.kubernetes.kubeclient.DefaultKubeClientFactory;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.util.AbstractID;

import javax.annotation.Nullable;

import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A {@link ClusterClientFactory} for a Kubernetes cluster. */
@Internal
public class KubernetesClusterClientFactory
        extends AbstractContainerizedClusterClientFactory<String> {

    private static final String CLUSTER_ID_PREFIX = "flink-cluster-";

    @Override
    public boolean isCompatibleWith(Configuration configuration) {
        checkNotNull(configuration);
        final String deploymentTarget = configuration.getString(DeploymentOptions.TARGET);
        return KubernetesDeploymentTarget.isValidKubernetesTarget(deploymentTarget);
    }

    @Override
    public KubernetesClusterDescriptor createClusterDescriptor(Configuration configuration) {
        checkNotNull(configuration);
        if (!configuration.contains(KubernetesConfigOptions.CLUSTER_ID)) {
            final String clusterId = generateClusterId();
            configuration.setString(KubernetesConfigOptions.CLUSTER_ID, clusterId);
        }
        return new KubernetesClusterDescriptor(
                configuration,
                DefaultKubeClientFactory.getInstance().fromConfiguration(configuration));
    }

    @Nullable
    @Override
    public String getClusterId(Configuration configuration) {
        checkNotNull(configuration);
        return configuration.getString(KubernetesConfigOptions.CLUSTER_ID);
    }

    @Override
    public Optional<String> getApplicationTargetName() {
        return Optional.of(KubernetesDeploymentTarget.APPLICATION.getName());
    }

    private String generateClusterId() {
        final String randomID = new AbstractID().toString();
        return (CLUSTER_ID_PREFIX + randomID)
                .substring(0, Constants.MAXIMUM_CHARACTERS_OF_CLUSTER_ID);
    }
}
