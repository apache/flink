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

import org.apache.flink.client.deployment.ClusterClientFactory;
import org.apache.flink.client.deployment.ClusterClientServiceLoader;
import org.apache.flink.client.deployment.DefaultClusterClientServiceLoader;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.kubernetes.executors.KubernetesSessionClusterExecutor;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

/** Test for the {@link KubernetesClusterClientFactory} discovery. */
public class KubernetesClusterClientFactoryTest {

    @Test
    public void testKubernetesClusterClientFactoryDiscoveryWithSessionExecutor() {
        testKubernetesClusterClientFactoryDiscoveryHelper(KubernetesSessionClusterExecutor.NAME);
    }

    private void testKubernetesClusterClientFactoryDiscoveryHelper(final String targetName) {
        final Configuration configuration = new Configuration();
        configuration.setString(DeploymentOptions.TARGET, targetName);

        final ClusterClientServiceLoader serviceLoader = new DefaultClusterClientServiceLoader();
        final ClusterClientFactory<String> factory =
                serviceLoader.getClusterClientFactory(configuration);

        assertTrue(factory instanceof KubernetesClusterClientFactory);
    }
}
