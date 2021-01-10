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

package org.apache.flink.kubernetes.entrypoint;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.KubernetesResourceManagerDriver;
import org.apache.flink.kubernetes.KubernetesWorkerNode;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.KubernetesResourceManagerDriverConfiguration;
import org.apache.flink.kubernetes.kubeclient.DefaultKubeClientFactory;
import org.apache.flink.runtime.resourcemanager.ResourceManagerRuntimeServicesConfiguration;
import org.apache.flink.runtime.resourcemanager.active.ActiveResourceManager;
import org.apache.flink.runtime.resourcemanager.active.ActiveResourceManagerFactory;
import org.apache.flink.runtime.resourcemanager.active.ResourceManagerDriver;
import org.apache.flink.util.ConfigurationException;

import javax.annotation.Nullable;

/**
 * {@link ActiveResourceManagerFactory} implementation which creates {@link ActiveResourceManager}
 * with {@link KubernetesResourceManagerDriver}.
 */
public class KubernetesResourceManagerFactory
        extends ActiveResourceManagerFactory<KubernetesWorkerNode> {

    private static final KubernetesResourceManagerFactory INSTANCE =
            new KubernetesResourceManagerFactory();

    private static final Time POD_CREATION_RETRY_INTERVAL = Time.seconds(3L);

    private KubernetesResourceManagerFactory() {}

    public static KubernetesResourceManagerFactory getInstance() {
        return INSTANCE;
    }

    @Override
    protected ResourceManagerDriver<KubernetesWorkerNode> createResourceManagerDriver(
            Configuration configuration, @Nullable String webInterfaceUrl, String rpcAddress) {
        final KubernetesResourceManagerDriverConfiguration
                kubernetesResourceManagerDriverConfiguration =
                        new KubernetesResourceManagerDriverConfiguration(
                                configuration.getString(KubernetesConfigOptions.CLUSTER_ID),
                                POD_CREATION_RETRY_INTERVAL);

        return new KubernetesResourceManagerDriver(
                configuration,
                DefaultKubeClientFactory.getInstance(),
                kubernetesResourceManagerDriverConfiguration);
    }

    @Override
    protected ResourceManagerRuntimeServicesConfiguration
            createResourceManagerRuntimeServicesConfiguration(Configuration configuration)
                    throws ConfigurationException {
        return ResourceManagerRuntimeServicesConfiguration.fromConfiguration(
                configuration, KubernetesWorkerResourceSpecFactory.INSTANCE);
    }
}
