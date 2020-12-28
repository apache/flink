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

package org.apache.flink.kubernetes.configuration;

import org.apache.flink.configuration.Configuration;

import java.time.Duration;

/**
 * Configuration specific to {@link
 * org.apache.flink.kubernetes.kubeclient.resources.KubernetesLeaderElector}.
 */
public class KubernetesLeaderElectionConfiguration {

    private final String clusterId;
    private final String configMapName;
    private final String lockIdentity;
    private final Duration leaseDuration;
    private final Duration renewDeadline;
    private final Duration retryPeriod;

    public KubernetesLeaderElectionConfiguration(
            String configMapName, String lockIdentity, Configuration config) {
        this.clusterId = config.getString(KubernetesConfigOptions.CLUSTER_ID);
        this.configMapName = configMapName;
        this.lockIdentity = lockIdentity;

        this.leaseDuration =
                config.get(KubernetesHighAvailabilityOptions.KUBERNETES_LEASE_DURATION);
        this.renewDeadline =
                config.get(KubernetesHighAvailabilityOptions.KUBERNETES_RENEW_DEADLINE);
        this.retryPeriod = config.get(KubernetesHighAvailabilityOptions.KUBERNETES_RETRY_PERIOD);
    }

    public String getClusterId() {
        return clusterId;
    }

    public String getConfigMapName() {
        return configMapName;
    }

    public String getLockIdentity() {
        return lockIdentity;
    }

    public Duration getLeaseDuration() {
        return leaseDuration;
    }

    public Duration getRenewDeadline() {
        return renewDeadline;
    }

    public Duration getRetryPeriod() {
        return retryPeriod;
    }
}
