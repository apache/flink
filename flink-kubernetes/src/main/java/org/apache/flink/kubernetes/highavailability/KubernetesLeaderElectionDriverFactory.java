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

package org.apache.flink.kubernetes.highavailability;

import org.apache.flink.kubernetes.configuration.KubernetesLeaderElectionConfiguration;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.KubernetesConfigMapSharedWatcher;
import org.apache.flink.runtime.leaderelection.LeaderElectionDriverFactory;
import org.apache.flink.runtime.leaderelection.LeaderElectionEventHandler;
import org.apache.flink.runtime.rpc.FatalErrorHandler;

import java.util.concurrent.ExecutorService;

/** {@link LeaderElectionDriverFactory} implementation for Kubernetes. */
public class KubernetesLeaderElectionDriverFactory implements LeaderElectionDriverFactory {

    private final FlinkKubeClient kubeClient;
    private final KubernetesConfigMapSharedWatcher configMapSharedWatcher;
    private final ExecutorService watchExecutorService;
    private final KubernetesLeaderElectionConfiguration leaderConfig;

    public KubernetesLeaderElectionDriverFactory(
            FlinkKubeClient kubeClient,
            KubernetesConfigMapSharedWatcher configMapSharedWatcher,
            ExecutorService watchExecutorService,
            KubernetesLeaderElectionConfiguration leaderConfig) {
        this.kubeClient = kubeClient;
        this.configMapSharedWatcher = configMapSharedWatcher;
        this.watchExecutorService = watchExecutorService;
        this.leaderConfig = leaderConfig;
    }

    @Override
    public KubernetesLeaderElectionDriver createLeaderElectionDriver(
            LeaderElectionEventHandler leaderEventHandler,
            FatalErrorHandler fatalErrorHandler,
            String leaderContenderDescription) {
        return new KubernetesLeaderElectionDriver(
                kubeClient,
                configMapSharedWatcher,
                watchExecutorService,
                leaderConfig,
                leaderEventHandler,
                fatalErrorHandler);
    }
}
