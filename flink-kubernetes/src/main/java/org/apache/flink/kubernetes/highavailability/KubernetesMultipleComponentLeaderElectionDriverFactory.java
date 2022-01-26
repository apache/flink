/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.flink.runtime.leaderelection.MultipleComponentLeaderElectionDriver;
import org.apache.flink.runtime.leaderelection.MultipleComponentLeaderElectionDriverFactory;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.util.Preconditions;

import java.util.concurrent.Executor;

/** Factory that instantiates a {@link KubernetesMultipleComponentLeaderElectionDriver}. */
public class KubernetesMultipleComponentLeaderElectionDriverFactory
        implements MultipleComponentLeaderElectionDriverFactory {
    private final FlinkKubeClient kubeClient;

    private final KubernetesLeaderElectionConfiguration kubernetesLeaderElectionConfiguration;

    private final KubernetesConfigMapSharedWatcher configMapSharedWatcher;

    private final Executor watchExecutor;

    private final FatalErrorHandler fatalErrorHandler;

    public KubernetesMultipleComponentLeaderElectionDriverFactory(
            FlinkKubeClient kubeClient,
            KubernetesLeaderElectionConfiguration kubernetesLeaderElectionConfiguration,
            KubernetesConfigMapSharedWatcher configMapSharedWatcher,
            Executor watchExecutor,
            FatalErrorHandler fatalErrorHandler) {
        this.kubeClient = Preconditions.checkNotNull(kubeClient);
        this.kubernetesLeaderElectionConfiguration =
                Preconditions.checkNotNull(kubernetesLeaderElectionConfiguration);
        this.configMapSharedWatcher = Preconditions.checkNotNull(configMapSharedWatcher);
        this.watchExecutor = Preconditions.checkNotNull(watchExecutor);
        this.fatalErrorHandler = Preconditions.checkNotNull(fatalErrorHandler);
    }

    @Override
    public KubernetesMultipleComponentLeaderElectionDriver create(
            MultipleComponentLeaderElectionDriver.Listener leaderElectionListener)
            throws Exception {
        return new KubernetesMultipleComponentLeaderElectionDriver(
                kubernetesLeaderElectionConfiguration,
                kubeClient,
                leaderElectionListener,
                configMapSharedWatcher,
                watchExecutor,
                fatalErrorHandler);
    }
}
