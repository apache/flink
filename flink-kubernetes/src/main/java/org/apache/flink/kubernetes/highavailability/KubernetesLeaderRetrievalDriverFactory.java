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

import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.KubernetesConfigMapSharedWatcher;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalDriverFactory;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalEventHandler;
import org.apache.flink.runtime.rpc.FatalErrorHandler;

import java.util.concurrent.ExecutorService;

/** {@link LeaderRetrievalDriverFactory} implementation for Kubernetes. */
public class KubernetesLeaderRetrievalDriverFactory implements LeaderRetrievalDriverFactory {

    private final FlinkKubeClient kubeClient;

    private final KubernetesConfigMapSharedWatcher configMapSharedWatcher;
    private final ExecutorService watchExecutorService;

    private final String configMapName;

    public KubernetesLeaderRetrievalDriverFactory(
            FlinkKubeClient kubeClient,
            KubernetesConfigMapSharedWatcher configMapSharedWatcher,
            ExecutorService watchExecutorService,
            String configMapName) {
        this.kubeClient = kubeClient;
        this.configMapSharedWatcher = configMapSharedWatcher;
        this.watchExecutorService = watchExecutorService;
        this.configMapName = configMapName;
    }

    @Override
    public KubernetesLeaderRetrievalDriver createLeaderRetrievalDriver(
            LeaderRetrievalEventHandler leaderEventHandler, FatalErrorHandler fatalErrorHandler) {
        return new KubernetesLeaderRetrievalDriver(
                kubeClient,
                configMapSharedWatcher,
                watchExecutorService,
                configMapName,
                leaderEventHandler,
                fatalErrorHandler);
    }
}
