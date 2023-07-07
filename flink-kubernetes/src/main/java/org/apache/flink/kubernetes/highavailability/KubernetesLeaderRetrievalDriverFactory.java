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

import org.apache.flink.kubernetes.kubeclient.KubernetesConfigMapSharedWatcher;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesConfigMap;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.runtime.leaderelection.LeaderInformation;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalDriver;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalDriverFactory;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalEventHandler;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.util.Preconditions;

import java.util.Map;
import java.util.concurrent.Executor;

/**
 * Factory that instantiates a {@link KubernetesLeaderRetrievalDriver} in single leader election
 * mode.
 */
public class KubernetesLeaderRetrievalDriverFactory implements LeaderRetrievalDriverFactory {

    private final KubernetesConfigMapSharedWatcher configMapSharedWatcher;

    private final Executor watchExecutor;

    private final String configMapName;

    private final String componentId;

    public KubernetesLeaderRetrievalDriverFactory(
            KubernetesConfigMapSharedWatcher configMapSharedWatcher,
            Executor watchExecutor,
            String configMapName,
            String componentId) {
        this.configMapSharedWatcher = Preconditions.checkNotNull(configMapSharedWatcher);
        this.watchExecutor = Preconditions.checkNotNull(watchExecutor);
        this.configMapName = Preconditions.checkNotNull(configMapName);
        this.componentId = Preconditions.checkNotNull(componentId);
    }

    @Override
    public LeaderRetrievalDriver createLeaderRetrievalDriver(
            LeaderRetrievalEventHandler leaderEventHandler, FatalErrorHandler fatalErrorHandler) {
        return new KubernetesLeaderRetrievalDriver(
                configMapSharedWatcher,
                watchExecutor,
                configMapName,
                leaderEventHandler,
                this::extractLeaderInformation,
                fatalErrorHandler);
    }

    public LeaderInformation extractLeaderInformation(KubernetesConfigMap configMap) {
        final String configDataLeaderKey = KubernetesUtils.createSingleLeaderKey(componentId);

        final Map<String, String> data = configMap.getData();

        if (data.containsKey(configDataLeaderKey)) {
            return KubernetesUtils.parseLeaderInformationSafely(data.get(configDataLeaderKey))
                    .orElse(LeaderInformation.empty());
        } else {
            return LeaderInformation.empty();
        }
    }
}
