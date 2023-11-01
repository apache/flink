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
import org.apache.flink.kubernetes.kubeclient.KubernetesSharedWatcher;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesConfigMap;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesException;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesLeaderElector;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.runtime.leaderelection.LeaderElectionException;
import org.apache.flink.runtime.leaderelection.LeaderInformation;
import org.apache.flink.runtime.leaderelection.LeaderInformationWithComponentId;
import org.apache.flink.runtime.leaderelection.MultipleComponentLeaderElectionDriver;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static org.apache.flink.kubernetes.utils.Constants.LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY;
import static org.apache.flink.kubernetes.utils.KubernetesUtils.getOnlyConfigMap;

/** {@link MultipleComponentLeaderElectionDriver} for Kubernetes. */
public class KubernetesMultipleComponentLeaderElectionDriver
        implements MultipleComponentLeaderElectionDriver {

    private static final Logger LOG =
            LoggerFactory.getLogger(KubernetesMultipleComponentLeaderElectionDriver.class);

    private final FlinkKubeClient kubeClient;

    private final String configMapName;

    private final String lockIdentity;

    private final MultipleComponentLeaderElectionDriver.Listener leaderElectionListener;

    private final KubernetesLeaderElector leaderElector;

    // Labels will be used to clean up the ha related ConfigMaps.
    private final Map<String, String> configMapLabels;

    private final FatalErrorHandler fatalErrorHandler;

    private final KubernetesSharedWatcher.Watch kubernetesWatch;

    private final AtomicBoolean running = new AtomicBoolean(true);

    public KubernetesMultipleComponentLeaderElectionDriver(
            KubernetesLeaderElectionConfiguration leaderElectionConfiguration,
            FlinkKubeClient kubeClient,
            Listener leaderElectionListener,
            KubernetesConfigMapSharedWatcher configMapSharedWatcher,
            Executor watchExecutor,
            FatalErrorHandler fatalErrorHandler) {
        Preconditions.checkNotNull(leaderElectionConfiguration);
        this.kubeClient = Preconditions.checkNotNull(kubeClient);
        this.leaderElectionListener = Preconditions.checkNotNull(leaderElectionListener);
        this.fatalErrorHandler = Preconditions.checkNotNull(fatalErrorHandler);
        Preconditions.checkNotNull(configMapSharedWatcher);
        Preconditions.checkNotNull(watchExecutor);

        this.configMapName = leaderElectionConfiguration.getConfigMapName();
        this.lockIdentity = leaderElectionConfiguration.getLockIdentity();

        this.leaderElector =
                kubeClient.createLeaderElector(
                        leaderElectionConfiguration, new LeaderCallbackHandlerImpl());

        this.configMapLabels =
                KubernetesUtils.getConfigMapLabels(
                        leaderElectionConfiguration.getClusterId(),
                        LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY);

        kubernetesWatch =
                configMapSharedWatcher.watch(
                        configMapName, new ConfigMapCallbackHandlerImpl(), watchExecutor);

        leaderElector.run();

        LOG.debug("Starting the {} for config map {}.", getClass().getSimpleName(), configMapName);
    }

    @Override
    public void close() throws Exception {
        if (running.compareAndSet(true, false)) {
            LOG.info("Closing {}.", this);

            leaderElector.stop();
            kubernetesWatch.close();
        }
    }

    @Override
    public boolean hasLeadership() {
        Preconditions.checkState(running.get());
        final Optional<KubernetesConfigMap> optionalConfigMap =
                kubeClient.getConfigMap(configMapName);

        if (optionalConfigMap.isPresent()) {
            return KubernetesLeaderElector.hasLeadership(optionalConfigMap.get(), lockIdentity);
        } else {
            fatalErrorHandler.onFatalError(
                    new KubernetesException(
                            String.format(
                                    "ConfigMap %s does not exist. This indicates that somebody has interfered with Flink's operation.",
                                    configMapName)));
            return false;
        }
    }

    @Override
    public void publishLeaderInformation(String componentId, LeaderInformation leaderInformation)
            throws Exception {
        Preconditions.checkState(running.get());

        kubeClient
                .checkAndUpdateConfigMap(
                        configMapName,
                        updateConfigMapWithLeaderInformation(componentId, leaderInformation))
                .get();

        LOG.debug(
                "Successfully wrote leader information {} for leader {} into the config map {}.",
                leaderInformation,
                componentId,
                configMapName);
    }

    @Override
    public void deleteLeaderInformation(String componentId) throws Exception {
        publishLeaderInformation(componentId, LeaderInformation.empty());
    }

    private Function<KubernetesConfigMap, Optional<KubernetesConfigMap>>
            updateConfigMapWithLeaderInformation(
                    String leaderName, LeaderInformation leaderInformation) {
        final String configMapDataKey = KubernetesUtils.createSingleLeaderKey(leaderName);

        return kubernetesConfigMap -> {
            if (KubernetesLeaderElector.hasLeadership(kubernetesConfigMap, lockIdentity)) {
                final Map<String, String> data = kubernetesConfigMap.getData();

                if (leaderInformation.isEmpty()) {
                    data.remove(configMapDataKey);
                } else {
                    data.put(
                            configMapDataKey,
                            KubernetesUtils.encodeLeaderInformation(leaderInformation));
                }

                kubernetesConfigMap.getLabels().putAll(configMapLabels);
                return Optional.of(kubernetesConfigMap);
            }

            return Optional.empty();
        };
    }

    private static Collection<LeaderInformationWithComponentId> extractLeaderInformation(
            KubernetesConfigMap configMap) {
        final Map<String, String> data = configMap.getData();

        final Collection<LeaderInformationWithComponentId> leaderInformationWithLeaderNames =
                new ArrayList<>();

        for (Map.Entry<String, String> keyValuePair : data.entrySet()) {
            final String key = keyValuePair.getKey();
            if (KubernetesUtils.isSingleLeaderKey(key)) {
                final String leaderName = KubernetesUtils.extractLeaderName(key);
                final LeaderInformation leaderInformation =
                        KubernetesUtils.parseLeaderInformationSafely(keyValuePair.getValue())
                                .orElse(LeaderInformation.empty());
                leaderInformationWithLeaderNames.add(
                        LeaderInformationWithComponentId.create(leaderName, leaderInformation));
            }
        }

        return leaderInformationWithLeaderNames;
    }

    private class LeaderCallbackHandlerImpl extends KubernetesLeaderElector.LeaderCallbackHandler {
        @Override
        public void isLeader() {
            leaderElectionListener.isLeader();
        }

        @Override
        public void notLeader() {
            leaderElectionListener.notLeader();
            leaderElector.run();
        }
    }

    private class ConfigMapCallbackHandlerImpl
            implements FlinkKubeClient.WatchCallbackHandler<KubernetesConfigMap> {
        @Override
        public void onAdded(List<KubernetesConfigMap> resources) {
            // nothing to do
        }

        @Override
        public void onModified(List<KubernetesConfigMap> configMaps) {
            final KubernetesConfigMap configMap = getOnlyConfigMap(configMaps, configMapName);

            if (KubernetesLeaderElector.hasLeadership(configMap, lockIdentity)) {
                Collection<LeaderInformationWithComponentId> leaderInformationWithLeaderNames =
                        extractLeaderInformation(configMap);

                leaderElectionListener.notifyAllKnownLeaderInformation(
                        leaderInformationWithLeaderNames);
            }
        }

        @Override
        public void onDeleted(List<KubernetesConfigMap> configMaps) {
            final KubernetesConfigMap configMap = getOnlyConfigMap(configMaps, configMapName);
            if (KubernetesLeaderElector.hasLeadership(configMap, lockIdentity)) {
                fatalErrorHandler.onFatalError(
                        new LeaderElectionException(
                                String.format(
                                        "ConfigMap %s has been deleted externally.",
                                        configMapName)));
            }
        }

        @Override
        public void onError(List<KubernetesConfigMap> configMaps) {
            final KubernetesConfigMap configMap = getOnlyConfigMap(configMaps, configMapName);
            if (KubernetesLeaderElector.hasLeadership(configMap, lockIdentity)) {
                fatalErrorHandler.onFatalError(
                        new LeaderElectionException(
                                String.format(
                                        "Error while watching the ConfigMap %s.", configMapName)));
            }
        }

        @Override
        public void handleError(Throwable throwable) {
            fatalErrorHandler.onFatalError(
                    new LeaderElectionException(
                            String.format("Error while watching the ConfigMap %s.", configMapName),
                            throwable));
        }
    }
}
