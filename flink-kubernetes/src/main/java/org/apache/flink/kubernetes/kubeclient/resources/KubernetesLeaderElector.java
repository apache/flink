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

package org.apache.flink.kubernetes.kubeclient.resources;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.kubernetes.configuration.KubernetesLeaderElectionConfiguration;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.concurrent.FutureUtils;

import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import io.fabric8.kubernetes.client.extended.leaderelection.LeaderCallbacks;
import io.fabric8.kubernetes.client.extended.leaderelection.LeaderElectionConfig;
import io.fabric8.kubernetes.client.extended.leaderelection.LeaderElectionConfigBuilder;
import io.fabric8.kubernetes.client.extended.leaderelection.LeaderElector;
import io.fabric8.kubernetes.client.extended.leaderelection.resourcelock.ConfigMapLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.kubernetes.utils.Constants.LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY;

/**
 * Represent {@link KubernetesLeaderElector} in kubernetes. {@link LeaderElector#run()} is a
 * blocking call. It should be run in the IO executor, not the main thread. The lifecycle is bound
 * to single leader election. Once the leadership is revoked, as well as the {@link
 * LeaderCallbackHandler#notLeader()} is called, the {@link LeaderElector#run()} will finish. To
 * start another round of election, we need to trigger again.
 *
 * <p>{@link LeaderElector#run()} is responsible for creating the leader ConfigMap and continuously
 * update the annotation. The annotation key is {@link #LEADER_ANNOTATION_KEY} and the value is in
 * the following json format. metadata: annotations: control-plane.alpha.kubernetes.io/leader:
 * '{"holderIdentity":"623e39fb-70c3-44f1-811f-561ec4a28d75","leaseDuration":15.000000000,"acquireTime":"2020-10-20T04:06:31.431000Z","renewTime":"2020-10-22T08:51:36.843000Z","leaderTransitions":37981}'
 */
public class KubernetesLeaderElector {

    private static final Logger LOG = LoggerFactory.getLogger(KubernetesLeaderElector.class);

    @VisibleForTesting
    public static final String LEADER_ANNOTATION_KEY = "control-plane.alpha.kubernetes.io/leader";

    private final Object lock = new Object();

    private final NamespacedKubernetesClient kubernetesClient;
    private final LeaderElectionConfig leaderElectionConfig;
    private final ExecutorService executorService;

    private CompletableFuture<?> currentLeaderElectionSession = FutureUtils.completedVoidFuture();

    public KubernetesLeaderElector(
            NamespacedKubernetesClient kubernetesClient,
            KubernetesLeaderElectionConfiguration leaderConfig,
            LeaderCallbackHandler leaderCallbackHandler) {
        this(
                kubernetesClient,
                leaderConfig,
                leaderCallbackHandler,
                Executors.newSingleThreadExecutor(
                        new ExecutorThreadFactory("KubernetesLeaderElector-ExecutorService")));
    }

    @VisibleForTesting
    public KubernetesLeaderElector(
            NamespacedKubernetesClient kubernetesClient,
            KubernetesLeaderElectionConfiguration leaderConfig,
            LeaderCallbackHandler leaderCallbackHandler,
            ExecutorService executorService) {
        this.kubernetesClient = kubernetesClient;
        this.leaderElectionConfig =
                new LeaderElectionConfigBuilder()
                        .withName(leaderConfig.getConfigMapName())
                        .withLeaseDuration(leaderConfig.getLeaseDuration())
                        .withLock(
                                new ConfigMapLock(
                                        new ObjectMetaBuilder()
                                                .withNamespace(kubernetesClient.getNamespace())
                                                .withName(leaderConfig.getConfigMapName())
                                                // Labels will be used to clean up the ha related
                                                // ConfigMaps.
                                                .withLabels(
                                                        KubernetesUtils.getConfigMapLabels(
                                                                leaderConfig.getClusterId(),
                                                                LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY))
                                                .build(),
                                        leaderConfig.getLockIdentity()))
                        .withRenewDeadline(leaderConfig.getRenewDeadline())
                        .withRetryPeriod(leaderConfig.getRetryPeriod())
                        .withReleaseOnCancel(true)
                        .withLeaderCallbacks(
                                new LeaderCallbacks(
                                        leaderCallbackHandler::isLeader,
                                        leaderCallbackHandler::notLeader,
                                        newLeader ->
                                                LOG.info(
                                                        "New leader elected {} for {}.",
                                                        newLeader,
                                                        leaderConfig.getConfigMapName())))
                        .build();
        this.executorService = executorService;

        LOG.info(
                "Create KubernetesLeaderElector on lock {}.",
                leaderElectionConfig.getLock().describe());
    }

    @GuardedBy("lock")
    private void resetInternalLeaderElector() {
        cancelCurrentLeaderElectionSession();

        currentLeaderElectionSession =
                new LeaderElector(kubernetesClient, leaderElectionConfig, executorService).start();

        LOG.info(
                "Triggered leader election on lock {}.", leaderElectionConfig.getLock().describe());
    }

    @GuardedBy("lock")
    private void cancelCurrentLeaderElectionSession() {
        currentLeaderElectionSession.cancel(true);
    }

    public void run() {
        synchronized (lock) {
            if (executorService.isShutdown()) {
                LOG.debug(
                        "Ignoring KubernetesLeaderElector.run call because the leader elector has already been shut down.");
            } else {
                resetInternalLeaderElector();
            }
        }
    }

    public void stop() {
        synchronized (lock) {
            // cancelling the current session needs to happen explicitly to allow the execution of
            // code that handles the leader loss
            cancelCurrentLeaderElectionSession();

            // the shutdown of the executor needs to happen gracefully for scenarios where the
            // release is called in the executorService. Interrupting this logic will result in the
            // leadership-lost event not being sent to the client.
            final List<Runnable> outStandingTasks =
                    ExecutorUtils.gracefulShutdown(30, TimeUnit.SECONDS, executorService);

            if (!outStandingTasks.isEmpty()) {
                LOG.warn(
                        "{} events were not processed before stopping the {} instance.",
                        outStandingTasks.size(),
                        KubernetesLeaderElector.class.getSimpleName());
            }
        }
    }

    public static boolean hasLeadership(KubernetesConfigMap configMap, String lockIdentity) {
        final String leader = configMap.getAnnotations().get(LEADER_ANNOTATION_KEY);
        return leader != null && leader.contains(lockIdentity);
    }

    /** Callback handler for leader election. */
    public abstract static class LeaderCallbackHandler {

        public abstract void isLeader();

        public abstract void notLeader();
    }
}
