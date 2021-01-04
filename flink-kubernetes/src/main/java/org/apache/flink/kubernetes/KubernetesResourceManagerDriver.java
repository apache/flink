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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.KubernetesResourceManagerDriverConfiguration;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.KubeClientFactory;
import org.apache.flink.kubernetes.kubeclient.factory.KubernetesTaskManagerFactory;
import org.apache.flink.kubernetes.kubeclient.parameters.KubernetesTaskManagerParameters;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesPod;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesWatch;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessSpec;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.externalresource.ExternalResourceUtils;
import org.apache.flink.runtime.resourcemanager.active.AbstractResourceManagerDriver;
import org.apache.flink.runtime.resourcemanager.active.ResourceManagerDriver;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Implementation of {@link ResourceManagerDriver} for Kubernetes deployment. */
public class KubernetesResourceManagerDriver
        extends AbstractResourceManagerDriver<KubernetesWorkerNode> {

    /** The taskmanager pod name pattern is {clusterId}-{taskmanager}-{attemptId}-{podIndex}. */
    private static final String TASK_MANAGER_POD_FORMAT = "%s-taskmanager-%d-%d";

    private final String clusterId;

    private final Time podCreationRetryInterval;

    private final KubeClientFactory kubeClientFactory;

    private Optional<FlinkKubeClient> kubeClientOpt;

    /** Request resource futures, keyed by pod names. */
    private final Map<String, CompletableFuture<KubernetesWorkerNode>> requestResourceFutures;

    /** When ResourceManager failover, the max attempt should recover. */
    private long currentMaxAttemptId = 0;

    /** Current max pod index. When creating a new pod, it should increase one. */
    private long currentMaxPodId = 0;

    private Optional<KubernetesWatch> podsWatchOpt;

    /**
     * Incompletion of this future indicates that there was a pod creation failure recently and the
     * driver should not retry creating pods until the future become completed again. It's
     * guaranteed to be modified in main thread.
     */
    private CompletableFuture<Void> podCreationCoolDown;

    public KubernetesResourceManagerDriver(
            Configuration flinkConfig,
            KubeClientFactory kubeClientFactory,
            KubernetesResourceManagerDriverConfiguration configuration) {
        super(flinkConfig, GlobalConfiguration.loadConfiguration());
        this.clusterId = Preconditions.checkNotNull(configuration.getClusterId());
        this.podCreationRetryInterval =
                Preconditions.checkNotNull(configuration.getPodCreationRetryInterval());
        this.kubeClientFactory = Preconditions.checkNotNull(kubeClientFactory);
        this.requestResourceFutures = new HashMap<>();
        this.podCreationCoolDown = FutureUtils.completedVoidFuture();
    }

    // ------------------------------------------------------------------------
    //  ResourceManagerDriver
    // ------------------------------------------------------------------------

    @Override
    protected void initializeInternal() throws Exception {
        kubeClientOpt =
                Optional.of(kubeClientFactory.fromConfiguration(flinkConfig, getIoExecutor()));
        podsWatchOpt =
                Optional.of(
                        getKubeClient()
                                .watchPodsAndDoCallback(
                                        KubernetesUtils.getTaskManagerLabels(clusterId),
                                        new PodCallbackHandlerImpl()));
        recoverWorkerNodesFromPreviousAttempts();
    }

    @Override
    public CompletableFuture<Void> terminate() {
        // shut down all components
        Exception exception = null;

        try {
            podsWatchOpt.ifPresent(KubernetesWatch::close);
        } catch (Exception e) {
            exception = e;
        }

        try {
            if (kubeClientOpt.isPresent()) {
                kubeClientOpt.get().close();
            }
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        return exception == null
                ? FutureUtils.completedVoidFuture()
                : FutureUtils.completedExceptionally(exception);
    }

    @Override
    public void deregisterApplication(
            ApplicationStatus finalStatus, @Nullable String optionalDiagnostics) {
        log.info(
                "Deregistering Flink Kubernetes cluster, clusterId: {}, diagnostics: {}",
                clusterId,
                optionalDiagnostics == null ? "" : optionalDiagnostics);
        getKubeClient().stopAndCleanupCluster(clusterId);
    }

    @Override
    public CompletableFuture<KubernetesWorkerNode> requestResource(
            TaskExecutorProcessSpec taskExecutorProcessSpec) {
        final KubernetesTaskManagerParameters parameters =
                createKubernetesTaskManagerParameters(taskExecutorProcessSpec);
        final KubernetesPod taskManagerPod =
                KubernetesTaskManagerFactory.buildTaskManagerKubernetesPod(parameters);
        final String podName = taskManagerPod.getName();
        final CompletableFuture<KubernetesWorkerNode> requestResourceFuture =
                new CompletableFuture<>();

        requestResourceFutures.put(podName, requestResourceFuture);

        log.info(
                "Creating new TaskManager pod with name {} and resource <{},{}>.",
                podName,
                parameters.getTaskManagerMemoryMB(),
                parameters.getTaskManagerCPU());

        // When K8s API Server is temporary unavailable, `kubeClient.createTaskManagerPod` might
        // fail immediately.
        // In case of pod creation failures, we should wait for an interval before trying to create
        // new pods.
        // Otherwise, ActiveResourceManager will always re-requesting the worker, which keeps the
        // main thread busy.
        final CompletableFuture<Void> createPodFuture =
                podCreationCoolDown.thenCompose(
                        (ignore) -> getKubeClient().createTaskManagerPod(taskManagerPod));

        FutureUtils.assertNoException(
                createPodFuture.handleAsync(
                        (ignore, exception) -> {
                            if (exception != null) {
                                log.warn(
                                        "Could not create pod {}, exception: {}",
                                        podName,
                                        exception);
                                tryResetPodCreationCoolDown();
                                CompletableFuture<KubernetesWorkerNode> future =
                                        requestResourceFutures.remove(taskManagerPod.getName());
                                if (future != null) {
                                    future.completeExceptionally(exception);
                                }
                            } else {
                                log.info("Pod {} is created.", podName);
                            }
                            return null;
                        },
                        getMainThreadExecutor()));

        return requestResourceFuture;
    }

    @Override
    public void releaseResource(KubernetesWorkerNode worker) {
        final String podName = worker.getResourceID().toString();

        log.info("Stopping TaskManager pod {}.", podName);

        stopPod(podName);
    }

    // ------------------------------------------------------------------------
    //  Internal
    // ------------------------------------------------------------------------

    private void recoverWorkerNodesFromPreviousAttempts() throws ResourceManagerException {
        List<KubernetesPod> podList =
                getKubeClient().getPodsWithLabels(KubernetesUtils.getTaskManagerLabels(clusterId));
        final List<KubernetesWorkerNode> recoveredWorkers = new ArrayList<>();

        for (KubernetesPod pod : podList) {
            final KubernetesWorkerNode worker =
                    new KubernetesWorkerNode(new ResourceID(pod.getName()));
            final long attempt = worker.getAttempt();
            if (attempt > currentMaxAttemptId) {
                currentMaxAttemptId = attempt;
            }

            if (pod.isTerminated()) {
                stopPod(pod.getName());
            } else {
                recoveredWorkers.add(worker);
            }
        }

        log.info(
                "Recovered {} pods from previous attempts, current attempt id is {}.",
                recoveredWorkers.size(),
                ++currentMaxAttemptId);

        // Should not invoke resource event handler on the main thread executor.
        // We are in the initializing thread. The main thread executor is not yet ready.
        getResourceEventHandler().onPreviousAttemptWorkersRecovered(recoveredWorkers);
    }

    private KubernetesTaskManagerParameters createKubernetesTaskManagerParameters(
            TaskExecutorProcessSpec taskExecutorProcessSpec) {
        final String podName =
                String.format(
                        TASK_MANAGER_POD_FORMAT, clusterId, currentMaxAttemptId, ++currentMaxPodId);

        final ContaineredTaskManagerParameters taskManagerParameters =
                ContaineredTaskManagerParameters.create(flinkConfig, taskExecutorProcessSpec);

        final Configuration taskManagerConfig = new Configuration(flinkConfig);
        taskManagerConfig.set(TaskManagerOptions.TASK_MANAGER_RESOURCE_ID, podName);

        final String dynamicProperties =
                BootstrapTools.getDynamicPropertiesAsString(flinkClientConfig, taskManagerConfig);

        return new KubernetesTaskManagerParameters(
                flinkConfig,
                podName,
                dynamicProperties,
                taskManagerParameters,
                ExternalResourceUtils.getExternalResources(
                        flinkConfig,
                        KubernetesConfigOptions.EXTERNAL_RESOURCE_KUBERNETES_CONFIG_KEY_SUFFIX));
    }

    private void tryResetPodCreationCoolDown() {
        if (podCreationCoolDown.isDone()) {
            log.info(
                    "Pod creation failed. Will not retry creating pods in {}.",
                    podCreationRetryInterval);
            podCreationCoolDown = new CompletableFuture<>();
            getMainThreadExecutor()
                    .schedule(
                            () -> podCreationCoolDown.complete(null),
                            podCreationRetryInterval.getSize(),
                            podCreationRetryInterval.getUnit());
        }
    }

    private void terminatedPodsInMainThread(List<KubernetesPod> pods) {
        getMainThreadExecutor()
                .execute(
                        () -> {
                            for (KubernetesPod pod : pods) {
                                if (pod.isTerminated()) {
                                    final String podName = pod.getName();
                                    log.debug("TaskManager pod {} is terminated.", podName);

                                    // this is a safe net, in case onModified/onDeleted/onError is
                                    // received before onAdded
                                    final CompletableFuture<KubernetesWorkerNode>
                                            requestResourceFuture =
                                                    requestResourceFutures.remove(podName);
                                    if (requestResourceFuture != null) {
                                        log.warn(
                                                "Pod {} is terminated before receiving the ADDED event.",
                                                podName);
                                        requestResourceFuture.completeExceptionally(
                                                new FlinkException("Pod is terminated."));
                                    }

                                    getResourceEventHandler()
                                            .onWorkerTerminated(
                                                    new ResourceID(podName),
                                                    pod.getTerminatedDiagnostics());
                                    stopPod(podName);
                                }
                            }
                        });
    }

    private void stopPod(String podName) {
        getKubeClient()
                .stopPod(podName)
                .whenComplete(
                        (ignore, throwable) -> {
                            if (throwable != null) {
                                log.warn(
                                        "Could not remove TaskManager pod {}, exception: {}",
                                        podName,
                                        throwable);
                            }
                        });
    }

    private FlinkKubeClient getKubeClient() {
        Preconditions.checkState(
                kubeClientOpt.isPresent(),
                "Cannot get the kube client. Resource manager driver is not initialized.");
        return kubeClientOpt.get();
    }

    // ------------------------------------------------------------------------
    //  FlinkKubeClient.WatchCallbackHandler
    // ------------------------------------------------------------------------

    private class PodCallbackHandlerImpl
            implements FlinkKubeClient.WatchCallbackHandler<KubernetesPod> {
        @Override
        public void onAdded(List<KubernetesPod> pods) {
            getMainThreadExecutor()
                    .execute(
                            () -> {
                                for (KubernetesPod pod : pods) {
                                    final String podName = pod.getName();
                                    final CompletableFuture<KubernetesWorkerNode>
                                            requestResourceFuture =
                                                    requestResourceFutures.remove(podName);

                                    if (requestResourceFuture == null) {
                                        log.debug(
                                                "Ignore TaskManager pod that is already added: {}",
                                                podName);
                                        continue;
                                    }

                                    log.info("Received new TaskManager pod: {}", podName);
                                    requestResourceFuture.complete(
                                            new KubernetesWorkerNode(new ResourceID(podName)));
                                }
                            });
        }

        @Override
        public void onModified(List<KubernetesPod> pods) {
            terminatedPodsInMainThread(pods);
        }

        @Override
        public void onDeleted(List<KubernetesPod> pods) {
            terminatedPodsInMainThread(pods);
        }

        @Override
        public void onError(List<KubernetesPod> pods) {
            terminatedPodsInMainThread(pods);
        }

        @Override
        public void handleFatalError(Throwable throwable) {
            getResourceEventHandler().onError(throwable);
        }
    }
}
