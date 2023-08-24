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

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.KubernetesLeaderElectionConfiguration;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.KubernetesConfigMapSharedWatcher;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.runtime.blob.BlobStoreService;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.highavailability.AbstractHaServices;
import org.apache.flink.runtime.highavailability.FileSystemJobResultStore;
import org.apache.flink.runtime.jobmanager.JobGraphStore;
import org.apache.flink.runtime.leaderelection.LeaderElectionDriverFactory;
import org.apache.flink.runtime.leaderretrieval.DefaultLeaderRetrievalService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.kubernetes.utils.Constants.LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY;
import static org.apache.flink.kubernetes.utils.Constants.NAME_SEPARATOR;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Kubernetes HA services that use a single leader election service per JobManager. */
public class KubernetesLeaderElectionHaServices extends AbstractHaServices {

    private static final Logger LOG =
            LoggerFactory.getLogger(KubernetesLeaderElectionHaServices.class);

    private final String clusterId;

    private final FlinkKubeClient kubeClient;

    private final KubernetesConfigMapSharedWatcher configMapSharedWatcher;
    private final ExecutorService watchExecutorService;

    private final String lockIdentity;

    KubernetesLeaderElectionHaServices(
            FlinkKubeClient kubeClient,
            Executor ioExecutor,
            Configuration configuration,
            BlobStoreService blobStoreService)
            throws Exception {
        this(
                kubeClient,
                kubeClient.createConfigMapSharedWatcher(
                        KubernetesUtils.getConfigMapLabels(
                                configuration.get(KubernetesConfigOptions.CLUSTER_ID),
                                LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY)),
                Executors.newCachedThreadPool(
                        new ExecutorThreadFactory("config-map-watch-handler")),
                ioExecutor,
                configuration.get(KubernetesConfigOptions.CLUSTER_ID),
                UUID.randomUUID().toString(),
                configuration,
                blobStoreService);
    }

    private KubernetesLeaderElectionHaServices(
            FlinkKubeClient kubeClient,
            KubernetesConfigMapSharedWatcher configMapSharedWatcher,
            ExecutorService watchExecutorService,
            Executor ioExecutor,
            String clusterId,
            String lockIdentity,
            Configuration configuration,
            BlobStoreService blobStoreService)
            throws Exception {
        super(
                configuration,
                createDriverFactory(
                        kubeClient,
                        configMapSharedWatcher,
                        watchExecutorService,
                        clusterId,
                        lockIdentity,
                        configuration),
                ioExecutor,
                blobStoreService,
                FileSystemJobResultStore.fromConfiguration(configuration, ioExecutor));

        this.kubeClient = checkNotNull(kubeClient);
        this.clusterId = checkNotNull(clusterId);
        this.configMapSharedWatcher = checkNotNull(configMapSharedWatcher);
        this.watchExecutorService = checkNotNull(watchExecutorService);
        this.lockIdentity = checkNotNull(lockIdentity);
    }

    private static LeaderElectionDriverFactory createDriverFactory(
            FlinkKubeClient kubeClient,
            KubernetesConfigMapSharedWatcher configMapSharedWatcher,
            Executor watchExecutorService,
            String clusterId,
            String lockIdentity,
            Configuration configuration) {
        final KubernetesLeaderElectionConfiguration leaderElectionConfiguration =
                new KubernetesLeaderElectionConfiguration(
                        getClusterConfigMap(clusterId), lockIdentity, configuration);
        return new KubernetesLeaderElectionDriverFactory(
                kubeClient,
                leaderElectionConfiguration,
                configMapSharedWatcher,
                watchExecutorService);
    }

    @Override
    protected LeaderRetrievalService createLeaderRetrievalService(String componentId) {
        return new DefaultLeaderRetrievalService(
                new KubernetesLeaderRetrievalDriverFactory(
                        configMapSharedWatcher,
                        watchExecutorService,
                        getClusterConfigMap(),
                        componentId));
    }

    @Override
    protected CheckpointRecoveryFactory createCheckpointRecoveryFactory() {
        return KubernetesCheckpointRecoveryFactory.withoutLeadershipValidation(
                kubeClient, configuration, ioExecutor, clusterId, this::getJobSpecificConfigMap);
    }

    private String getJobSpecificConfigMap(JobID jobID) {
        return clusterId + NAME_SEPARATOR + jobID.toString() + NAME_SEPARATOR + "config-map";
    }

    @Override
    protected JobGraphStore createJobGraphStore() throws Exception {
        return KubernetesUtils.createJobGraphStore(
                configuration, kubeClient, getClusterConfigMap(), lockIdentity);
    }

    private String getClusterConfigMap() {
        return getClusterConfigMap(clusterId);
    }

    private static String getClusterConfigMap(String clusterId) {
        return clusterId + NAME_SEPARATOR + "cluster-config-map";
    }

    @Override
    public void internalClose() throws Exception {
        Exception exception = null;
        try {
            closeK8sServices();
        } catch (Exception e) {
            exception = e;
        }

        kubeClient.close();
        ExecutorUtils.gracefulShutdown(5, TimeUnit.SECONDS, this.watchExecutorService);

        ExceptionUtils.tryRethrowException(exception);
    }

    private void closeK8sServices() {
        configMapSharedWatcher.close();
        final int outstandingTaskCount = watchExecutorService.shutdownNow().size();
        if (outstandingTaskCount != 0) {
            LOG.debug(
                    "The k8s HA services were closed with {} event(s) still not being processed. No further action necessary.",
                    outstandingTaskCount);
        }
    }

    @Override
    public void internalCleanup() throws Exception {
        Exception exception = null;
        // in order to clean up, we first need to stop the services that rely on the config maps
        try {
            closeK8sServices();
        } catch (Exception e) {
            exception = e;
        }

        kubeClient
                .deleteConfigMapsByLabels(
                        KubernetesUtils.getConfigMapLabels(
                                clusterId, LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY))
                .get();

        ExceptionUtils.tryRethrowException(exception);
    }

    @Override
    public void internalCleanupJobData(JobID jobID) throws Exception {
        kubeClient.deleteConfigMap(getJobSpecificConfigMap(jobID)).get();
        // need to delete job specific leader address from leader config map
    }

    @Override
    protected String getLeaderPathForResourceManager() {
        return "resourcemanager";
    }

    @Override
    protected String getLeaderPathForDispatcher() {
        return "dispatcher";
    }

    @Override
    protected String getLeaderPathForJobManager(JobID jobID) {
        return "job-" + jobID.toString();
    }

    @Override
    protected String getLeaderPathForRestServer() {
        return "restserver";
    }
}
