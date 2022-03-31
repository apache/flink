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
import org.apache.flink.runtime.leaderelection.DefaultLeaderElectionService;
import org.apache.flink.runtime.leaderelection.DefaultMultipleComponentLeaderElectionService;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.leaderelection.MultipleComponentLeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.DefaultLeaderRetrievalService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.kubernetes.utils.Constants.LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY;
import static org.apache.flink.kubernetes.utils.Constants.NAME_SEPARATOR;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Kubernetes HA services that use a single leader election service per JobManager. */
public class KubernetesMultipleComponentLeaderElectionHaServices extends AbstractHaServices {

    private final Object lock = new Object();

    private final String clusterId;

    private final FlinkKubeClient kubeClient;

    private final KubernetesConfigMapSharedWatcher configMapSharedWatcher;
    private final ExecutorService watchExecutorService;

    private final String lockIdentity;

    private final FatalErrorHandler fatalErrorHandler;

    @Nullable
    @GuardedBy("lock")
    private DefaultMultipleComponentLeaderElectionService multipleComponentLeaderElectionService =
            null;

    KubernetesMultipleComponentLeaderElectionHaServices(
            FlinkKubeClient kubeClient,
            Executor executor,
            Configuration config,
            BlobStoreService blobStoreService,
            FatalErrorHandler fatalErrorHandler)
            throws IOException {

        super(
                config,
                executor,
                blobStoreService,
                FileSystemJobResultStore.fromConfiguration(config));
        this.kubeClient = checkNotNull(kubeClient);
        this.clusterId = checkNotNull(config.get(KubernetesConfigOptions.CLUSTER_ID));
        this.fatalErrorHandler = checkNotNull(fatalErrorHandler);

        this.configMapSharedWatcher =
                this.kubeClient.createConfigMapSharedWatcher(
                        KubernetesUtils.getConfigMapLabels(
                                clusterId, LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY));
        this.watchExecutorService =
                Executors.newCachedThreadPool(
                        new ExecutorThreadFactory("config-map-watch-handler"));

        lockIdentity = UUID.randomUUID().toString();
    }

    @Override
    protected LeaderElectionService createLeaderElectionService(String leaderName) {
        final MultipleComponentLeaderElectionService multipleComponentLeaderElectionService =
                getOrInitializeSingleLeaderElectionService();

        return new DefaultLeaderElectionService(
                multipleComponentLeaderElectionService.createDriverFactory(leaderName));
    }

    private DefaultMultipleComponentLeaderElectionService
            getOrInitializeSingleLeaderElectionService() {
        synchronized (lock) {
            if (multipleComponentLeaderElectionService == null) {
                try {

                    final KubernetesLeaderElectionConfiguration leaderElectionConfiguration =
                            new KubernetesLeaderElectionConfiguration(
                                    getClusterConfigMap(), lockIdentity, configuration);
                    multipleComponentLeaderElectionService =
                            new DefaultMultipleComponentLeaderElectionService(
                                    fatalErrorHandler,
                                    new KubernetesMultipleComponentLeaderElectionDriverFactory(
                                            kubeClient,
                                            leaderElectionConfiguration,
                                            configMapSharedWatcher,
                                            watchExecutorService,
                                            fatalErrorHandler));
                } catch (Exception e) {
                    throw new FlinkRuntimeException(
                            "Could not initialize the default single leader election service.", e);
                }
            }

            return multipleComponentLeaderElectionService;
        }
    }

    @Override
    protected LeaderRetrievalService createLeaderRetrievalService(String componentId) {
        return new DefaultLeaderRetrievalService(
                new KubernetesMultipleComponentLeaderRetrievalDriverFactory(
                        kubeClient,
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

    private void closeK8sServices() throws Exception {
        Exception exception = null;
        synchronized (lock) {
            if (multipleComponentLeaderElectionService != null) {
                try {
                    multipleComponentLeaderElectionService.close();
                } catch (Exception e) {
                    exception = e;
                }
                multipleComponentLeaderElectionService = null;
            }
        }

        configMapSharedWatcher.close();

        ExceptionUtils.tryRethrowException(exception);
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
        return jobID.toString();
    }

    @Override
    protected String getLeaderPathForRestServer() {
        return "restserver";
    }
}
