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

package org.apache.flink.kubernetes.kubeclient;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.KubernetesLeaderElectionConfiguration;
import org.apache.flink.kubernetes.kubeclient.decorators.ExternalServiceDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.InternalServiceDecorator;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesConfigMap;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesConfigMapSharedInformer;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesException;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesLeaderElector;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesPod;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesPodsWatcher;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesService;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesWatch;
import org.apache.flink.kubernetes.kubeclient.services.ServiceType;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.runtime.persistence.PossibleInconsistentStateException;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.StringUtils;
import org.apache.flink.util.concurrent.FutureUtils;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.LoadBalancerIngress;
import io.fabric8.kubernetes.api.model.NodeAddress;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** The implementation of {@link FlinkKubeClient}. */
public class Fabric8FlinkKubeClient implements FlinkKubeClient {

    private static final Logger LOG = LoggerFactory.getLogger(Fabric8FlinkKubeClient.class);

    private final String clusterId;
    private final String namespace;
    private final int maxRetryAttempts;
    private final KubernetesConfigOptions.NodePortAddressType nodePortAddressType;

    private final NamespacedKubernetesClient internalClient;
    private final ExecutorService kubeClientExecutorService;
    // save the master deployment atomic reference for setting owner reference of task manager pods
    private final AtomicReference<Deployment> masterDeploymentRef;

    public Fabric8FlinkKubeClient(
            Configuration flinkConfig,
            NamespacedKubernetesClient client,
            ExecutorService executorService) {
        this.clusterId =
                flinkConfig
                        .getOptional(KubernetesConfigOptions.CLUSTER_ID)
                        .orElseThrow(
                                () ->
                                        new IllegalArgumentException(
                                                String.format(
                                                        "Configuration option '%s' is not set.",
                                                        KubernetesConfigOptions.CLUSTER_ID.key())));
        this.namespace = flinkConfig.getString(KubernetesConfigOptions.NAMESPACE);
        this.maxRetryAttempts =
                flinkConfig.getInteger(
                        KubernetesConfigOptions.KUBERNETES_TRANSACTIONAL_OPERATION_MAX_RETRIES);
        this.nodePortAddressType =
                flinkConfig.get(
                        KubernetesConfigOptions.REST_SERVICE_EXPOSED_NODE_PORT_ADDRESS_TYPE);
        this.internalClient = checkNotNull(client);
        this.kubeClientExecutorService = checkNotNull(executorService);
        this.masterDeploymentRef = new AtomicReference<>();
    }

    @Override
    public void createJobManagerComponent(KubernetesJobManagerSpecification kubernetesJMSpec) {
        final Deployment deployment = kubernetesJMSpec.getDeployment();
        final List<HasMetadata> accompanyingResources = kubernetesJMSpec.getAccompanyingResources();

        // create Deployment
        LOG.debug(
                "Start to create deployment with spec {}{}",
                System.lineSeparator(),
                KubernetesUtils.tryToGetPrettyPrintYaml(deployment));
        final Deployment createdDeployment =
                this.internalClient.apps().deployments().create(deployment);

        // Note that we should use the uid of the created Deployment for the OwnerReference.
        setOwnerReference(createdDeployment, accompanyingResources);

        this.internalClient.resourceList(accompanyingResources).createOrReplace();
    }

    @Override
    public CompletableFuture<Void> createTaskManagerPod(KubernetesPod kubernetesPod) {
        return CompletableFuture.runAsync(
                () -> {
                    if (masterDeploymentRef.get() == null) {
                        final Deployment masterDeployment =
                                this.internalClient
                                        .apps()
                                        .deployments()
                                        .withName(KubernetesUtils.getDeploymentName(clusterId))
                                        .get();
                        if (masterDeployment == null) {
                            throw new RuntimeException(
                                    "Failed to find Deployment named "
                                            + clusterId
                                            + " in namespace "
                                            + this.namespace);
                        }
                        masterDeploymentRef.compareAndSet(null, masterDeployment);
                    }

                    // Note that we should use the uid of the master Deployment for the
                    // OwnerReference.
                    setOwnerReference(
                            checkNotNull(masterDeploymentRef.get()),
                            Collections.singletonList(kubernetesPod.getInternalResource()));

                    LOG.debug(
                            "Start to create pod with spec {}{}",
                            System.lineSeparator(),
                            KubernetesUtils.tryToGetPrettyPrintYaml(
                                    kubernetesPod.getInternalResource()));

                    this.internalClient.pods().create(kubernetesPod.getInternalResource());
                },
                kubeClientExecutorService);
    }

    @Override
    public CompletableFuture<Void> stopPod(String podName) {
        return CompletableFuture.runAsync(
                () -> this.internalClient.pods().withName(podName).delete(),
                kubeClientExecutorService);
    }

    @Override
    public Optional<Endpoint> getRestEndpoint(String clusterId) {
        Optional<KubernetesService> restService =
                getService(KubernetesService.ServiceType.REST_SERVICE, clusterId);
        if (!restService.isPresent()) {
            return Optional.empty();
        }
        final Service service = restService.get().getInternalResource();
        final int restPort = getRestPortFromExternalService(service);
        return getRestEndPointFromService(service, restPort);
    }

    @Override
    public List<KubernetesPod> getPodsWithLabels(Map<String, String> labels) {
        final List<Pod> podList = this.internalClient.pods().withLabels(labels).list().getItems();

        if (podList == null || podList.isEmpty()) {
            return new ArrayList<>();
        }

        return podList.stream().map(KubernetesPod::new).collect(Collectors.toList());
    }

    @Override
    public void stopAndCleanupCluster(String clusterId) {
        this.internalClient
                .apps()
                .deployments()
                .withName(KubernetesUtils.getDeploymentName(clusterId))
                .cascading(true)
                .delete();
    }

    @Override
    public Optional<KubernetesService> getService(
            KubernetesService.ServiceType serviceType, String clusterId) {
        final String serviceName = getServiceName(serviceType, clusterId);
        final Service service =
                this.internalClient.services().withName(serviceName).fromServer().get();
        if (service == null) {
            LOG.debug("Service {} does not exist", serviceName);
            return Optional.empty();
        }
        return Optional.of(new KubernetesService(service));
    }

    @Override
    public KubernetesWatch watchPodsAndDoCallback(
            Map<String, String> labels, WatchCallbackHandler<KubernetesPod> podCallbackHandler)
            throws Exception {
        return FutureUtils.retry(
                        () ->
                                CompletableFuture.supplyAsync(
                                        () ->
                                                new KubernetesWatch(
                                                        this.internalClient
                                                                .pods()
                                                                .withLabels(labels)
                                                                .watch(
                                                                        new KubernetesPodsWatcher(
                                                                                podCallbackHandler))),
                                        kubeClientExecutorService),
                        maxRetryAttempts,
                        t ->
                                ExceptionUtils.findThrowable(t, KubernetesClientException.class)
                                        .isPresent(),
                        kubeClientExecutorService)
                .get();
    }

    @Override
    public KubernetesLeaderElector createLeaderElector(
            KubernetesLeaderElectionConfiguration leaderElectionConfiguration,
            KubernetesLeaderElector.LeaderCallbackHandler leaderCallbackHandler) {
        return new KubernetesLeaderElector(
                this.internalClient, leaderElectionConfiguration, leaderCallbackHandler);
    }

    @Override
    public CompletableFuture<Void> createConfigMap(KubernetesConfigMap configMap) {
        final String configMapName = configMap.getName();
        return CompletableFuture.runAsync(
                        () ->
                                this.internalClient
                                        .configMaps()
                                        .create(configMap.getInternalResource()),
                        kubeClientExecutorService)
                .exceptionally(
                        throwable -> {
                            throw new CompletionException(
                                    new KubernetesException(
                                            "Failed to create ConfigMap " + configMapName,
                                            throwable));
                        });
    }

    @Override
    public Optional<KubernetesConfigMap> getConfigMap(String name) {
        final ConfigMap configMap = this.internalClient.configMaps().withName(name).get();
        return configMap == null
                ? Optional.empty()
                : Optional.of(new KubernetesConfigMap(configMap));
    }

    @Override
    public CompletableFuture<Boolean> checkAndUpdateConfigMap(
            String configMapName,
            Function<KubernetesConfigMap, Optional<KubernetesConfigMap>> updateFunction) {
        return FutureUtils.retry(
                () -> attemptCheckAndUpdateConfigMap(configMapName, updateFunction),
                maxRetryAttempts,
                // Only KubernetesClientException is retryable
                t -> ExceptionUtils.findThrowable(t, KubernetesClientException.class).isPresent(),
                kubeClientExecutorService);
    }

    private CompletableFuture<Boolean> attemptCheckAndUpdateConfigMap(
            String configMapName,
            Function<KubernetesConfigMap, Optional<KubernetesConfigMap>> updateFunction) {
        return CompletableFuture.supplyAsync(
                () -> {
                    final KubernetesConfigMap configMap =
                            getConfigMap(configMapName)
                                    .orElseThrow(
                                            () ->
                                                    new CompletionException(
                                                            new KubernetesException(
                                                                    "Cannot retry checkAndUpdateConfigMap with configMap "
                                                                            + configMapName
                                                                            + " because it does not exist.")));
                    final Optional<KubernetesConfigMap> maybeUpdate =
                            updateFunction.apply(configMap);
                    if (maybeUpdate.isPresent()) {
                        try {
                            internalClient
                                    .configMaps()
                                    .withName(configMapName)
                                    .lockResourceVersion(maybeUpdate.get().getResourceVersion())
                                    .replace(maybeUpdate.get().getInternalResource());
                            return true;
                        } catch (Throwable throwable) {
                            LOG.debug(
                                    "Failed to update ConfigMap {} with data {}. Trying again.",
                                    configMap.getName(),
                                    configMap.getData());
                            // the client implementation does not expose the different kind of error
                            // causes to a degree that we could do a more fine-grained error
                            // handling here
                            throw new CompletionException(
                                    new PossibleInconsistentStateException(throwable));
                        }
                    }
                    return false;
                },
                kubeClientExecutorService);
    }

    @Override
    public CompletableFuture<Void> deleteConfigMapsByLabels(Map<String, String> labels) {
        // the only time, the delete method returns false is due to a 404 HTTP status which is
        // returned if the underlying resource doesn't exist
        return CompletableFuture.runAsync(
                () -> this.internalClient.configMaps().withLabels(labels).delete(),
                kubeClientExecutorService);
    }

    @Override
    public CompletableFuture<Void> deleteConfigMap(String configMapName) {
        // the only time, the delete method returns false is due to a 404 HTTP status which is
        // returned if the underlying resource doesn't exist
        return CompletableFuture.runAsync(
                () -> this.internalClient.configMaps().withName(configMapName).delete(),
                kubeClientExecutorService);
    }

    @Override
    public KubernetesConfigMapSharedWatcher createConfigMapSharedWatcher(
            Map<String, String> labels) {
        return new KubernetesConfigMapSharedInformer(this.internalClient, labels);
    }

    @Override
    public void close() {
        this.internalClient.close();
        ExecutorUtils.gracefulShutdown(5, TimeUnit.SECONDS, this.kubeClientExecutorService);
    }

    @Override
    public KubernetesPod loadPodFromTemplateFile(File file) {
        if (!file.exists()) {
            throw new FlinkRuntimeException(
                    String.format("Pod template file %s does not exist.", file));
        }
        return new KubernetesPod(this.internalClient.pods().load(file).get());
    }

    @Override
    public CompletableFuture<Void> updateServiceTargetPort(
            KubernetesService.ServiceType serviceType,
            String clusterId,
            String portName,
            int targetPort) {
        LOG.debug("Update {} target port to {}", portName, targetPort);
        return CompletableFuture.runAsync(
                () ->
                        getService(serviceType, clusterId)
                                .ifPresent(
                                        service -> {
                                            final Service updatedService =
                                                    new ServiceBuilder(
                                                                    service.getInternalResource())
                                                            .editSpec()
                                                            .editMatchingPort(
                                                                    servicePortBuilder ->
                                                                            servicePortBuilder
                                                                                    .build()
                                                                                    .getName()
                                                                                    .equals(
                                                                                            portName))
                                                            .withTargetPort(
                                                                    new IntOrString(targetPort))
                                                            .endPort()
                                                            .endSpec()
                                                            .build();
                                            this.internalClient
                                                    .services()
                                                    .withName(
                                                            getServiceName(serviceType, clusterId))
                                                    .replace(updatedService);
                                        }),
                kubeClientExecutorService);
    }

    /**
     * Get the Kubernetes service name.
     *
     * @param serviceType The service type
     * @param clusterId The cluster id
     * @return Return the Kubernetes service name if the service type is known.
     */
    private String getServiceName(KubernetesService.ServiceType serviceType, String clusterId) {
        switch (serviceType) {
            case REST_SERVICE:
                return ExternalServiceDecorator.getExternalServiceName(clusterId);
            case INTERNAL_SERVICE:
                return InternalServiceDecorator.getInternalServiceName(clusterId);
            default:
                throw new IllegalArgumentException(
                        "Unrecognized service type: " + serviceType.name());
        }
    }

    private void setOwnerReference(Deployment deployment, List<HasMetadata> resources) {
        final OwnerReference deploymentOwnerReference =
                new OwnerReferenceBuilder()
                        .withName(deployment.getMetadata().getName())
                        .withApiVersion(deployment.getApiVersion())
                        .withUid(deployment.getMetadata().getUid())
                        .withKind(deployment.getKind())
                        .withController(true)
                        .withBlockOwnerDeletion(true)
                        .build();
        resources.forEach(
                resource ->
                        resource.getMetadata()
                                .setOwnerReferences(
                                        Collections.singletonList(deploymentOwnerReference)));
    }

    /** Get rest port from the external Service. */
    private int getRestPortFromExternalService(Service externalService) {
        final List<ServicePort> servicePortCandidates =
                externalService.getSpec().getPorts().stream()
                        .filter(x -> x.getName().equals(Constants.REST_PORT_NAME))
                        .collect(Collectors.toList());

        if (servicePortCandidates.isEmpty()) {
            throw new RuntimeException(
                    "Failed to find port \""
                            + Constants.REST_PORT_NAME
                            + "\" in Service \""
                            + ExternalServiceDecorator.getExternalServiceName(this.clusterId)
                            + "\"");
        }

        final ServicePort externalServicePort = servicePortCandidates.get(0);

        final KubernetesConfigOptions.ServiceExposedType externalServiceType =
                KubernetesConfigOptions.ServiceExposedType.valueOf(
                        externalService.getSpec().getType());

        switch (externalServiceType) {
            case ClusterIP:
            case LoadBalancer:
                return externalServicePort.getPort();
            case NodePort:
                return externalServicePort.getNodePort();
            default:
                throw new RuntimeException("Unrecognized Service type: " + externalServiceType);
        }
    }

    private Optional<Endpoint> getRestEndPointFromService(Service service, int restPort) {

        final KubernetesConfigOptions.ServiceExposedType serviceExposedType =
                ServiceType.classify(service);

        switch (serviceExposedType) {
            case ClusterIP:
            case Headless_ClusterIP:
                return getClusterIPRestEndpoint(restPort);
            case LoadBalancer:
                return getLoadBalancerRestEndpoint(service, restPort);
            case NodePort:
                if (service.getSpec() != null
                        && !CollectionUtil.isNullOrEmpty(service.getSpec().getExternalIPs())) {
                    final String address = service.getSpec().getExternalIPs().get(0);
                    if (address != null && !address.isEmpty()) {
                        return Optional.of(new Endpoint(address, restPort));
                    }
                }
                return getNodePortRestEndpoint(restPort);
        }
        return Optional.empty();
    }

    private Optional<Endpoint> getClusterIPRestEndpoint(int restPort) {
        return Optional.of(
                new Endpoint(
                        ExternalServiceDecorator.getNamespacedExternalServiceName(
                                clusterId, namespace),
                        restPort));
    }

    private Optional<Endpoint> getNodePortRestEndpoint(int restPort) {
        final String address =
                internalClient.nodes().list().getItems().stream()
                        .flatMap(node -> node.getStatus().getAddresses().stream())
                        .filter(
                                nodeAddress ->
                                        nodePortAddressType.name().equals(nodeAddress.getType()))
                        .map(NodeAddress::getAddress)
                        .filter(ip -> !ip.isEmpty())
                        .findAny()
                        .orElse(null);

        return StringUtils.isNullOrWhitespaceOnly(address)
                ? Optional.empty()
                : Optional.of(new Endpoint(address, restPort));
    }

    private Optional<Endpoint> getLoadBalancerRestEndpoint(Service service, int restPort) {
        if (null != service.getStatus()
                && null != service.getStatus().getLoadBalancer()
                && !CollectionUtil.isNullOrEmpty(
                        service.getStatus().getLoadBalancer().getIngress())) {
            LoadBalancerIngress loadBalancerIngress =
                    service.getStatus().getLoadBalancer().getIngress().get(0);
            String address = loadBalancerIngress.getIp();
            // Use hostname when the ip address is null
            if (address == null || address.isEmpty()) {
                address = loadBalancerIngress.getHostname();
            }
            return StringUtils.isNullOrWhitespaceOnly(address)
                    ? Optional.empty()
                    : Optional.of(new Endpoint(address, restPort));
        }
        LOG.error(
                "LoadBalancer is temporarily unavailable, which will cause the Flink client to not be able to connect to the JobMaster."
                        + " Please contact your cloud service provider, or select another service type (parameter configuration is `kubernetes.rest-service.exposed.type`)");
        throw new FlinkRuntimeException("LoadBalancer is temporarily unavailable");
    }
}
