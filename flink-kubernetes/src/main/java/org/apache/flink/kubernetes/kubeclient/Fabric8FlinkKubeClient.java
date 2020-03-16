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

import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.decorators.ConfigMapDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.Decorator;
import org.apache.flink.kubernetes.kubeclient.decorators.FlinkMasterDeploymentDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.InitializerDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.OwnerReferenceDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.ServiceDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.TaskManagerPodDecorator;
import org.apache.flink.kubernetes.kubeclient.resources.ActionWatcher;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesConfigMap;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesDeployment;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesPod;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesPodsWatcher;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesService;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesWatch;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.TimeUtils;
import org.apache.flink.util.function.FunctionUtils;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.LoadBalancerStatus;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The implementation of {@link FlinkKubeClient}.
 */
public class Fabric8FlinkKubeClient implements FlinkKubeClient {

	private static final Logger LOG = LoggerFactory.getLogger(Fabric8FlinkKubeClient.class);

	private final Configuration flinkConfig;
	private final KubernetesClient internalClient;
	private final String clusterId;
	private final String nameSpace;

	private final List<Decorator<ConfigMap, KubernetesConfigMap>> configMapDecorators = new ArrayList<>();
	private final List<Decorator<Service, KubernetesService>> internalServiceDecorators = new ArrayList<>();
	private final List<Decorator<Service, KubernetesService>> restServiceDecorators = new ArrayList<>();
	private final List<Decorator<Deployment, KubernetesDeployment>> flinkMasterDeploymentDecorators = new ArrayList<>();
	private final List<Decorator<Pod, KubernetesPod>> taskManagerPodDecorators = new ArrayList<>();

	private final ExecutorService kubeClientExecutorService;

	public Fabric8FlinkKubeClient(
			Configuration flinkConfig,
			KubernetesClient client,
			Supplier<ExecutorService> asyncExecutorFactory) {
		this.flinkConfig = checkNotNull(flinkConfig);
		this.internalClient = checkNotNull(client);
		this.clusterId = checkNotNull(flinkConfig.getString(KubernetesConfigOptions.CLUSTER_ID));

		this.nameSpace = flinkConfig.getString(KubernetesConfigOptions.NAMESPACE);

		this.kubeClientExecutorService = asyncExecutorFactory.get();

		initialize();
	}

	private void initialize() {
		this.configMapDecorators.add(new InitializerDecorator<>(Constants.CONFIG_MAP_PREFIX + clusterId));
		this.configMapDecorators.add(new OwnerReferenceDecorator<>());
		this.configMapDecorators.add(new ConfigMapDecorator());

		this.internalServiceDecorators.add(new InitializerDecorator<>(clusterId));
		this.internalServiceDecorators.add(new ServiceDecorator(
			KubernetesConfigOptions.ServiceExposedType.ClusterIP,
			false));

		this.restServiceDecorators.add(new InitializerDecorator<>(clusterId + Constants.FLINK_REST_SERVICE_SUFFIX));
		final String exposedType = flinkConfig.getString(KubernetesConfigOptions.REST_SERVICE_EXPOSED_TYPE);
		this.restServiceDecorators.add(new ServiceDecorator(
			KubernetesConfigOptions.ServiceExposedType.valueOf(exposedType),
			true));
		this.restServiceDecorators.add(new OwnerReferenceDecorator<>());

		this.flinkMasterDeploymentDecorators.add(new InitializerDecorator<>(clusterId, Constants.APPS_API_VERSION));
		this.flinkMasterDeploymentDecorators.add(new OwnerReferenceDecorator<>(Constants.APPS_API_VERSION));

		this.taskManagerPodDecorators.add(new InitializerDecorator<>());
		this.taskManagerPodDecorators.add(new OwnerReferenceDecorator<>());
	}

	@Override
	public void createConfigMap() {
		KubernetesConfigMap configMap = new KubernetesConfigMap(this.flinkConfig);

		for (Decorator<ConfigMap, KubernetesConfigMap> c : this.configMapDecorators) {
			configMap = c.decorate(configMap);
		}

		LOG.debug("Create config map with data size {}", configMap.getInternalResource().getData().size());
		this.internalClient.configMaps().create(configMap.getInternalResource());
	}

	@Override
	public CompletableFuture<KubernetesService> createInternalService(String clusterId) {
		return createService(clusterId, this.internalServiceDecorators);
	}

	@Override
	public CompletableFuture<KubernetesService> createRestService(String clusterId) {
		return createService(clusterId + Constants.FLINK_REST_SERVICE_SUFFIX, this.restServiceDecorators);
	}

	@Override
	public void createFlinkMasterDeployment(ClusterSpecification clusterSpecification) {
		KubernetesDeployment deployment = new KubernetesDeployment(this.flinkConfig);

		for (Decorator<Deployment, KubernetesDeployment> d : this.flinkMasterDeploymentDecorators) {
			deployment = d.decorate(deployment);
		}

		deployment = new FlinkMasterDeploymentDecorator(clusterSpecification).decorate(deployment);

		LOG.debug("Create Flink Master deployment with spec: {}", deployment.getInternalResource().getSpec());

		this.internalClient
			.apps()
			.deployments()
			.inNamespace(this.nameSpace)
			.create(deployment.getInternalResource());
	}

	@Override
	public CompletableFuture<Void> createTaskManagerPod(TaskManagerPodParameter parameter) {
		return CompletableFuture.runAsync(
			() -> {
				KubernetesPod pod = new KubernetesPod(this.flinkConfig);

				for (Decorator<Pod, KubernetesPod> d : this.taskManagerPodDecorators) {
					pod = d.decorate(pod);
				}

				pod = new TaskManagerPodDecorator(parameter).decorate(pod);

				LOG.debug("Create TaskManager pod with spec: {}", pod.getInternalResource().getSpec());

				this.internalClient.pods().inNamespace(this.nameSpace).create(pod.getInternalResource());
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
		int restPort = this.flinkConfig.getInteger(RestOptions.PORT);
		String serviceExposedType = flinkConfig.getString(KubernetesConfigOptions.REST_SERVICE_EXPOSED_TYPE);

		// Return the service.namespace directly when use ClusterIP.
		if (serviceExposedType.equals(KubernetesConfigOptions.ServiceExposedType.ClusterIP.toString())) {
			return Optional.of(new Endpoint(clusterId + "." + nameSpace, restPort));
		}
		return getRestService(clusterId)
			.flatMap(restService -> getRestEndPointFromService(restService.getInternalResource(), restPort));
	}

	@Override
	public List<KubernetesPod> getPodsWithLabels(Map<String, String> labels) {
		final List<Pod> podList = this.internalClient.pods().withLabels(labels).list().getItems();

		if (podList == null || podList.isEmpty()) {
			return new ArrayList<>();
		}

		return podList
			.stream()
			.map(e -> new KubernetesPod(flinkConfig, e))
			.collect(Collectors.toList());
	}

	@Override
	public void stopAndCleanupCluster(String clusterId) {
		this.internalClient.services().inNamespace(this.nameSpace).withName(clusterId).cascading(true).delete();
	}

	@Override
	public void handleException(Exception e) {
		LOG.error("A Kubernetes exception occurred.", e);
	}

	@Override
	public Optional<KubernetesService> getInternalService(String clusterId) {
		return getService(clusterId);
	}

	@Override
	public Optional<KubernetesService> getRestService(String clusterId) {
		return getService(clusterId + Constants.FLINK_REST_SERVICE_SUFFIX);
	}

	@Override
	public KubernetesWatch watchPodsAndDoCallback(Map<String, String> labels, PodCallbackHandler podCallbackHandler) {
		return new KubernetesWatch(
			flinkConfig,
			this.internalClient.pods()
				.withLabels(labels)
				.watch(new KubernetesPodsWatcher(flinkConfig, podCallbackHandler)));
	}

	@Override
	public void close() {
		this.internalClient.close();
		ExecutorUtils.gracefulShutdown(5, TimeUnit.SECONDS, this.kubeClientExecutorService);
	}

	private CompletableFuture<KubernetesService> createService(
			String serviceName,
			List<Decorator<Service, KubernetesService>> serviceDecorators) {
		KubernetesService kubernetesService = new KubernetesService(this.flinkConfig);
		for (Decorator<Service, KubernetesService> d : serviceDecorators) {
			kubernetesService = d.decorate(kubernetesService);
		}

		LOG.debug("Create service {} with spec: {}", serviceName, kubernetesService.getInternalResource().getSpec());

		this.internalClient.services().create(kubernetesService.getInternalResource());

		final ActionWatcher<Service> watcher = new ActionWatcher<>(
			Watcher.Action.ADDED,
			kubernetesService.getInternalResource());

		final Watch watchConnectionManager = this.internalClient
			.services()
			.inNamespace(this.nameSpace)
			.withName(serviceName)
			.watch(watcher);

		final Duration timeout = TimeUtils.parseDuration(
			flinkConfig.get(KubernetesConfigOptions.SERVICE_CREATE_TIMEOUT));

		return CompletableFuture.supplyAsync(
			FunctionUtils.uncheckedSupplier(() -> {
				final Service createdService = watcher.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
				watchConnectionManager.close();

				return new KubernetesService(this.flinkConfig, createdService);
			}));
	}

	private Optional<KubernetesService> getService(String serviceName) {
		final Service service = this
			.internalClient
			.services()
			.inNamespace(nameSpace)
			.withName(serviceName)
			.fromServer()
			.get();

		if (service == null) {
			LOG.debug("Service {} does not exist", serviceName);
			return Optional.empty();
		}

		return Optional.of(new KubernetesService(flinkConfig, service));
	}

	/**
	 * To get nodePort of configured ports.
	 */
	private int getServiceNodePort(Service service, ConfigOption<Integer> configPort) {
		final int port = this.flinkConfig.getInteger(configPort);
		if (service.getSpec() != null && service.getSpec().getPorts() != null) {
			for (ServicePort p : service.getSpec().getPorts()) {
				if (p.getPort() == port) {
					return p.getNodePort();
				}
			}
		}
		return port;
	}

	private Optional<Endpoint> getRestEndPointFromService(Service service, int restPort) {
		if (service.getStatus() == null) {
			return Optional.empty();
		}

		LoadBalancerStatus loadBalancer = service.getStatus().getLoadBalancer();
		boolean hasExternalIP = service.getSpec() != null &&
			service.getSpec().getExternalIPs() != null && !service.getSpec().getExternalIPs().isEmpty();

		if (loadBalancer != null) {
			return getLoadBalancerRestEndpoint(loadBalancer, service, restPort);
		} else if (hasExternalIP) {
			final String address = service.getSpec().getExternalIPs().get(0);
			if (address != null && !address.isEmpty()) {
				return Optional.of(new Endpoint(address, restPort));
			}
		}
		return Optional.empty();
	}

	private Optional<Endpoint> getLoadBalancerRestEndpoint(LoadBalancerStatus loadBalancer, Service svc, int restPort) {
		boolean hasIngress = loadBalancer.getIngress() != null && !loadBalancer.getIngress().isEmpty();
		String address;
		int port = restPort;
		if (hasIngress) {
			address = loadBalancer.getIngress().get(0).getIp();
			// Use hostname when the ip address is null
			if (address == null || address.isEmpty()) {
				address = loadBalancer.getIngress().get(0).getHostname();
			}
		} else {
			// Use node port
			address = this.internalClient.getMasterUrl().getHost();
			port = getServiceNodePort(svc, RestOptions.PORT);
		}
		boolean noAddress = address == null || address.isEmpty();
		return noAddress ? Optional.empty() : Optional.of(new Endpoint(address, port));
	}
}
