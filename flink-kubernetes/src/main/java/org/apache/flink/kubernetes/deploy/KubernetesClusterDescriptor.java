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

package org.apache.flink.kubernetes.deploy;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.client.deployment.ClusterDeploymentException;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.ClusterRetrieveException;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.kubernetes.cli.KubernetesRestClusterClient;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.entrypoint.KubernetesJobClusterEntrypoint;
import org.apache.flink.kubernetes.entrypoint.KubernetesSessionClusterEntrypoint;
import org.apache.flink.kubernetes.utils.KubernetesClientFactory;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.util.Preconditions;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.KeyToPath;
import io.fabric8.kubernetes.api.model.LoadBalancerIngress;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.QuantityBuilder;
import io.fabric8.kubernetes.api.model.ReplicationController;
import io.fabric8.kubernetes.api.model.ReplicationControllerBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.configuration.ConfigConstants.ENV_FLINK_CONF_DIR;
import static org.apache.flink.configuration.GlobalConfiguration.FLINK_CONF_FILENAME;
import static org.apache.flink.kubernetes.configuration.Constants.CONFIG_FILE_LOG4J_NAME;
import static org.apache.flink.kubernetes.configuration.Constants.ENV_FLINK_CLASSPATH;
import static org.apache.flink.kubernetes.configuration.Constants.FILES_SEPARATOR;
import static org.apache.flink.kubernetes.configuration.Constants.FLINK_CONF_VOLUME;
import static org.apache.flink.kubernetes.configuration.Constants.JOBMANAGER_BLOB_PORT;
import static org.apache.flink.kubernetes.configuration.Constants.JOBMANAGER_RC_NAME_SUFFIX;
import static org.apache.flink.kubernetes.configuration.Constants.JOBMANAGER_REST_PORT;
import static org.apache.flink.kubernetes.configuration.Constants.JOBMANAGER_RPC_PORT;
import static org.apache.flink.kubernetes.configuration.Constants.JOB_MANAGER_CONFIG_MAP_SUFFIX;
import static org.apache.flink.kubernetes.configuration.Constants.SERVICE_NAME_SUFFIX;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The descriptor with deployment information for spawning or resuming a KubernetesClusterClient.
 */
public class KubernetesClusterDescriptor implements ClusterDescriptor<KubernetesClusterId> {

	private static final Logger LOG = LoggerFactory.getLogger(KubernetesClusterDescriptor.class);

	private String configurationDirectory;

	private final Configuration flinkConfiguration;

	private KubernetesClient kubernetesClient;

	public KubernetesClusterDescriptor(
			Configuration flinkConfiguration,
			String configurationDir) {
		this.flinkConfiguration = Preconditions.checkNotNull(flinkConfiguration);
		this.configurationDirectory = configurationDir;
		this.kubernetesClient = KubernetesClientFactory.create(flinkConfiguration);
	}

	@VisibleForTesting
	public KubernetesClusterDescriptor(
		Configuration flinkConfiguration,
		String configurationDir,
		KubernetesClient client) {
		this(flinkConfiguration, configurationDir);
		kubernetesClient = client;
	}

	@Override
	public String getClusterDescription() {
		// TODO return kubernetes cluster information, including resource
		return null;
	}

	@Override
	public ClusterClient<KubernetesClusterId> retrieve(KubernetesClusterId clusterId) throws ClusterRetrieveException {
		try {
			return new KubernetesRestClusterClient<>(this, getClientEffectiveConfiguration(), clusterId);
		} catch (Exception e) {
			throw new ClusterRetrieveException(
				String.format("Could not retrieve the cluster %s, %s", clusterId, e.getMessage()));
		}
	}

	@Override
	public ClusterClient<KubernetesClusterId> deploySessionCluster(ClusterSpecification clusterSpecification)
			throws ClusterDeploymentException {
		try {
			return deployInternal(getSessionClusterEntrypoint(), clusterSpecification, true);
		} catch (Exception e) {
			throw new ClusterDeploymentException("Couldn't deploy Kubernetes session cluster", e);
		}
	}

	@Override
	public void killCluster(KubernetesClusterId clusterId) {
		kubernetesClient.services().withName(clusterId.toString() + SERVICE_NAME_SUFFIX).delete();
	}

	@Override
	public ClusterClient<KubernetesClusterId> deployJobCluster(ClusterSpecification clusterSpecification,
			JobGraph jobGraph, boolean detached)
			throws ClusterDeploymentException {
		final ClusterEntrypoint.ExecutionMode executionMode = detached ?
			ClusterEntrypoint.ExecutionMode.DETACHED
			: ClusterEntrypoint.ExecutionMode.NORMAL;
		flinkConfiguration.setString(ClusterEntrypoint.EXECUTION_MODE, executionMode.toString());
		try {
			return deployInternal(getJobClusterEntrypoint(), clusterSpecification, false);
		} catch (Exception e) {
			throw new ClusterDeploymentException("Couldn't deploy Kubernetes job cluster", e);
		}
	}

	@Override
	public void close() {
		kubernetesClient.close();
	}

	/**
	 * The class to start the job manager with. This class runs the main
	 * method in case of session cluster.
	 */
	private String getSessionClusterEntrypoint() {
		return KubernetesSessionClusterEntrypoint.class.getName();
	}

	/**
	 * The class to start the job manager with. This class runs the main
	 * method in case of job cluster.
	 */
	private String getJobClusterEntrypoint() {
		return KubernetesJobClusterEntrypoint.class.getName();
	}

	private ClusterClient<KubernetesClusterId> deployInternal(
		String entryPoint,
		ClusterSpecification clusterSpecification,
		Boolean waitForLaunched) throws Exception {
		String customName = flinkConfiguration.getString(KubernetesConfigOptions.CLUSTER_ID);
		KubernetesClusterId clusterId = (customName != null ? KubernetesClusterId.fromString(customName)
			: KubernetesClusterId.getInstance());
		Map<String, String> labels = new HashMap<>();
		labels.put("app", clusterId.toString());
		labels.put("component", "jobmanager");
		startJobManager(entryPoint, clusterSpecification, clusterId, labels);

		if (waitForLaunched) {
			waitForPodLaunched(labels);

			int restPort = flinkConfiguration.getInteger(RestOptions.PORT);
			String serviceName = clusterId.toString() + SERVICE_NAME_SUFFIX;
			Service service = kubernetesClient.services().withName(serviceName).get();
			checkNotNull(service);
			String serviceExposedType = service.getSpec().getType();
			switch (KubernetesConfigOptions.ServiceExposedType.fromString(serviceExposedType)) {
				case CLUSTER_IP :
					LOG.info("Service exposed type is {}, you need to start a local proxy to " +
						"submit job and provide dashboard. " +
						"e.g. kubectl port-forward service/{} {}", serviceExposedType, serviceName, restPort);
					break;
				case NODE_PORT:
				case EXTERNAL_NAME:
					// TODO support NodePort and ExternalName
					LOG.info("Not supported exposed type {}", serviceExposedType);
					break;
				case LOAD_BALANCER:
					String exposedIp = waitForExposedIp(serviceName);
					flinkConfiguration.setString(KubernetesConfigOptions.SERVICE_EXPOSED_ADDRESS, exposedIp);
					LOG.info("Service exposed type is {}, {}:{} could be used to submit job and view dashboard.",
						serviceExposedType, exposedIp, restPort);
					break;
				default:
					break;
			}
		}

		return new KubernetesRestClusterClient<>(this, getClientEffectiveConfiguration(), clusterId);
	}

	@VisibleForTesting
	public void startJobManager(String entryPoint, ClusterSpecification clusterSpecification,
			KubernetesClusterId clusterId, Map<String, String> labels) {

		// 1. create configMap.
		ConfigMap configMap = createConfigMap(clusterId);

		// 2. create the jobmanager replication controller.
		Container container = createJobManagerContainer(entryPoint, clusterSpecification.getMasterMemoryMB());
		ReplicationController jobManagerRc = createReplicationController(clusterId, labels, container, configMap);

		// 3. create the service.
		Service service = createService(clusterId, labels);

		// 4. set owner reference.
		setJobManagerServiceOwnerReference(service, jobManagerRc, configMap);
		kubernetesClient.configMaps().create(configMap);
		kubernetesClient.replicationControllers().create(jobManagerRc);
	}

	private ReplicationController createReplicationController(KubernetesClusterId clusterId,
			Map<String, String> labels, Container container, ConfigMap configMap) {
		String jobManagerRcName = clusterId.toString() + JOBMANAGER_RC_NAME_SUFFIX;
		String configMapName = clusterId.toString() + JOB_MANAGER_CONFIG_MAP_SUFFIX;
		String serviceAccount = flinkConfiguration.getString(KubernetesConfigOptions.JOB_MANAGER_SERVICE_ACCOUNT);
		List<KeyToPath> configMapItems = configMap.getData().keySet().stream()
			.map(e -> new KeyToPath(e, null, e)).collect(Collectors.toList());
		return new ReplicationControllerBuilder()
			.editOrNewMetadata()
				.withName(jobManagerRcName)
				.endMetadata()
			.editOrNewSpec()
				.withNewReplicas(1)
				.withSelector(labels)
				.withNewTemplate()
					.editOrNewMetadata()
						.withLabels(labels)
						.endMetadata()
					.editOrNewSpec()
						.withServiceAccountName(serviceAccount)
						.addToContainers(container)
						.addNewVolume()
							.withName(FLINK_CONF_VOLUME)
							.withNewConfigMap()
								.withName(configMapName)
								.addAllToItems(configMapItems)
								.endConfigMap()
							.endVolume()
						.endSpec()
					.endTemplate()
				.endSpec()
			.build();
	}

	@VisibleForTesting
	public Container createJobManagerContainer(String clusterEntrypoint, int jobManagerMemoryMb) {
		String confDir = flinkConfiguration.getString(KubernetesConfigOptions.CONF_DIR);

		String javaOpts = flinkConfiguration.getString(CoreOptions.FLINK_JVM_OPTIONS);
		if (flinkConfiguration.getString(CoreOptions.FLINK_JM_JVM_OPTIONS).length() > 0) {
			if (javaOpts.isEmpty()) {
				javaOpts = flinkConfiguration.getString(CoreOptions.FLINK_JM_JVM_OPTIONS);
			} else {
				javaOpts += " " + flinkConfiguration.getString(CoreOptions.FLINK_JM_JVM_OPTIONS);
			}
		}

		final Map<String, String> startCommandValues = new HashMap<>();
		startCommandValues.put("java", "$JAVA_HOME/bin/java");
		startCommandValues.put("classpath", "-classpath " + confDir + File.pathSeparator + "$" + ENV_FLINK_CLASSPATH);
		startCommandValues.put("jvmmem", "-Xmx" + jobManagerMemoryMb + "m");
		startCommandValues.put("jvmopts", javaOpts);
		startCommandValues.put("logging", "-Dlog4j.configuration=" + CONFIG_FILE_LOG4J_NAME);
		startCommandValues.put("class", clusterEntrypoint);

		final String commandTemplate = flinkConfiguration.getString(KubernetesConfigOptions.CONTAINER_START_COMMAND_TEMPLATE);
		final String command = BootstrapTools.getStartCommand(commandTemplate, startCommandValues);

		LOG.debug("Job manager start command: {}", command);

		String containerName = flinkConfiguration.getString(KubernetesConfigOptions.JOB_MANAGER_CONTAINER_NAME);

		String image = flinkConfiguration.getString(KubernetesConfigOptions.JOB_MANAGER_CONTAINER_IMAGE,
			flinkConfiguration.getString(KubernetesConfigOptions.CONTAINER_IMAGE));
		checkNotNull(image, "JobManager image should be specified");

		String pullPolicy = flinkConfiguration.getString(KubernetesConfigOptions.CONTAINER_IMAGE_PULL_POLICY);

		double cpu = flinkConfiguration.getDouble(KubernetesConfigOptions.JOB_MANAGER_CORE);
		Quantity jobManagerCpuQuantity = new QuantityBuilder(false)
			.withAmount(String.valueOf(cpu))
			.build();
		Quantity jobManagerMemoryQuantity = new QuantityBuilder(false)
			.withAmount(String.valueOf(((long) jobManagerMemoryMb) << 20))
			.build();

		return new ContainerBuilder()
			.withName(containerName)
			.withImage(image)
			.withImagePullPolicy(pullPolicy)
			.addNewPort()
				.withName(JOBMANAGER_RPC_PORT)
				.withContainerPort(flinkConfiguration.getInteger(JobManagerOptions.PORT))
				.withProtocol("TCP")
				.endPort()
			.addNewPort()
				.withName(JOBMANAGER_REST_PORT)
				.withContainerPort(flinkConfiguration.getInteger(RestOptions.PORT))
				.withProtocol("TCP")
				.endPort()
			.addNewPort()
				.withName(JOBMANAGER_BLOB_PORT)
				.withContainerPort(Integer.valueOf(flinkConfiguration.getString(BlobServerOptions.PORT)))
				.withProtocol("TCP")
				.endPort()
			.addNewEnv()
				.withName(ENV_FLINK_CONF_DIR)
				.withValue(confDir)
				.endEnv()
			.withArgs(Arrays.asList("/bin/bash", "-c", command))
			.editOrNewResources()
				.addToRequests("memory", jobManagerMemoryQuantity)
				.addToLimits("memory", jobManagerMemoryQuantity)
				.addToRequests("cpu", jobManagerCpuQuantity)
				.endResources()
			.withVolumeMounts(new VolumeMountBuilder()
				.withName(FLINK_CONF_VOLUME)
				.withMountPath(confDir)
				.build())
			.build();
	}

	private Service createService(KubernetesClusterId clusterId, Map<String, String> labels) {
		String exposedType = KubernetesConfigOptions.ServiceExposedType.valueOf(
			flinkConfiguration.getString(KubernetesConfigOptions.SERVICE_EXPOSED_TYPE)).getServiceExposedType();
		return kubernetesClient.services().create(new ServiceBuilder()
			.withNewMetadata()
				.withName(clusterId.toString() + SERVICE_NAME_SUFFIX)
				.endMetadata()
			.withNewSpec()
			.withType(exposedType)
			.withSelector(labels)
			.addNewPort()
				.withName(JOBMANAGER_RPC_PORT)
				.withPort(flinkConfiguration.getInteger(JobManagerOptions.PORT))
				.withProtocol("TCP")
				.endPort()
			.addNewPort()
				.withName(JOBMANAGER_REST_PORT)
				.withPort(flinkConfiguration.getInteger(RestOptions.PORT))
				.withProtocol("TCP")
				.endPort()
			.addNewPort()
				.withName(JOBMANAGER_BLOB_PORT)
				.withPort(Integer.valueOf(flinkConfiguration.getString(BlobServerOptions.PORT)))
				.withProtocol("TCP")
				.endPort()
			.endSpec()
			.build());
	}

	/**
	 * Setup a Config Map that will generate a flink-conf.yaml and log4j file.
	 * @param clusterId the cluster id
	 * @return the created configMap
	 */
	private ConfigMap createConfigMap(KubernetesClusterId clusterId) {
		StringBuilder flinkConfContent = new StringBuilder();
		flinkConfiguration.setString(KubernetesConfigOptions.CLUSTER_ID, clusterId.toString());
		flinkConfiguration.setString(HighAvailabilityOptions.HA_CLUSTER_ID, clusterId.toString());
		// TM use servicename to discover jobmanager
		flinkConfiguration.setString(JobManagerOptions.ADDRESS, clusterId.toString() + SERVICE_NAME_SUFFIX);
		// parse files
		String files = flinkConfiguration.getString(KubernetesConfigOptions.CONTAINER_FILES);
		Map<String, String> fileMap = new LinkedHashMap<>();
		if (files != null && !files.isEmpty()) {
			for (String filePath : files.split(FILES_SEPARATOR)) {
				if (filePath.indexOf(File.separatorChar) == -1) {
					filePath = configurationDirectory + File.separator + filePath;
				}
				String fileName = filePath.substring(filePath.lastIndexOf(File.separator) + 1);
				String fileContent = KubernetesUtils.getContentFromFile(filePath);
				if (fileContent != null) {
					fileMap.put(fileName, fileContent);
				} else {
					LOG.info("File {} not exist, will not add to configMap", filePath);
				}
			}
		}
		if (!fileMap.isEmpty()) {
			flinkConfiguration.setString(KubernetesConfigOptions.CONTAINER_FILES,
				StringUtils.join(fileMap.keySet(), ','));
		}
		flinkConfiguration.toMap().forEach((k, v) ->
			flinkConfContent.append(k).append(": ").append(v).append(System.lineSeparator()));

		String configMapName = clusterId.toString() + JOB_MANAGER_CONFIG_MAP_SUFFIX;
		ConfigMapBuilder configMapBuilder = new ConfigMapBuilder()
			.withNewMetadata()
			.withName(configMapName)
			.endMetadata()
			.addToData(FLINK_CONF_FILENAME, flinkConfContent.toString());
		String log4jContent = KubernetesUtils.getContentFromFile(configurationDirectory + File.separator + CONFIG_FILE_LOG4J_NAME);
		if (log4jContent != null) {
			configMapBuilder.addToData(CONFIG_FILE_LOG4J_NAME, log4jContent);
		} else {
			LOG.info("File {} not exist, will not add to configMap",
				configurationDirectory + File.separator + CONFIG_FILE_LOG4J_NAME);
		}
		if (!fileMap.isEmpty()) {
			fileMap.entrySet().stream().forEach(e -> configMapBuilder.addToData(e.getKey(), e.getValue()));
		}
		return configMapBuilder.build();
	}

	private void waitForPodLaunched(Map<String, String> labels) throws InterruptedException {
		String containerName = flinkConfiguration.getString(KubernetesConfigOptions.JOB_MANAGER_CONTAINER_NAME);
		//wait for job manager to be launched
		PodList podList;
		LOG.info("Waiting for the cluster to be allocated");
		final long startTime = System.currentTimeMillis();
		loop: while (true) {
			podList = kubernetesClient.pods().withLabels(labels).list();
			if (podList != null && podList.getItems().size() > 0) {
				for (Pod pod : podList.getItems()) {
					for (ContainerStatus containerStatus : pod.getStatus().getContainerStatuses()) {
						if (containerStatus.getState().getRunning() != null
							&& containerStatus.getName().equals(containerName)) {
							break loop;
						}
					}
				}
			}
			if (System.currentTimeMillis() - startTime > 60000) {
				LOG.info("Deployment took more than 60 seconds. " +
					"Please check if the requested resources are available in the Kubernetes cluster");
			}
			Thread.sleep(250);
		}
	}

	private String waitForExposedIp(String serviceName) throws InterruptedException {
		LOG.info("Waiting for service {} to be exposed.", serviceName);
		List<String> ipList = new ArrayList<>();
		List<LoadBalancerIngress> ingressList;
		final long startTime = System.currentTimeMillis();
		Service service;
		while (true) {
			service = kubernetesClient.services().withName(serviceName).get();
			if (service != null) {
				ingressList = service.getStatus().getLoadBalancer().getIngress();
				if (ingressList != null && ingressList.size() > 0) {
					for (LoadBalancerIngress ingress : ingressList) {
						ipList.add(ingress.getIp());
					}
					break;
				}
			}
			if (System.currentTimeMillis() - startTime > 60000) {
				LOG.info("Exposed service took more than 60 seconds, please check logs on the Kubernetes cluster.");
			}
			Thread.sleep(250);
		}
		// just return the first one load balancer ip
		return ipList.get(0);
	}

	private Configuration getClientEffectiveConfiguration() {
		Configuration configuration = new Configuration(flinkConfiguration);
		// it may be including a port, e.g. localhost:8081
		String[] address = flinkConfiguration.getString(KubernetesConfigOptions.SERVICE_EXPOSED_ADDRESS).split(":");
		configuration.setString(JobManagerOptions.ADDRESS, address[0]);
		configuration.setString(RestOptions.ADDRESS, address[0]);
		if (address.length == 2) {
			configuration.setInteger(RestOptions.PORT, Integer.parseInt(address[1]));
		}
		return configuration;
	}

	private void setJobManagerServiceOwnerReference(Service service, ReplicationController rc, ConfigMap configMap) {
		OwnerReference ownerReference = new OwnerReferenceBuilder()
			.withName(service.getMetadata().getName())
			.withApiVersion(service.getApiVersion())
			.withUid(service.getMetadata().getUid())
			.withKind(service.getKind())
			.withController(true)
			.build();
		rc.getMetadata().setOwnerReferences(Collections.singletonList(ownerReference));
		configMap.getMetadata().setOwnerReferences(Collections.singletonList(ownerReference));
	}
}
