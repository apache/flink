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

package org.apache.flink.kubernetes.utils;

import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.resources.CommonExtendedResource;
import org.apache.flink.api.common.resources.Resource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.configuration.Constants;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.entrypoint.KubernetesTaskExecutorRunner;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;
import org.apache.flink.runtime.clusterframework.standalone.TaskManagerResourceCalculator;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.TaskManagerResource;
import org.apache.flink.util.Preconditions;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.KeyToPath;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.QuantityBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.net.util.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.configuration.ConfigConstants.ENV_FLINK_CONF_DIR;
import static org.apache.flink.configuration.GlobalConfiguration.FLINK_CONF_FILENAME;
import static org.apache.flink.kubernetes.configuration.Constants.CONFIG_FILE_LOG4J_NAME;
import static org.apache.flink.kubernetes.configuration.Constants.ENV_FLINK_CLASSPATH;
import static org.apache.flink.kubernetes.configuration.Constants.FILES_SEPARATOR;
import static org.apache.flink.kubernetes.configuration.Constants.FLINK_CONF_VOLUME;
import static org.apache.flink.kubernetes.configuration.Constants.POD_RESTART_POLICY;
import static org.apache.flink.kubernetes.configuration.Constants.PROTOCOL_TCP;
import static org.apache.flink.kubernetes.configuration.Constants.RESOURCE_NAME_CPU;
import static org.apache.flink.kubernetes.configuration.Constants.RESOURCE_NAME_MEMORY;
import static org.apache.flink.kubernetes.configuration.Constants.TASK_MANAGER_RPC_PORT;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Utils for Kubernetes RM.
 */
public class KubernetesRMUtils {

	private static final Logger LOG = LoggerFactory.getLogger(KubernetesRMUtils.class);

	public static OwnerReference createOwnerReference(Service service) {
		Preconditions.checkNotNull(service,
			"Service is required to create owner reference.");
		OwnerReference ownerReference = new OwnerReferenceBuilder()
			.withName(service.getMetadata().getName())
			.withApiVersion(service.getApiVersion())
			.withUid(service.getMetadata().getUid()).withKind(service.getKind())
			.withController(true).build();
		return ownerReference;
	}

	public static ConfigMap createTaskManagerConfigMap(Configuration flinkConfig, String confDir,
		OwnerReference ownerReference, String configMapName) {
		StringBuilder flinkConfContent = new StringBuilder();
		flinkConfig.toMap().forEach((k, v) ->
			flinkConfContent.append(k).append(": ").append(v).append(System.lineSeparator()));

		ConfigMapBuilder configMapBuilder = new ConfigMapBuilder()
			.withNewMetadata()
			.withName(configMapName)
			.withOwnerReferences(ownerReference)
			.endMetadata()
			.addToData(FLINK_CONF_FILENAME, flinkConfContent.toString());
		String log4jPath = confDir + File.separator + CONFIG_FILE_LOG4J_NAME;
		String log4jContent = KubernetesUtils.getContentFromFile(log4jPath);
		if (log4jContent != null) {
			configMapBuilder.addToData(CONFIG_FILE_LOG4J_NAME, log4jContent);
		} else {
			LOG.info("File {} not exist, will not add to configMap", log4jPath);
		}
		String files = flinkConfig.getString(KubernetesConfigOptions.CONTAINER_FILES);
		if (files != null && !files.isEmpty()) {
			for (String filePath : files.split(FILES_SEPARATOR)) {
				if (filePath.indexOf(File.separatorChar) == -1) {
					filePath = confDir + File.separator + filePath;
				}
				String fileName = filePath.substring(filePath.lastIndexOf(File.separator) + 1);
				String fileContent = KubernetesUtils.getContentFromFile(filePath);
				if (fileContent != null) {
					configMapBuilder.addToData(fileName, fileContent);
				} else {
					LOG.info("File {} not exist, will not add to configMap", filePath);
				}
			}
		}
		return configMapBuilder.build();
	}

	public static Pod createTaskManagerPod(Map<String, String> labels, String name,
		String configMapName, OwnerReference ownerReference, Container container, ConfigMap configMap) {
		List<KeyToPath> configMapItems = configMap.getData().keySet().stream()
			.map(e -> new KeyToPath(e, null, e)).collect(Collectors.toList());
		return new PodBuilder()
			.editOrNewMetadata()
			.withLabels(labels)
			.withName(name)
			.withOwnerReferences(ownerReference)
			.endMetadata()
			.editOrNewSpec()
			.withRestartPolicy(POD_RESTART_POLICY)
			.addToContainers(container)
			.addNewVolume()
			.withName(FLINK_CONF_VOLUME)
			.withNewConfigMap()
			.withName(configMapName)
			.addAllToItems(configMapItems)
			.endConfigMap()
			.endVolume()
			.endSpec()
			.build();
	}

	public static Container createTaskManagerContainer(
		Configuration flinkConfig, TaskManagerResource taskManagerResource,
		String confDir, String resourceId, Double minCorePerContainer, Integer minMemoryPerContainer,
		Map<String, String> additionalEnvs) {
		final ContaineredTaskManagerParameters taskManagerParameters = ContaineredTaskManagerParameters.create(
			flinkConfig,
			taskManagerResource.getTotalContainerMemory(),
			taskManagerResource.getTotalHeapMemory(),
			taskManagerResource.getTotalDirectMemory(),
			taskManagerResource.getSlotNum(),
			taskManagerResource.getYoungHeapMemory());
		String command = getTaskManagerShellCommand(flinkConfig, taskManagerParameters,
			confDir, false, true,
			KubernetesTaskExecutorRunner.class);
		LOG.info("TaskExecutor will be started with container size {} MB, JVM heap size {} MB, " +
				"new generation size {} MB, JVM direct memory limit {} MB, command: {}",
			taskManagerParameters.taskManagerTotalMemoryMB(),
			taskManagerParameters.taskManagerHeapSizeMB(),
			taskManagerParameters.getYoungMemoryMB(),
			taskManagerParameters.taskManagerDirectMemoryLimitMB(),
			command);

		String image = flinkConfig.getString(KubernetesConfigOptions.CONTAINER_IMAGE);
		checkNotNull(image, "TaskManager image should be specified");

		String pullPolicy = flinkConfig.getString(KubernetesConfigOptions.CONTAINER_IMAGE_PULL_POLICY);

		int rpcPort = Integer.parseInt(flinkConfig.getString(TaskManagerOptions.RPC_PORT));

		double cpu;
		if (minCorePerContainer == null) {
			cpu = taskManagerResource.getContainerCpuCores();
		} else {
			cpu = Math.max(taskManagerResource.getContainerCpuCores(), minCorePerContainer);
		}
		long memory;
		if (minMemoryPerContainer == null) {
			memory = (long) taskManagerResource.getTotalContainerMemory();
		} else {
			memory = Math.max(taskManagerResource.getTotalContainerMemory(), minMemoryPerContainer);
		}
		Map<String, Resource> pureExtendedResources = getPureExtendedResources(
			taskManagerResource.getTaskResourceProfile());
		Quantity taskManagerCpuQuantity = new QuantityBuilder(false)
			.withAmount(String.valueOf(cpu))
			.build();
		long totalContainerMemoryBytes = memory << 20;
		Quantity taskManagerMemoryQuantity = new QuantityBuilder(false)
			.withAmount(String.valueOf(totalContainerMemoryBytes))
			.build();
		ContainerBuilder containerBuilder = new ContainerBuilder();
		containerBuilder
			.withName(resourceId)
			.withImage(image)
			.withImagePullPolicy(pullPolicy)
			.addNewPort()
			.withName(TASK_MANAGER_RPC_PORT)
			.withContainerPort(rpcPort)
			.withProtocol(PROTOCOL_TCP)
			.endPort()
			.withVolumeMounts(
				new VolumeMountBuilder().withName(FLINK_CONF_VOLUME)
					.withMountPath(confDir).build())
			.withArgs(Arrays.asList("/bin/bash", "-c", command))
			.editOrNewResources()
			.addToRequests(RESOURCE_NAME_MEMORY, taskManagerMemoryQuantity)
			.addToLimits(RESOURCE_NAME_MEMORY, taskManagerMemoryQuantity)
			.addToRequests(RESOURCE_NAME_CPU, taskManagerCpuQuantity)
			.endResources();
		if (pureExtendedResources != null && !pureExtendedResources.isEmpty()) {
			pureExtendedResources.values().stream().forEach(
				e -> containerBuilder.editOrNewResources()
					.addToLimits(e.getName(), new QuantityBuilder(false)
						.withAmount(String.valueOf(e.getValue())).build())
					.endResources());
		}
		// Add environments
		containerBuilder.addNewEnv()
			.withName(ENV_FLINK_CONF_DIR)
			.withValue(confDir)
			.endEnv()
			.addNewEnv()
			.withName(Constants.ENV_FLINK_CONTAINER_ID)
			.withValue(resourceId)
			.endEnv();
		if (additionalEnvs != null && !additionalEnvs.isEmpty()) {
			additionalEnvs.entrySet().stream().forEach(
				e -> containerBuilder.addNewEnv().withName(e.getKey()).withValue(e.getValue()).endEnv()
			);
		}
		return containerBuilder.build();
	}

	public static String getTaskManagerShellCommand(
		Configuration flinkConfig,
		ContaineredTaskManagerParameters tmParams,
		String configDirectory,
		boolean hasLogback,
		boolean hasLog4j,
		Class<?> mainClass) {

		String confDir = flinkConfig.getString(KubernetesConfigOptions.CONF_DIR);
		final Map<String, String> startCommandValues = new HashMap<>();
		startCommandValues.put("java", "$JAVA_HOME/bin/java");
		startCommandValues.put("classpath", "-classpath " + confDir + File.pathSeparator + "$" + ENV_FLINK_CLASSPATH);
		startCommandValues.put("class", mainClass.getName());

		ArrayList<String> params = new ArrayList<>();
		params.add(String.format("-Xms%dm", tmParams.taskManagerHeapSizeMB()));
		params.add(String.format("-Xmx%dm", tmParams.taskManagerHeapSizeMB()));

		if (tmParams.getYoungMemoryMB() > 0) {
			params.add(String.format("-Xmn%dm", tmParams.getYoungMemoryMB()));
		}

		if (tmParams.taskManagerDirectMemoryLimitMB() >= 0) {
			params.add(String.format("-XX:MaxDirectMemorySize=%dm",
				tmParams.taskManagerDirectMemoryLimitMB()));
		}

		startCommandValues.put("jvmmem", StringUtils.join(params, ' '));

		String javaOpts = flinkConfig.getString(CoreOptions.FLINK_JVM_OPTIONS);
		if (flinkConfig.getString(CoreOptions.FLINK_TM_JVM_OPTIONS).length() > 0) {
			javaOpts += " " + flinkConfig.getString(CoreOptions.FLINK_TM_JVM_OPTIONS);
		}
		startCommandValues.put("jvmopts", javaOpts);

		String logging = "";
		if (hasLogback) {
			logging +=
				" -Dlogback.configurationFile=file:" + configDirectory +
					File.separator + Constants.CONFIG_FILE_LOGBACK_NAME;
		}
		if (hasLog4j) {
			logging += " -Dlog4j.configuration=file:" + configDirectory +
				File.separator + Constants.CONFIG_FILE_LOG4J_NAME;
		}
		startCommandValues.put("logging", logging);

		final String commandTemplate = flinkConfig
			.getString(KubernetesConfigOptions.CONTAINER_START_COMMAND_TEMPLATE);
		String startCommand = BootstrapTools.getStartCommand(commandTemplate, startCommandValues);
		return startCommand;
	}

	public static ResourceProfile createTaskManagerResourceProfile(Configuration flinkConfig) {
		double core = flinkConfig.getDouble(TaskManagerOptions.TASK_MANAGER_CORE);
		int heapMemory = flinkConfig.getInteger(TaskManagerOptions.TASK_MANAGER_HEAP_MEMORY);
		int nativeMemory = flinkConfig.getInteger(TaskManagerOptions.TASK_MANAGER_NATIVE_MEMORY);
		int directMemory = flinkConfig.getInteger(TaskManagerOptions.TASK_MANAGER_DIRECT_MEMORY);
		int networkMemory = (int) Math.ceil(TaskManagerResourceCalculator
			.calculateNetworkBufferMemory(flinkConfig) / (1024.0 * 1024.0));

		// Add managed memory to extended resources.
		long managedMemory = flinkConfig.getLong(TaskManagerOptions.MANAGED_MEMORY_SIZE);
		Map<String, org.apache.flink.api.common.resources.Resource> resourceMap = getExtendedResources(flinkConfig);
		resourceMap.put(ResourceSpec.MANAGED_MEMORY_NAME,
			new CommonExtendedResource(ResourceSpec.MANAGED_MEMORY_NAME, managedMemory));
		long floatingManagedMemory = flinkConfig.getLong(TaskManagerOptions.FLOATING_MANAGED_MEMORY_SIZE);
		resourceMap.put(ResourceSpec.FLOATING_MANAGED_MEMORY_NAME,
			new CommonExtendedResource(ResourceSpec.FLOATING_MANAGED_MEMORY_NAME, floatingManagedMemory));
		return new ResourceProfile(
			core,
			heapMemory,
			directMemory,
			nativeMemory,
			networkMemory,
			resourceMap);
	}

	/**
	 * Get extended resources from config.
	 * @return The extended resources.
	 */
	public static Map<String, org.apache.flink.api.common.resources.Resource> getExtendedResources(Configuration flinkConfig) {
		Map<String, org.apache.flink.api.common.resources.Resource> extendedResources = new HashMap<>();
		String resourcesStr = flinkConfig.getString(TaskManagerOptions.TASK_MANAGER_EXTENDED_RESOURCES);
		if (resourcesStr != null && !resourcesStr.isEmpty()) {
			for (String resource : resourcesStr.split(",")) {
				String[] splits = resource.split(":");
				if (splits.length == 2) {
					try {
						extendedResources.put(splits[0],
							new CommonExtendedResource(splits[0], Long.parseLong(splits[1])));
					} catch (NumberFormatException ex) {
						LOG.error("Parse extended resource " + resource + " error.", ex);
					}
				}
			}
		}
		return extendedResources;
	}

	public static Map<String, Double> loadExtendedResourceConstrains(Configuration config) {
		String[] constrains = config.getString(TaskManagerOptions.TASK_MANAGER_MULTI_SLOTS_MAX_EXTENDED_RESOURCES)
			.split(",");
		Map<String, Double> extendedResourceConstrains = new HashMap<>(constrains.length);
		for (String constrain : constrains) {
			String[] kv = constrain.split("=");
			if (kv.length == 2) {
				extendedResourceConstrains.put(kv[0].toLowerCase(), Double.valueOf(kv[1]));
			}
		}
		return extendedResourceConstrains;
	}

	public static Map<String, Resource> getPureExtendedResources(ResourceProfile resourceProfile) {
		if (resourceProfile != null && resourceProfile.getExtendedResources() != null) {
			return resourceProfile.getExtendedResources().entrySet().stream().filter(
				e -> !e.getKey().equals(ResourceSpec.MANAGED_MEMORY_NAME) && !e.getKey()
					.equals(ResourceSpec.FLOATING_MANAGED_MEMORY_NAME))
				.collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
		}
		return null;
	}

	public static String getEncodedResourceProfile(ResourceProfile resourceProfile) throws IOException {
		ByteArrayOutputStream output = new ByteArrayOutputStream();
		ObjectOutputStream rpOutput = new ObjectOutputStream(output);
		rpOutput.writeObject(resourceProfile);
		rpOutput.close();
		return new String(Base64.encodeBase64(output.toByteArray()));
	}
}
