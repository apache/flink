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

package org.apache.flink.kubernetes.configuration;

import org.apache.flink.configuration.ConfigOption;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * This class holds configuration constants used by Flink's kubernetes runners.
 *
 * <p>These options are not expected to be ever configured by users explicitly.
 */
public class KubernetesConfigOptions {

	public static final ConfigOption<String> CLUSTER_ID =
		key("kubernetes.cluster-id")
		.noDefaultValue()
		.withDescription("The custom name for the Flink cluster on Kubernetes. It could be specified by -nm argument. " +
			"If it's not set, the client will generate a random UUID name");

	public static final ConfigOption<String> CONF_DIR =
		key("kubernetes.flink.conf.dir")
			.defaultValue("/etc/flink/conf")
			.withDescription("The conf dir will be mounted in pod.");

	public static final ConfigOption<String> MASTER_URL =
		key("kubernetes.master.url")
		.defaultValue("localhost:8080")
		.withDescription("The kubernetes master url.");

	public static final ConfigOption<String> NAME_SPACE =
		key("kubernetes.namespace")
			.defaultValue("default")
			.withDescription("The namespace that will be used for running the jobmanager and taskmanager pods.");

	public static final ConfigOption<String> SERVICE_EXPOSED_TYPE =
		key("kubernetes.service.exposed.type")
			.defaultValue("CLUSTER_IP")
			.withDescription("It could be CLUSTER_IP(default)/NODE_PORT/LOAD_BALANCER/EXTERNAL_NAME.");

	public static final ConfigOption<String> SERVICE_EXPOSED_ADDRESS =
		key("kubernetes.service.external.address")
			.defaultValue("localhost")
			.withDescription("The exposed address of kubernetes service to submit job and view dashboard.");

	public static final ConfigOption<String> JOB_MANAGER_SERVICE_ACCOUNT =
		key("kubernetes.jobmanager.service-account")
			.defaultValue("default")
			.withDescription("Service account that is used by jobmanager within kubernetes cluster. " +
				"The job manager uses this service account when requesting taskmanager pods from the API server.");

	public static final ConfigOption<Double> JOB_MANAGER_CORE =
		key("kubernetes.jobmanager.cpu")
		.defaultValue(1.0)
		.withDescription("The number of cpu used by job manager");

	public static final ConfigOption<String> CONTAINER_IMAGE =
		key("kubernetes.container.image")
		.defaultValue("flink-k8s:latest")
		.withDescription("Container image to use for Flink containers. Individual container types " +
			"(e.g. jobmanager or taskmanager) can also be configured to use different images if desired, " +
			"by setting the container type-specific image name.");

	public static final ConfigOption<String> CONTAINER_FILES =
		key("kubernetes.container.files")
			.noDefaultValue()
			.withDescription("Files to be used for Flink containers, will be transferred to flink conf directory " +
				"and appended to classpath in containers.");

	public static final ConfigOption<String> CONTAINER_IMAGE_PULL_POLICY =
		key("kubernetes.container.image.pullPolicy")
		.defaultValue("IfNotPresent")
		.withDescription("Kubernetes image pull policy. Valid values are Always, Never, and IfNotPresent.");

	public static final ConfigOption<String> CONTAINER_START_COMMAND_TEMPLATE =
		key("kubernetes.container-start-command-template")
		.defaultValue("%java% %classpath% %jvmmem% %jvmopts% %logging% %class%")
		.withDescription("Template for the kubernetes container start invocation");

	public static final ConfigOption<String> JOBMANAGER_POD_NAME =
		key("kubernetes.jobmanager.pod.name")
		.defaultValue("jobmanager")
		.withDescription("Name of the jobmanager pod.");

	public static final ConfigOption<String> JOB_MANAGER_CONTAINER_NAME =
		key("kubernetes.jobmanager.container.name")
		.defaultValue("flink-kubernetes-jobmanager")
		.withDescription("Name of the jobmanager container.");

	public static final ConfigOption<String> JOB_MANAGER_CONTAINER_IMAGE =
		key("kubernetes.jobmanager.container.image")
		.noDefaultValue()
		.withDescription("Container image to use for the jobmanager.");

	public static final ConfigOption<Integer> TASK_MANAGER_COUNT =
		key("kubernetes.taskmanager.count")
		.defaultValue(1)
		.withDescription("The task manager count for session cluster.");

	public static final ConfigOption<Long> TASK_MANAGER_REGISTER_TIMEOUT =
		key("kubernetes.taskmanager.register-timeout")
			.defaultValue(120L)
			.withDescription("The register timeout for a task manager before released by resource manager. In seconds." +
				"In case of a task manager took very long time to be launched.");

	public static final ConfigOption<Integer> WORKER_NODE_MAX_FAILED_ATTEMPTS =
		key("kubernetes.workernode.max-failed-attempts")
			.defaultValue(100)
			.withDescription("The max failed attempts for work node.");

	public static final ConfigOption<String> USER_PROGRAM_ENTRYPOINT_CLASS =
		key("kubernetes.program.entrypoint.class")
			.noDefaultValue()
			.withDescription("Class with the program entry point (\"main\" method or \"getPlan()\" method. Only needed if the " +
				"JAR file does not specify the class in its manifest.");

	public static final ConfigOption<String> USER_PROGRAM_ARGS =
		key("kubernetes.program.args")
			.noDefaultValue()
			.withDescription("Arguments specified for user program.");

	public static final ConfigOption<Boolean> DESTROY_PERJOB_CLUSTER_AFTER_JOB_FINISHED =
		key("kubernetes.destroy-perjob-cluster.after-job-finished")
			.defaultValue(true)
			.withDescription("Whether to kill perjob-cluster on kubernetes after job finished." +
				"If you want to check logs and view dashboard after job finished, set this to false.");

	public static final ConfigOption<Integer> KUBERNETES_CONNECTION_RETRY_TIMES =
		key("kubernetes.connection.retry.times")
			.defaultValue(120)
			.withDescription("The max retry attempts for RM talking to kubernetes.");

	public static final ConfigOption<Long> KUBERNETES_CONNECTION_RETRY_INTERVAL_MS =
		key("kubernetes.connection.retry.interval.ms")
			.defaultValue(1000L)
			.withDescription("The retry interval in milliseconds for RM talking to kubernetes.");

	/**
	 * Service exposed type on kubernetes cluster.
	 */
	public enum ServiceExposedType {
		CLUSTER_IP("ClusterIP"),
		NODE_PORT("NodePort"),
		LOAD_BALANCER("LoadBalancer"),
		EXTERNAL_NAME("ExternalName");

		private final String serviceExposedType;

		private ServiceExposedType(String type) {
			serviceExposedType = type;
		}

		public String getServiceExposedType() {
			return serviceExposedType;
		}

		/**
		 * Convert exposed type string in kubernetes spec to ServiceExposedType.
		 * @param type exposed in kubernetes spec, e.g. LoadBanlancer
		 * @return ServiceExposedType
		 */
		public static ServiceExposedType fromString(String type) {
			for (ServiceExposedType exposedType : ServiceExposedType.values()) {
				if (exposedType.getServiceExposedType().equals(type)) {
					return exposedType;
				}
			}
			return CLUSTER_IP;
		}
	}
}
