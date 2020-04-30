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

package org.apache.flink.mesos.runtime.clusterframework;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessSpec;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils;
import org.apache.flink.util.Preconditions;

import com.netflix.fenzo.ConstraintEvaluator;
import com.netflix.fenzo.functions.Func1;
import com.netflix.fenzo.plugins.HostAttrValueConstraint;
import org.apache.mesos.Protos;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import scala.Option;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * This class describes the Mesos-specific parameters for launching a TaskManager process.
 *
 * <p>These parameters are in addition to the common parameters
 * provided by {@link ContaineredTaskManagerParameters}.
 */
public class MesosTaskManagerParameters {

	/** Pattern replaced in the {@link #MESOS_TM_HOSTNAME} by the actual task id of the Mesos task. */
	public static final Pattern TASK_ID_PATTERN = Pattern.compile("_TASK_", Pattern.LITERAL);

	public static final ConfigOption<Integer> MESOS_RM_TASKS_DISK_MB =
		key("mesos.resourcemanager.tasks.disk")
		.defaultValue(0)
		.withDescription(Description.builder().text("Disk space to assign to the Mesos workers in MB.").build());

	public static final ConfigOption<Double> MESOS_RM_TASKS_CPUS =
		key("mesos.resourcemanager.tasks.cpus")
		.doubleType()
		.defaultValue(0.0)
		.withDescription("CPUs to assign to the Mesos workers.");

	public static final ConfigOption<Integer> MESOS_RM_TASKS_GPUS =
		key("mesos.resourcemanager.tasks.gpus")
		.defaultValue(0)
		.withDescription(Description.builder().text("GPUs to assign to the Mesos workers.").build());

	public static final ConfigOption<String> MESOS_RM_CONTAINER_TYPE =
		key("mesos.resourcemanager.tasks.container.type")
		.defaultValue("mesos")
		.withDescription("Type of the containerization used: “mesos” or “docker”.");

	public static final ConfigOption<String> MESOS_RM_CONTAINER_IMAGE_NAME =
		key("mesos.resourcemanager.tasks.container.image.name")
		.noDefaultValue()
		.withDescription("Image name to use for the container.");

	public static final ConfigOption<String> MESOS_TM_HOSTNAME =
		key("mesos.resourcemanager.tasks.hostname")
		.noDefaultValue()
		.withDescription(Description.builder()
			.text("Optional value to define the TaskManager’s hostname. " +
				"The pattern _TASK_ is replaced by the actual id of the Mesos task. " +
				"This can be used to configure the TaskManager to use Mesos DNS (e.g. _TASK_.flink-service.mesos) for name lookups.")
			.build());

	public static final ConfigOption<String> MESOS_TM_CMD =
		key("mesos.resourcemanager.tasks.taskmanager-cmd")
		.defaultValue("$FLINK_HOME/bin/mesos-taskmanager.sh"); // internal

	public static final ConfigOption<String> MESOS_TM_BOOTSTRAP_CMD =
		key("mesos.resourcemanager.tasks.bootstrap-cmd")
		.noDefaultValue()
		.withDescription(Description.builder()
			.text("A command which is executed before the TaskManager is started.")
			.build());

	public static final ConfigOption<String> MESOS_TM_URIS =
		key("mesos.resourcemanager.tasks.uris")
		.noDefaultValue()
		.withDescription("A comma separated list of URIs of custom artifacts to be downloaded into the sandbox" +
			" of Mesos workers.");

	public static final ConfigOption<String> MESOS_RM_CONTAINER_VOLUMES =
		key("mesos.resourcemanager.tasks.container.volumes")
		.noDefaultValue()
		.withDescription("A comma separated list of [host_path:]container_path[:RO|RW]. This allows for mounting" +
			" additional volumes into your container.");

	public static final ConfigOption<String> MESOS_RM_CONTAINER_DOCKER_PARAMETERS =
		key("mesos.resourcemanager.tasks.container.docker.parameters")
		.noDefaultValue()
		.withDescription("Custom parameters to be passed into docker run command when using the docker containerizer." +
			" Comma separated list of \"key=value\" pairs. The \"value\" may contain '='.");

	public static final ConfigOption<Boolean> MESOS_RM_CONTAINER_DOCKER_FORCE_PULL_IMAGE =
		key("mesos.resourcemanager.tasks.container.docker.force-pull-image")
		.defaultValue(false)
		.withDescription("Instruct the docker containerizer to forcefully pull the image rather than" +
			" reuse a cached version.");

	public static final ConfigOption<String> MESOS_CONSTRAINTS_HARD_HOSTATTR =
		key("mesos.constraints.hard.hostattribute")
		.noDefaultValue()
		.withDescription(Description.builder()
			.text("Constraints for task placement on Mesos based on agent attributes. " +
				"Takes a comma-separated list of key:value pairs corresponding to the attributes exposed by the target mesos agents. " +
				"Example: az:eu-west-1a,series:t2")
			.build());

	/**
	 * Value for {@code MESOS_RESOURCEMANAGER_TASKS_CONTAINER_TYPE} setting. Tells to use the Mesos containerizer.
	 */
	public static final String MESOS_RESOURCEMANAGER_TASKS_CONTAINER_TYPE_MESOS = "mesos";
	/**
	 * Value for {@code MESOS_RESOURCEMANAGER_TASKS_CONTAINER_TYPE} setting. Tells to use the Docker containerizer.
	 */
	public static final String MESOS_RESOURCEMANAGER_TASKS_CONTAINER_TYPE_DOCKER = "docker";

	private final int gpus;

	private final int disk;

	private final ContainerType containerType;

	private final Option<String> containerImageName;

	private final ContaineredTaskManagerParameters containeredParameters;

	private final List<Protos.Volume> containerVolumes;

	private final List<Protos.Parameter> dockerParameters;

	private final boolean dockerForcePullImage;

	private final List<ConstraintEvaluator> constraints;

	private final String command;

	private final Option<String> bootstrapCommand;

	private final Option<String> taskManagerHostname;

	private final List<String> uris;

	public MesosTaskManagerParameters(
			int gpus,
			int disk,
			ContainerType containerType,
			Option<String> containerImageName,
			ContaineredTaskManagerParameters containeredParameters,
			List<Protos.Volume> containerVolumes,
			List<Protos.Parameter> dockerParameters,
			boolean dockerForcePullImage,
			List<ConstraintEvaluator> constraints,
			String command,
			Option<String> bootstrapCommand,
			Option<String> taskManagerHostname,
			List<String> uris) {

		this.gpus = gpus;
		this.disk = disk;
		this.containerType = Preconditions.checkNotNull(containerType);
		this.containerImageName = Preconditions.checkNotNull(containerImageName);
		this.containeredParameters = Preconditions.checkNotNull(containeredParameters);
		this.containerVolumes = Preconditions.checkNotNull(containerVolumes);
		this.dockerParameters = Preconditions.checkNotNull(dockerParameters);
		this.dockerForcePullImage = dockerForcePullImage;
		this.constraints = Preconditions.checkNotNull(constraints);
		this.command = Preconditions.checkNotNull(command);
		this.bootstrapCommand = Preconditions.checkNotNull(bootstrapCommand);
		this.taskManagerHostname = Preconditions.checkNotNull(taskManagerHostname);
		this.uris = Preconditions.checkNotNull(uris);
	}

	/**
	 * Get the CPU units to use for the TaskManager process.
	 */
	public double cpus() {
		return containeredParameters.getTaskExecutorProcessSpec().getCpuCores().getValue().doubleValue();
	}

	/**
	 * Get the GPU units to use for the TaskManager Process.
	 */
	public int gpus() {
		return gpus;
	}

	/**
	 * Get the disk space in MB to use for the TaskManager Process.
	 */
	public int disk() {
		return disk;
	}

	/**
	 * Get the container type (Mesos or Docker).  The default is Mesos.
	 *
	 * <p>Mesos provides a facility for a framework to specify which containerizer to use.
	 */
	public ContainerType containerType() {
		return containerType;
	}

	/**
	 * Get the container image name.
	 */
	public Option<String> containerImageName() {
		return containerImageName;
	}

	/**
	 * Get the common containered parameters.
	 */
	public ContaineredTaskManagerParameters containeredParameters() {
		return containeredParameters;
	}

	/**
	 * Get the container volumes string.
	 */
	public List<Protos.Volume> containerVolumes() {
		return containerVolumes;
	}

	/**
	 * Get Docker runtime parameters.
	 */
	public List<Protos.Parameter> dockerParameters() {
		return dockerParameters;
	}

	/**
	 * Get Docker option to force pull image.
	 */
	public boolean dockerForcePullImage() {
		return dockerForcePullImage;
	}

	/**
	 * Get the placement constraints.
	 */
	public List<ConstraintEvaluator> constraints() {
		return constraints;
	}

	/**
	 * Get the taskManager hostname.
	 */
	public Option<String> getTaskManagerHostname() {
		return taskManagerHostname;
	}

	/**
	 * Get the command.
	 */
	public String command() {
		return command;
	}

	/**
	 * Get the bootstrap command.
	 */
	public Option<String> bootstrapCommand() {
		return bootstrapCommand;
	}

	/**
	 * Get custom artifact URIs.
	 */
	public List<String> uris() {
		return uris;
	}

	@Override
	public String toString() {
		return "MesosTaskManagerParameters{" +
			"cpus=" + cpus() +
			", gpus=" + gpus +
			", containerType=" + containerType +
			", containerImageName=" + containerImageName +
			", containeredParameters=" + containeredParameters +
			", containerVolumes=" + containerVolumes +
			", dockerParameters=" + dockerParameters +
			", dockerForcePullImage=" + dockerForcePullImage +
			", constraints=" + constraints +
			", taskManagerHostName=" + taskManagerHostname +
			", command=" + command +
			", bootstrapCommand=" + bootstrapCommand +
			", uris=" + uris +
			'}';
	}

	/**
	 * Create the Mesos TaskManager parameters.
	 *
	 * @param flinkConfig the TM configuration.
	 */
	public static MesosTaskManagerParameters create(Configuration flinkConfig) {

		List<ConstraintEvaluator> constraints = parseConstraints(flinkConfig.getString(MESOS_CONSTRAINTS_HARD_HOSTATTR));

		// parse the common parameters
		ContaineredTaskManagerParameters containeredParameters = createContaineredTaskManagerParameters(flinkConfig);

		int gpus = flinkConfig.getInteger(MESOS_RM_TASKS_GPUS);

		if (gpus < 0) {
			throw new IllegalConfigurationException(MESOS_RM_TASKS_GPUS.key() +
				" cannot be negative");
		}

		int disk = flinkConfig.getInteger(MESOS_RM_TASKS_DISK_MB);

		// parse the containerization parameters
		String imageName = flinkConfig.getString(MESOS_RM_CONTAINER_IMAGE_NAME);

		ContainerType containerType;
		String containerTypeString = flinkConfig.getString(MESOS_RM_CONTAINER_TYPE);
		switch (containerTypeString) {
			case MESOS_RESOURCEMANAGER_TASKS_CONTAINER_TYPE_MESOS:
				containerType = ContainerType.MESOS;
				break;
			case MESOS_RESOURCEMANAGER_TASKS_CONTAINER_TYPE_DOCKER:
				containerType = ContainerType.DOCKER;
				if (imageName == null || imageName.length() == 0) {
					throw new IllegalConfigurationException(MESOS_RM_CONTAINER_IMAGE_NAME.key() +
						" must be specified for docker container type");
				}
				break;
			default:
				throw new IllegalConfigurationException("invalid container type: " + containerTypeString);
		}

		Option<String> containerVolOpt = Option.<String>apply(flinkConfig.getString(MESOS_RM_CONTAINER_VOLUMES));

		Option<String> dockerParamsOpt = Option.<String>apply(flinkConfig.getString(MESOS_RM_CONTAINER_DOCKER_PARAMETERS));

		Option<String> uriParamsOpt = Option.<String>apply(flinkConfig.getString(MESOS_TM_URIS));

		boolean dockerForcePullImage = flinkConfig.getBoolean(MESOS_RM_CONTAINER_DOCKER_FORCE_PULL_IMAGE);

		List<Protos.Volume> containerVolumes = buildVolumes(containerVolOpt);

		List<Protos.Parameter> dockerParameters = buildDockerParameters(dockerParamsOpt);

		List<String> uris = buildUris(uriParamsOpt);

		//obtain Task Manager Host Name from the configuration
		Option<String> taskManagerHostname = Option.apply(flinkConfig.getString(MESOS_TM_HOSTNAME));

		//obtain command-line from the configuration
		String tmCommand = flinkConfig.getString(MESOS_TM_CMD);
		Option<String> tmBootstrapCommand = Option.apply(flinkConfig.getString(MESOS_TM_BOOTSTRAP_CMD));

		return new MesosTaskManagerParameters(
			gpus,
			disk,
			containerType,
			Option.apply(imageName),
			containeredParameters,
			containerVolumes,
			dockerParameters,
			dockerForcePullImage,
			constraints,
			tmCommand,
			tmBootstrapCommand,
			taskManagerHostname,
			uris);
	}

	private static ContaineredTaskManagerParameters createContaineredTaskManagerParameters(final Configuration flinkConfig) {
		double cpus = getCpuCores(flinkConfig);
		TaskExecutorProcessSpec taskExecutorProcessSpec = TaskExecutorProcessUtils
			.newProcessSpecBuilder(flinkConfig)
			.withCpuCores(cpus)
			.build();

		return ContaineredTaskManagerParameters.create(
			flinkConfig,
			taskExecutorProcessSpec);
	}

	private static double getCpuCores(final Configuration configuration) {
		return TaskExecutorProcessUtils.getCpuCoresWithFallbackConfigOption(configuration, MESOS_RM_TASKS_CPUS);
	}

	private static List<ConstraintEvaluator> parseConstraints(String mesosConstraints) {

		if (mesosConstraints == null || mesosConstraints.isEmpty()) {
			return Collections.emptyList();
		} else {
			List<ConstraintEvaluator> constraints = new ArrayList<>();

			for (String constraint : mesosConstraints.split(",")) {
				if (constraint.isEmpty()) {
					continue;
				}
				final String[] constraintList = constraint.split(":");
				if (constraintList.length != 2) {
					continue;
				}
				addHostAttrValueConstraint(constraints, constraintList[0], constraintList[1]);
			}

			return constraints;
		}
	}

	private static void addHostAttrValueConstraint(List<ConstraintEvaluator> constraints, String constraintKey, final String constraintValue) {
		constraints.add(new HostAttrValueConstraint(constraintKey, new Func1<String, String>() {
			@Override
			public String call(String s) {
				return constraintValue;
			}
		}));
	}

	/**
	 * Used to build volume specs for mesos. This allows for mounting additional volumes into a container
	 *
	 * @param containerVolumes a comma delimited optional string of [host_path:]container_path[:RO|RW] that
	 *                         defines mount points for a container volume. If None or empty string, returns
	 *                         an empty iterator
	 */
	public static List<Protos.Volume> buildVolumes(Option<String> containerVolumes) {
		if (containerVolumes.isEmpty()) {
			return Collections.emptyList();
		} else {
			String[] volumeSpecifications = containerVolumes.get().split(",");

			List<Protos.Volume> volumes = new ArrayList<>(volumeSpecifications.length);

			for (String volumeSpecification : volumeSpecifications) {
				if (!volumeSpecification.trim().isEmpty()) {
					Protos.Volume.Builder volume = Protos.Volume.newBuilder();
					volume.setMode(Protos.Volume.Mode.RW);

					String[] parts = volumeSpecification.split(":");

					switch (parts.length) {
						case 1:
							volume.setContainerPath(parts[0]);
							break;
						case 2:
							try {
								Protos.Volume.Mode mode = Protos.Volume.Mode.valueOf(parts[1].trim().toUpperCase());
								volume.setMode(mode)
									.setContainerPath(parts[0]);
							} catch (IllegalArgumentException e) {
								volume.setHostPath(parts[0])
									.setContainerPath(parts[1]);
							}
							break;
						case 3:
							Protos.Volume.Mode mode = Protos.Volume.Mode.valueOf(parts[2].trim().toUpperCase());
							volume.setMode(mode)
								.setHostPath(parts[0])
								.setContainerPath(parts[1]);
							break;
						default:
							throw new IllegalArgumentException("volume specification is invalid, given: " + volumeSpecification);
					}

					volumes.add(volume.build());
				}
			}
			return volumes;
		}
	}

	public static List<Protos.Parameter> buildDockerParameters(Option<String> dockerParameters) {
		if (dockerParameters.isEmpty()) {
			return Collections.emptyList();
		} else {
			String[] dockerParameterSpecifications = dockerParameters.get().split(",");

			List<Protos.Parameter> parameters = new ArrayList<>(dockerParameterSpecifications.length);

			for (String dockerParameterSpecification : dockerParameterSpecifications) {
				if (!dockerParameterSpecification.trim().isEmpty()) {
					// split with the limit of 2 in case the value includes '='
					String[] match = dockerParameterSpecification.split("=", 2);
					if (match.length != 2) {
						throw new IllegalArgumentException("Docker parameter specification is invalid, given: "
							+ dockerParameterSpecification);
					}
					Protos.Parameter.Builder parameter = Protos.Parameter.newBuilder();
					parameter.setKey(match[0]);
					parameter.setValue(match[1]);
					parameters.add(parameter.build());
				}
			}
			return parameters;
		}
	}

	/**
	 * Build a list of URIs for providing custom artifacts to Mesos tasks.
	 * @param uris a comma delimited optional string listing artifact URIs
	 */
	public static List<String> buildUris(Option<String> uris) {
		if (uris.isEmpty()) {
			return Collections.emptyList();
		} else {
			List<String> urisList = new ArrayList<>();
			for (String uri : uris.get().split(",")) {
				urisList.add(uri.trim());
			}
			return urisList;
		}
	}

	/**
	 * The supported containerizers.
	 */
	public enum ContainerType {
		MESOS,
		DOCKER
	}
}
