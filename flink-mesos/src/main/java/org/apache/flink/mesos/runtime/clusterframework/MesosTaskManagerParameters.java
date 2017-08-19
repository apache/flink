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

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;
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

	public static final ConfigOption<Integer> MESOS_RM_TASKS_SLOTS =
		key(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS)
		.defaultValue(1);

	public static final ConfigOption<Integer> MESOS_RM_TASKS_MEMORY_MB =
		key("mesos.resourcemanager.tasks.mem")
		.defaultValue(1024);

	public static final ConfigOption<Double> MESOS_RM_TASKS_CPUS =
		key("mesos.resourcemanager.tasks.cpus")
		.defaultValue(0.0);

	public static final ConfigOption<String> MESOS_RM_CONTAINER_TYPE =
		key("mesos.resourcemanager.tasks.container.type")
		.defaultValue("mesos");

	public static final ConfigOption<String> MESOS_RM_CONTAINER_IMAGE_NAME =
		key("mesos.resourcemanager.tasks.container.image.name")
		.noDefaultValue();

	public static final ConfigOption<String> MESOS_TM_HOSTNAME =
		key("mesos.resourcemanager.tasks.hostname")
		.noDefaultValue();

	public static final ConfigOption<String> MESOS_TM_CMD =
		key("mesos.resourcemanager.tasks.taskmanager-cmd")
		.defaultValue("$FLINK_HOME/bin/mesos-taskmanager.sh"); // internal

	public static final ConfigOption<String> MESOS_TM_BOOTSTRAP_CMD =
		key("mesos.resourcemanager.tasks.bootstrap-cmd")
		.noDefaultValue();

	public static final ConfigOption<String> MESOS_RM_CONTAINER_VOLUMES =
		key("mesos.resourcemanager.tasks.container.volumes")
		.noDefaultValue();

	public static final ConfigOption<String> MESOS_CONSTRAINTS_HARD_HOSTATTR =
		key("mesos.constraints.hard.hostattribute")
		.noDefaultValue();

	/**
	 * Value for {@code MESOS_RESOURCEMANAGER_TASKS_CONTAINER_TYPE} setting. Tells to use the Mesos containerizer.
	 */
	public static final String MESOS_RESOURCEMANAGER_TASKS_CONTAINER_TYPE_MESOS = "mesos";
	/**
	 * Value for {@code MESOS_RESOURCEMANAGER_TASKS_CONTAINER_TYPE} setting. Tells to use the Docker containerizer.
	 */
	public static final String MESOS_RESOURCEMANAGER_TASKS_CONTAINER_TYPE_DOCKER = "docker";

	private final double cpus;

	private final ContainerType containerType;

	private final Option<String> containerImageName;

	private final ContaineredTaskManagerParameters containeredParameters;

	private final List<Protos.Volume> containerVolumes;

	private final List<ConstraintEvaluator> constraints;

	private final String command;

	private final Option<String> bootstrapCommand;

	private final Option<String> taskManagerHostname;

	public MesosTaskManagerParameters(
			double cpus,
			ContainerType containerType,
			Option<String> containerImageName,
			ContaineredTaskManagerParameters containeredParameters,
			List<Protos.Volume> containerVolumes,
			List<ConstraintEvaluator> constraints,
			String command,
			Option<String> bootstrapCommand,
			Option<String> taskManagerHostname) {

		this.cpus = cpus;
		this.containerType = Preconditions.checkNotNull(containerType);
		this.containerImageName = Preconditions.checkNotNull(containerImageName);
		this.containeredParameters = Preconditions.checkNotNull(containeredParameters);
		this.containerVolumes = Preconditions.checkNotNull(containerVolumes);
		this.constraints = Preconditions.checkNotNull(constraints);
		this.command = Preconditions.checkNotNull(command);
		this.bootstrapCommand = Preconditions.checkNotNull(bootstrapCommand);
		this.taskManagerHostname = Preconditions.checkNotNull(taskManagerHostname);
	}

	/**
	 * Get the CPU units to use for the TaskManager process.
	 */
	public double cpus() {
		return cpus;
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

	@Override
	public String toString() {
		return "MesosTaskManagerParameters{" +
			"cpus=" + cpus +
			", containerType=" + containerType +
			", containerImageName=" + containerImageName +
			", containeredParameters=" + containeredParameters +
			", containerVolumes=" + containerVolumes +
			", constraints=" + constraints +
			", taskManagerHostName=" + taskManagerHostname +
			", command=" + command +
			", bootstrapCommand=" + bootstrapCommand +
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
		ContaineredTaskManagerParameters containeredParameters = ContaineredTaskManagerParameters.create(
			flinkConfig,
			flinkConfig.getInteger(MESOS_RM_TASKS_MEMORY_MB),
			flinkConfig.getInteger(MESOS_RM_TASKS_SLOTS));

		double cpus = flinkConfig.getDouble(MESOS_RM_TASKS_CPUS);
		if (cpus <= 0.0) {
			cpus = Math.max(containeredParameters.numSlots(), 1.0);
		}

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

		List<Protos.Volume> containerVolumes = buildVolumes(containerVolOpt);

		//obtain Task Manager Host Name from the configuration
		Option<String> taskManagerHostname = Option.apply(flinkConfig.getString(MESOS_TM_HOSTNAME));

		//obtain command-line from the configuration
		String tmCommand = flinkConfig.getString(MESOS_TM_CMD);
		Option<String> tmBootstrapCommand = Option.apply(flinkConfig.getString(MESOS_TM_BOOTSTRAP_CMD));

		return new MesosTaskManagerParameters(
			cpus,
			containerType,
			Option.apply(imageName),
			containeredParameters,
			containerVolumes,
			constraints,
			tmCommand,
			tmBootstrapCommand,
			taskManagerHostname);
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

	/**
	 * The supported containerizers.
	 */
	public enum ContainerType {
		MESOS,
		DOCKER
	}
}
