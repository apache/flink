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
import org.apache.mesos.Protos;
import scala.Option;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * This class describes the Mesos-specific parameters for launching a TaskManager process.
 *
 * These parameters are in addition to the common parameters
 * provided by {@link ContaineredTaskManagerParameters}.
 */
public class MesosTaskManagerParameters {

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

	public static final ConfigOption<String> MESOS_RM_CONTAINER_VOLUMES =
		key("mesos.resourcemanager.tasks.container.volumes")
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

	public MesosTaskManagerParameters(
		double cpus,
		ContainerType containerType,
		Option<String> containerImageName,
		ContaineredTaskManagerParameters containeredParameters,
		List<Protos.Volume> containerVolumes) {
		requireNonNull(containeredParameters);
		this.cpus = cpus;
		this.containerType = containerType;
		this.containerImageName = containerImageName;
		this.containeredParameters = containeredParameters;
		this.containerVolumes =  containerVolumes;
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
	 * Mesos provides a facility for a framework to specify which containerizer to use.
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
	 * Get the container volumes string
	 */
	public List<Protos.Volume> containerVolumes() {
		return containerVolumes;
	}

	@Override
	public String toString() {
		return "MesosTaskManagerParameters{" +
			"cpus=" + cpus +
			", containerType=" + containerType +
			", containerImageName=" + containerImageName +
			", containeredParameters=" + containeredParameters +
			", containerVolumes=" + containerVolumes.toString()	+
			'}';
	}

	/**
	 * Create the Mesos TaskManager parameters.
	 * @param flinkConfig the TM configuration.
     */
	public static MesosTaskManagerParameters create(Configuration flinkConfig) {

		// parse the common parameters
		ContaineredTaskManagerParameters containeredParameters = ContaineredTaskManagerParameters.create(
			flinkConfig,
			flinkConfig.getInteger(MESOS_RM_TASKS_MEMORY_MB),
			flinkConfig.getInteger(MESOS_RM_TASKS_SLOTS));

		double cpus = flinkConfig.getDouble(MESOS_RM_TASKS_CPUS);
		if(cpus <= 0.0) {
			cpus = Math.max(containeredParameters.numSlots(), 1.0);
		}

		// parse the containerization parameters
		String imageName = flinkConfig.getString(MESOS_RM_CONTAINER_IMAGE_NAME);

		ContainerType containerType;
		String containerTypeString = flinkConfig.getString(MESOS_RM_CONTAINER_TYPE);
		switch(containerTypeString) {
			case MESOS_RESOURCEMANAGER_TASKS_CONTAINER_TYPE_MESOS:
				containerType = ContainerType.MESOS;
				break;
			case MESOS_RESOURCEMANAGER_TASKS_CONTAINER_TYPE_DOCKER:
				containerType = ContainerType.DOCKER;
				if(imageName == null || imageName.length() == 0) {
					throw new IllegalConfigurationException(MESOS_RM_CONTAINER_IMAGE_NAME.key() +
						" must be specified for docker container type");
				}
				break;
			default:
				throw new IllegalConfigurationException("invalid container type: " + containerTypeString);
		}

		Option<String> containerVolOpt = Option.<String>apply(flinkConfig.getString(MESOS_RM_CONTAINER_VOLUMES));
		List<Protos.Volume> containerVolumes = buildVolumes(containerVolOpt);

		return new MesosTaskManagerParameters(
			cpus,
			containerType,
			Option.apply(imageName),
			containeredParameters,
			containerVolumes);
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
			return new ArrayList<Protos.Volume>();
		}
		String[] specs = containerVolumes.get().split(",");
		List<Protos.Volume> vols = new ArrayList<Protos.Volume>();
		for (String s : specs) {
			if (s.trim().isEmpty()) {
				continue;
			}
			Protos.Volume.Builder vol = Protos.Volume.newBuilder();
			vol.setMode(Protos.Volume.Mode.RW);

			String[] parts = s.split(":");
			switch (parts.length) {
				case 1:
					vol.setContainerPath(parts[0]);
					break;
				case 2:
					try {
						Protos.Volume.Mode mode = Protos.Volume.Mode.valueOf(parts[1].trim().toUpperCase());
						vol.setMode(mode)
								.setContainerPath(parts[0]);
					} catch (IllegalArgumentException e) {
						vol.setHostPath(parts[0])
								.setContainerPath(parts[1]);
					}
					break;
				case 3:
					Protos.Volume.Mode mode = Protos.Volume.Mode.valueOf(parts[2].trim().toUpperCase());
					vol.setMode(mode)
							.setHostPath(parts[0])
							.setContainerPath(parts[1]);
					break;
				default:
					throw new IllegalArgumentException("volume specification is invalid, given: " + s);
			}

			vols.add(vol.build());
		}
		return vols;
	}

	public enum ContainerType {
		MESOS,
		DOCKER
	}
}
