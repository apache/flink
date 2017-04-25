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

import com.netflix.fenzo.ConstraintEvaluator;
import com.netflix.fenzo.functions.Func1;
import com.netflix.fenzo.plugins.HostAttrValueConstraint;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;
import scala.Option;

import java.util.ArrayList;
import java.util.Arrays;
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

	private final List<ConstraintEvaluator> constraints;

	public MesosTaskManagerParameters(
		double cpus,
		ContainerType containerType,
		Option<String> containerImageName,
		ContaineredTaskManagerParameters containeredParameters,
		List<ConstraintEvaluator> constraints) {
		requireNonNull(containeredParameters);
		this.cpus = cpus;
		this.containerType = containerType;
		this.containerImageName = containerImageName;
		this.containeredParameters = containeredParameters;
		this.constraints = constraints;
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
	 *
	 * Get the placement constraints
	 */
	public List<ConstraintEvaluator> constraints() {
		return constraints;
	}

	@Override
	public String toString() {
		return "MesosTaskManagerParameters{" +
			"cpus=" + cpus +
			", containerType=" + containerType +
			", containerImageName=" + containerImageName +
			", containeredParameters=" + containeredParameters +
			", constraints=" + constraints +
			'}';
	}

	/**
	 * Create the Mesos TaskManager parameters.
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

		return new MesosTaskManagerParameters(
			cpus,
			containerType,
			Option.apply(imageName),
			containeredParameters,
			constraints);
		}

	private static List<ConstraintEvaluator> parseConstraints(String mesosConstraints) {
		List<ConstraintEvaluator> constraints = new ArrayList<>();

		if (mesosConstraints != null) {
			for (String constraint : Arrays.asList(mesosConstraints.split(","))) {
				if (constraint.isEmpty()) {
					continue;
				}
				final List<String> constraintList = Arrays.asList(constraint.split(":"));
				if (constraintList.size() != 2) {
					continue;
				}
				addConstraint(constraints, constraintList);
			}
		}

		return constraints;
	}

	private static void addConstraint(List<ConstraintEvaluator> constraints, final List<String> constraintList) {
		constraints.add(new HostAttrValueConstraint(constraintList.get(0), new Func1<String, String>() {
			@Override
			public String call(String s) {
				return constraintList.get(1);
			}
		}));
	}

	public enum ContainerType {
		MESOS,
		DOCKER
	}
}
