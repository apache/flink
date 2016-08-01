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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;

import static java.util.Objects.requireNonNull;

public class MesosTaskManagerParameters {

	private double cpus;

	private ContaineredTaskManagerParameters containeredParameters;

	public MesosTaskManagerParameters(double cpus, ContaineredTaskManagerParameters containeredParameters) {
		requireNonNull(containeredParameters);
		this.cpus = cpus;
		this.containeredParameters = containeredParameters;
	}

	public double cpus() {
		return cpus;
	}

	public ContaineredTaskManagerParameters containeredParameters() {
		return containeredParameters;
	}

	@Override
	public String toString() {
		return "MesosTaskManagerParameters{" +
			"cpus=" + cpus +
			", containeredParameters=" + containeredParameters +
			'}';
	}

	/**
	 * Create the Mesos TaskManager parameters.
	 * @param flinkConfig the TM configuration.
	 * @param containeredParameters additional containered parameters.
     */
	public static MesosTaskManagerParameters create(
		Configuration flinkConfig,
		ContaineredTaskManagerParameters containeredParameters) {

		double cpus = flinkConfig.getDouble(ConfigConstants.MESOS_RESOURCEMANAGER_TASKS_CPUS,
			Math.max(containeredParameters.numSlots(), 1.0));

		return new MesosTaskManagerParameters(cpus, containeredParameters);
	}
}
