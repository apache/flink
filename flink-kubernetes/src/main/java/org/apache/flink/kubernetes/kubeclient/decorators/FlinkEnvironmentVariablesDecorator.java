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

package org.apache.flink.kubernetes.kubeclient.decorators;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.parameters.AbstractKubernetesParameters;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;

import java.util.LinkedList;
import java.util.List;


/**
 * Creates environment variables for containers
 * Resource definition is contained in FlinkConfiguration as follows:
 *  for job manager:
 * 			"kubernetes.jobmanager.environment"
 * 	for task manager:
 * 			"kubernetes.taskmanager.environment"
 * Each key can contain multiple environment variables separated by ",". the environment variable itself is defined
 * as a key-value pair
 */
public class FlinkEnvironmentVariablesDecorator extends AbstractKubernetesStepDecorator{

	private final AbstractKubernetesParameters kubernetesComponentConf;
	private List<EnvVar> envVarList = new LinkedList<>();
	private ConfigOption<String> envkey;

	public FlinkEnvironmentVariablesDecorator(AbstractKubernetesParameters kubernetesComponentConf, ConfigOption<String> envkey) {
		this.envkey = envkey;
		this.kubernetesComponentConf = kubernetesComponentConf;
	}

	@Override
	public FlinkPod decorateFlinkPod(FlinkPod flinkPod) {

		// buil the list of environments
		getEnvInfo(kubernetesComponentConf.getFlinkConfiguration().getString(envkey));

		// Check if we need to add any resources
		if (envVarList.size() < 1) {
			return flinkPod;
		}

		// Get current environment variables and merge them with new ones
		List<EnvVar> current = flinkPod.getMainContainer().getEnv();
		if ((current != null) && (current.size() > 0)) {
			envVarList.addAll(current);
		}

		// Update container
		final Container containerWithEnvironment = new ContainerBuilder(flinkPod.getMainContainer())
			.withEnv(envVarList)
			.build();

		// Rebuild POd
		return new FlinkPod.Builder(flinkPod)
			.withMainContainer(containerWithEnvironment)
			.build();
	}

	/**
 	 * Build descriptors from config string.
	 * @param  envInfoString - string representation of env variables
	 */
	private void getEnvInfo(String envInfoString){
		if (envInfoString == null){
			return;
		}
		String[] envs = envInfoString.split(",");
		for (String env : envs){
			String[] envInfo = env.split(":");
			if (envInfo.length == 2) {
				envVarList.add(new EnvVar(envInfo[0], envInfo[1], null));
			}
		}
	}
}
