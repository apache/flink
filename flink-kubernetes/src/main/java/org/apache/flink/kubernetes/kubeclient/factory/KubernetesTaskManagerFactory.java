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

package org.apache.flink.kubernetes.kubeclient.factory;

import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.decorators.EnvSecretsDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.FlinkConfMountDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.HadoopConfMountDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.InitTaskManagerDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.JavaCmdTaskManagerDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.KerberosMountDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.KubernetesStepDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.MountSecretsDecorator;
import org.apache.flink.kubernetes.kubeclient.parameters.KubernetesTaskManagerParameters;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesPod;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;

/**
 * Utility class for constructing the TaskManager Pod on the JobManager.
 */
public class KubernetesTaskManagerFactory {

	public static KubernetesPod buildTaskManagerKubernetesPod(KubernetesTaskManagerParameters kubernetesTaskManagerParameters) {
		FlinkPod flinkPod = new FlinkPod.Builder().build();

		final KubernetesStepDecorator[] stepDecorators = new KubernetesStepDecorator[] {
			new InitTaskManagerDecorator(kubernetesTaskManagerParameters),
			new EnvSecretsDecorator(kubernetesTaskManagerParameters),
			new MountSecretsDecorator(kubernetesTaskManagerParameters),
			new JavaCmdTaskManagerDecorator(kubernetesTaskManagerParameters),
			new HadoopConfMountDecorator(kubernetesTaskManagerParameters),
			new KerberosMountDecorator(kubernetesTaskManagerParameters),
			new FlinkConfMountDecorator(kubernetesTaskManagerParameters)};

		for (KubernetesStepDecorator stepDecorator: stepDecorators) {
			flinkPod = stepDecorator.decorateFlinkPod(flinkPod);
		}

		final Pod resolvedPod = new PodBuilder(flinkPod.getPod())
			.editOrNewSpec()
				.addToContainers(flinkPod.getMainContainer())
				.endSpec()
			.build();

		return new KubernetesPod(resolvedPod);
	}
}
