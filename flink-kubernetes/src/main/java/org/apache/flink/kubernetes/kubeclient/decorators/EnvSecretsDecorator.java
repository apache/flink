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

import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.parameters.AbstractKubernetesParameters;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesSecretEnvVar;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Support setting environment variables via Secrets.
 */
public class EnvSecretsDecorator extends AbstractKubernetesStepDecorator {

	private final AbstractKubernetesParameters kubernetesComponentConf;

	public EnvSecretsDecorator(AbstractKubernetesParameters kubernetesComponentConf) {
		this.kubernetesComponentConf = checkNotNull(kubernetesComponentConf);
	}

	@Override
	public FlinkPod decorateFlinkPod(FlinkPod flinkPod) {
		final Container basicMainContainer = new ContainerBuilder(flinkPod.getMainContainer())
			.addAllToEnv(getSecretEnvs())
			.build();

		return new FlinkPod.Builder(flinkPod)
			.withMainContainer(basicMainContainer)
			.build();
	}

	private List<EnvVar> getSecretEnvs() {
		return kubernetesComponentConf.getEnvironmentsFromSecrets().stream()
			.map(e -> KubernetesSecretEnvVar.fromMap(e).getInternalResource())
			.collect(Collectors.toList());
	}
}
