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

import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.kubernetes.kubeclient.parameters.KubernetesJobManagerParameters;
import org.apache.flink.kubernetes.utils.Constants;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Creates an internal Service which forwards the requests from the TaskManager(s) to the
 * active JobManager.
 * Note that only the non-HA scenario relies on this Service for internal communication, since
 * in the HA mode, the TaskManager(s) directly connects to the JobManager via IP address.
 */
public class InternalServiceDecorator extends AbstractKubernetesStepDecorator {

	private final KubernetesJobManagerParameters kubernetesJobManagerParameters;

	public InternalServiceDecorator(KubernetesJobManagerParameters kubernetesJobManagerParameters) {
		this.kubernetesJobManagerParameters = checkNotNull(kubernetesJobManagerParameters);
	}

	@Override
	public List<HasMetadata> buildAccompanyingKubernetesResources() throws IOException {
		if (!kubernetesJobManagerParameters.isInternalServiceEnabled()) {
			return Collections.emptyList();
		}

		final String serviceName = getInternalServiceName(kubernetesJobManagerParameters.getClusterId());

		final Service headlessService = new ServiceBuilder()
			.withApiVersion(Constants.API_VERSION)
			.withNewMetadata()
				.withName(serviceName)
				.withLabels(kubernetesJobManagerParameters.getCommonLabels())
				.endMetadata()
			.withNewSpec()
				.withClusterIP(Constants.HEADLESS_SERVICE_CLUSTER_IP)
				.withSelector(kubernetesJobManagerParameters.getLabels())
				.addNewPort()
					.withName(Constants.JOB_MANAGER_RPC_PORT_NAME)
					.withPort(kubernetesJobManagerParameters.getRPCPort())
					.endPort()
				.addNewPort()
					.withName(Constants.BLOB_SERVER_PORT_NAME)
					.withPort(kubernetesJobManagerParameters.getBlobServerPort())
					.endPort()
				.endSpec()
			.build();

		// Set job manager address to namespaced service name
		final String namespace = kubernetesJobManagerParameters.getNamespace();
		kubernetesJobManagerParameters.getFlinkConfiguration().setString(
			JobManagerOptions.ADDRESS,
			getNamespacedInternalServiceName(serviceName, namespace));

		return Collections.singletonList(headlessService);
	}


	/**
	 * Generate name of the internal Service.
	 */
	public static String getInternalServiceName(String clusterId) {
		return clusterId;
	}

	/**
	 * Generate namespaced name of the internal Service.
	 */
	public static String getNamespacedInternalServiceName(String clusterId, String namespace) {
		return getInternalServiceName(clusterId) + "." + namespace;
	}
}


