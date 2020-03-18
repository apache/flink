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

import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.parameters.KubernetesJobManagerParameters;
import org.apache.flink.kubernetes.utils.Constants;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.ServicePortBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An abstract class containing some common implementations for the internal/external Services.
 */
public abstract class AbstractServiceDecorator extends AbstractKubernetesStepDecorator {

	protected final KubernetesJobManagerParameters kubernetesJobManagerParameters;

	public AbstractServiceDecorator(KubernetesJobManagerParameters kubernetesJobManagerParameters) {
		this.kubernetesJobManagerParameters = checkNotNull(kubernetesJobManagerParameters);
	}

	@Override
	public List<HasMetadata> buildAccompanyingKubernetesResources() throws IOException {
		final Service service = new ServiceBuilder()
			.withApiVersion(Constants.API_VERSION)
			.withNewMetadata()
				.withName(getServiceName())
				.withLabels(kubernetesJobManagerParameters.getCommonLabels())
				.endMetadata()
			.withNewSpec()
				.withType(getServiceType().name())
				.withPorts(getServicePorts())
				.withSelector(kubernetesJobManagerParameters.getLabels())
				.endSpec()
			.build();

		return Collections.singletonList(service);
	}

	protected abstract KubernetesConfigOptions.ServiceExposedType getServiceType();

	protected abstract String getServiceName();

	protected List<ServicePort> getServicePorts() {
		final List<ServicePort> servicePorts = new ArrayList<>();

		servicePorts.add(getServicePort(
			Constants.REST_PORT_NAME,
			kubernetesJobManagerParameters.getRestPort()));

		return servicePorts;
	}

	protected static ServicePort getServicePort(String name, int port) {
		return new ServicePortBuilder()
			.withName(name)
			.withPort(port)
			.build();
	}
}
