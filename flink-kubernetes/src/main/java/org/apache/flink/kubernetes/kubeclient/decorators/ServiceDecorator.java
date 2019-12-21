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

import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesService;
import org.apache.flink.kubernetes.utils.Constants;

import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.ServicePortBuilder;
import io.fabric8.kubernetes.api.model.ServiceSpec;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Setup services port.
 */
public class ServiceDecorator extends Decorator<Service, KubernetesService> {

	private final KubernetesConfigOptions.ServiceExposedType exposedType;

	private final boolean onlyRestPort;

	public ServiceDecorator() {
		this(false);
	}

	public ServiceDecorator(boolean onlyRestPort) {
		this(KubernetesConfigOptions.ServiceExposedType.LoadBalancer, onlyRestPort);
	}

	public ServiceDecorator(KubernetesConfigOptions.ServiceExposedType exposedType, boolean onlyRestPort) {
		this.exposedType = exposedType;
		this.onlyRestPort = onlyRestPort;
	}

	private ServicePort getServicePort(String name, int port) {
		return new ServicePortBuilder()
			.withName(name)
			.withPort(port)
			.build();
	}

	private String getPortName(String portName){
		return portName.replace('.', '-');
	}

	@Override
	protected Service decorateInternalResource(Service resource, Configuration flinkConfig) {

		final ServiceSpec spec = resource.getSpec() != null ? resource.getSpec() : new ServiceSpec();

		spec.setType(exposedType.toString());

		final List<ServicePort> servicePorts = new ArrayList<>();
		servicePorts.add(getServicePort(
			getPortName(RestOptions.PORT.key()),
			flinkConfig.getInteger(RestOptions.PORT)));

		if (!onlyRestPort) {
			servicePorts.add(getServicePort(
				getPortName(JobManagerOptions.PORT.key()),
				flinkConfig.getInteger(JobManagerOptions.PORT)));
			servicePorts.add(getServicePort(
				getPortName(BlobServerOptions.PORT.key()),
				Constants.BLOB_SERVER_PORT));
		}

		spec.setPorts(servicePorts);

		final Map<String, String> labels = new LabelBuilder()
			.withExist(resource.getMetadata().getLabels())
			.withJobManagerComponent()
			.toLabels();

		spec.setSelector(labels);

		resource.setSpec(spec);

		return resource;
	}
}
