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

package org.apache.flink.kubernetes.kubeclient.fabric8.decorators;

import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.QueryableStateOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.kubernetes.FlinkKubernetesOptions;
import org.apache.flink.kubernetes.kubeclient.fabric8.FlinkService;

import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.ServicePortBuilder;
import io.fabric8.kubernetes.api.model.ServiceSpec;

import java.util.Arrays;

/**
 * setup services port.
 * */
public class ServicePortDecorator extends Decorator<Service, FlinkService> {

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
	protected Service doDecorate(Service resource, FlinkKubernetesOptions flinkKubernetesOptions) {

		ServiceSpec spec = resource.getSpec() != null
			? resource.getSpec() : new ServiceSpec();

		spec.setPorts(Arrays.asList(
			//UI, reset
			getServicePort(getPortName(RestOptions.PORT.key()), flinkKubernetesOptions.getServicePort(RestOptions.PORT)),
			//rpc
			getServicePort(getPortName(JobManagerOptions.PORT.key()), flinkKubernetesOptions.getServicePort(JobManagerOptions.PORT)),
			//blob
			getServicePort(getPortName(BlobServerOptions.PORT.key()), 6124),
			getServicePort(getPortName(QueryableStateOptions.SERVER_PORT_RANGE.key()), 6125)));

		spec.setSelector(resource.getMetadata().getLabels());

		resource.setSpec(spec);

		return resource;
	}
}
