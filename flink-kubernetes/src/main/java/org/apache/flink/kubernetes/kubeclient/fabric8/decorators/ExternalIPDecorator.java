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

import org.apache.flink.kubernetes.FlinkKubernetesOptions;
import org.apache.flink.kubernetes.kubeclient.fabric8.FlinkService;
import org.apache.flink.util.Preconditions;

import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceSpec;

import java.util.Arrays;

/**
 * This decorator is for debug purpose.
 * */
public class ExternalIPDecorator extends Decorator<Service, FlinkService> {

	protected Boolean isEnabled(FlinkKubernetesOptions flinkKubernetesOptions) {
		return flinkKubernetesOptions.getIsDebugMode();
	}

	@Override
	protected Service doDecorate(Service resource, FlinkKubernetesOptions flinkKubernetesOptions) {

		String externalIP = flinkKubernetesOptions.getExternalIP();
		Preconditions.checkState(externalIP != null);

		ServiceSpec spec = resource.getSpec() != null ? resource.getSpec() : new ServiceSpec();
		spec.setExternalIPs(Arrays.asList(externalIP));
		resource.setSpec(spec);

		return resource;
	}
}
