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
import org.apache.flink.kubernetes.utils.KubernetesUtils;

import io.fabric8.kubernetes.api.model.HasMetadata;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Creates an external Service to expose the rest port of the Flink JobManager(s).
 */
public class ExternalServiceDecorator extends AbstractServiceDecorator {

	public ExternalServiceDecorator(KubernetesJobManagerParameters kubernetesJobManagerParameters) {
		super(kubernetesJobManagerParameters);
	}

	@Override
	public List<HasMetadata> buildAccompanyingKubernetesResources() throws IOException {
		if (kubernetesJobManagerParameters.getRestServiceExposedType().equals(
			KubernetesConfigOptions.ServiceExposedType.ClusterIP.name())) {
			return Collections.emptyList();
		}

		return super.buildAccompanyingKubernetesResources();
	}

	@Override
	protected String getServiceType() {
		return kubernetesJobManagerParameters.getRestServiceExposedType();
	}

	@Override
	protected String getServiceName() {
		return KubernetesUtils.getRestServiceName(kubernetesJobManagerParameters.getClusterId());
	}
}
