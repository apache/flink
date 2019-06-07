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
import org.apache.flink.kubernetes.kubeclient.fabric8.FlinkPod;
import org.apache.flink.util.Preconditions;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Pod;

import java.util.Map;

/**
 * Initial setup for a flink pod.
 * */
public class PodInitializerDecorator extends Decorator<Pod, FlinkPod> {

	@Override
	protected Pod doDecorate(Pod resource, FlinkKubernetesOptions flinkKubernetesOptions) {

		Preconditions.checkArgument(flinkKubernetesOptions != null && flinkKubernetesOptions.getClusterId() != null);

		Map<String, String> labels = org.apache.flink.kubernetes.kubeclient.fabric8.decorators.LabelBuilder
			.withCommon()
			.withClusterId(flinkKubernetesOptions.getClusterId())
			.toLabels();

		ObjectMeta meta = new ObjectMeta();
		meta.setName(flinkKubernetesOptions.getClusterId());
		meta.setLabels(labels);
		meta.setNamespace(flinkKubernetesOptions.getNameSpace());

		resource.setApiVersion(org.apache.flink.kubernetes.kubeclient.fabric8.decorators.Decorator.API_VERSION);
		resource.setMetadata(meta);
		return resource;
	}
}
