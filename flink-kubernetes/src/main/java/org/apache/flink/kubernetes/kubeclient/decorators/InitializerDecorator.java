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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesResource;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.util.Preconditions;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;

import java.util.Map;

/**
 * Initial setup for a kubernetes resource.
 */
public class InitializerDecorator <R extends HasMetadata, T extends KubernetesResource<R>> extends Decorator<R, T> {

	private final String name;

	private final String apiVersion;

	public InitializerDecorator() {
		this(null, Constants.API_VERSION);
	}

	public InitializerDecorator(String name) {
		this(name, Constants.API_VERSION);
	}

	public InitializerDecorator(String name, String apiVersion) {
		this.name = name;
		this.apiVersion = apiVersion;
	}

	@Override
	protected R decorateInternalResource(R resource, Configuration flinkConfig) {
		final String clusterId = flinkConfig.getString(KubernetesConfigOptions.CLUSTER_ID);
		Preconditions.checkNotNull(clusterId, "ClusterId must be specified!");

		final Map<String, String> labels = new LabelBuilder()
			.withCommon()
			.withClusterId(clusterId)
			.toLabels();

		final ObjectMeta meta = new ObjectMeta();
		meta.setName(name == null ? clusterId : name);
		meta.setLabels(labels);
		meta.setNamespace(flinkConfig.getString(KubernetesConfigOptions.NAMESPACE));

		resource.setApiVersion(apiVersion);
		resource.setMetadata(meta);
		return resource;
	}
}
