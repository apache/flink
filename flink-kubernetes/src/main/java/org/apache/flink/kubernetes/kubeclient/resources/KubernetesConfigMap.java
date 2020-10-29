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

package org.apache.flink.kubernetes.kubeclient.resources;

import io.fabric8.kubernetes.api.model.ConfigMap;

import java.util.HashMap;
import java.util.Map;

/**
 * Represent {@link ConfigMap} resource in kubernetes.
 */
public class KubernetesConfigMap extends KubernetesResource<ConfigMap> {

	public KubernetesConfigMap(ConfigMap configMap) {
		super(configMap);
	}

	public String getName() {
		return this.getInternalResource().getMetadata().getName();
	}

	public String getResourceVersion() {
		return this.getInternalResource().getMetadata().getResourceVersion();
	}

	public Map<String, String> getAnnotations() {
		if (this.getInternalResource().getMetadata().getAnnotations() == null) {
			this.getInternalResource().getMetadata().setAnnotations(new HashMap<>());
		}
		return this.getInternalResource().getMetadata().getAnnotations();
	}

	public Map<String, String> getData() {
		if (this.getInternalResource().getData() == null) {
			this.getInternalResource().setData(new HashMap<>());
		}
		return this.getInternalResource().getData();
	}

	public Map<String, String> getLabels() {
		if (this.getInternalResource().getMetadata().getLabels() == null) {
			this.getInternalResource().getMetadata().setLabels(new HashMap<>());
		}
		return this.getInternalResource().getMetadata().getLabels();
	}
}
