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

import org.apache.flink.kubernetes.utils.Constants;

import java.util.HashMap;
import java.util.Map;

/**
 * Builder to create label for jobmanager and taskmanager.
 */
public class LabelBuilder {

	private final Map<String, String> labels;

	public LabelBuilder() {
		labels = new HashMap<>();
	}

	private LabelBuilder withLabel(String key, String value) {
		this.labels.put(key, value);
		return this;
	}

	public LabelBuilder withCommon() {
		return this.withLabel(Constants.LABEL_TYPE_KEY, Constants.LABEL_TYPE_NATIVE_TYPE);
	}

	public LabelBuilder withExist(Map<String, String> labels) {
		this.labels.putAll(labels);
		return this;
	}

	public LabelBuilder withClusterId(String id) {
		return this.withLabel(Constants.LABEL_APP_KEY, id);
	}

	public LabelBuilder withJobManagerComponent() {
		return this.withLabel(Constants.LABEL_COMPONENT_KEY, Constants.LABEL_COMPONENT_JOB_MANAGER);
	}

	public LabelBuilder withTaskManagerComponent() {
		return this.withLabel(Constants.LABEL_COMPONENT_KEY, Constants.LABEL_COMPONENT_TASK_MANAGER);
	}

	public Map<String, String> toLabels() {
		return labels;
	}
}
