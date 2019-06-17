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

import java.util.HashMap;
import java.util.Map;

/**
 * Manager labels.
 * */
public class LabelBuilder {

	private Map<String, String> labels;

	private LabelBuilder(){
		this(new HashMap<>());
	}

	private LabelBuilder(Map<String, String> labels){
		this.labels = labels;
	}

	private LabelBuilder withLabel(String key, String value){
		this.labels.put(key, value);
		return this;
	}

	public static LabelBuilder withCommon(){
		return new LabelBuilder()
			.withLabel("app", "flink-native-k8s");
	}

	public static LabelBuilder withExist(Map<String, String> labels){
		return new LabelBuilder(labels);
	}

	public LabelBuilder withClusterId(String id){
		return this.withLabel("appId", id);
	}

	public LabelBuilder withJobManagerRole(){
		return this.withLabel("role", "jobmanager");
	}

	public LabelBuilder withTaskManagerRole(){
		return this.withLabel("role", "taskmanager");
	}

	public Map<String, String> toLabels(){
		return labels;
	}
}
