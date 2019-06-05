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

package org.apache.flink.kubernetes.kubeclient;

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;

import java.util.List;
import java.util.Map;

/**
 * Parameters to create a taskmanager pod.
 * */
public class TaskManagerPodParameter {

	private String podName;

	private String imageName;

	private List<String> args;

	private ResourceProfile resourceProfile;

	private Map<String, String> environmentVariables;

	public TaskManagerPodParameter(String podName, String imageName, List<String> args, ResourceProfile resourceProfile, Map<String, String> environmentVariables) {
		this.podName = podName;
		this.imageName = imageName;
		this.args = args;
		this.resourceProfile = resourceProfile;
		this.environmentVariables = environmentVariables;
	}

	public String getPodName() {
		return podName;
	}

	public List<String> getArgs() {
		return args;
	}

	public ResourceProfile getResourceProfile() {
		return resourceProfile;
	}

	public Map<String, String> getEnvironmentVariables() {
		return environmentVariables;
	}

	public String getImageName() {
		return imageName;
	}
}
