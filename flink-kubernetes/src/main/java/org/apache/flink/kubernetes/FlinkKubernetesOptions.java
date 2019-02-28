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

package org.apache.flink.kubernetes;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Preconditions;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * Parameters that will be used in Flink on k8s cluster.
 * */
public class FlinkKubernetesOptions {

	public static final ConfigOption<Boolean> DEBUG_MODE =
		key("k8s.debugmode.enable")
			.defaultValue(false)
			.withDescription("Whether enable debug mode.");

	public static final ConfigOption<String> EXTERNAL_IP =
		key("jobmanager.k8s.externalip")
			.defaultValue("")
			.withDescription("The external ip for job manager external IP.");

	private Configuration configuration;

	private String clusterId;

	private String namespace = "default";

	private String imageName;

	public FlinkKubernetesOptions(Configuration configuration, String clusterId) {
		Preconditions.checkArgument(configuration != null);
		this.configuration = configuration;
		this.clusterId = clusterId;
	}

	public String getClusterId() {
		return clusterId;
	}

	public void setClusterId(String clusterId) {
		this.clusterId = clusterId;
	}

	public String getImageName(){
		return this.imageName;
	}

	public String getNamespace() {
		return namespace;
	}

	public String getExternalIP() {
		return this.configuration.getString(FlinkKubernetesOptions.EXTERNAL_IP);
	}

	public void setNamespace(String namespace) {
		this.namespace = namespace;
	}

	public Integer getServicePort(ConfigOption<Integer> port) {
		return this.configuration.getInteger(port);
	}

	public Boolean getIsDebugMode() {
		return this.configuration.getBoolean(FlinkKubernetesOptions.DEBUG_MODE);
	}

	public Configuration getConfiguration() {
		return configuration;
	}
}
