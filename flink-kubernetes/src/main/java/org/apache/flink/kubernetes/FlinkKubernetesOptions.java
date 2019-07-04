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

import org.apache.commons.cli.Option;

/**
 * Parameters that will be used in Flink on k8s cluster.
 * */
public class FlinkKubernetesOptions {

	private final Configuration configuration;

	private String clusterId;

	private String imageName;

	private String nameSpace = "default";

	private String kubeConfigFile;

	private String serviceUUID;

	public FlinkKubernetesOptions(Configuration configuration) {
		Preconditions.checkNotNull(configuration);
		this.configuration = configuration;
	}

	public Configuration getConfiguration() {
		return configuration;
	}

	public String getClusterId() {
		return clusterId;
	}

	public void setClusterId(String clusterId) {
		this.clusterId = clusterId;
	}

	public String getNameSpace() {
		return nameSpace;
	}

	public void setNameSpace(String nameSpace) {
		this.nameSpace = nameSpace;
	}

	public String getServiceUUID() {
		return serviceUUID;
	}

	public void setServiceUUID(String serviceUUID) {
		this.serviceUUID = serviceUUID;
	}

	public String getImageName(){
		return this.imageName;
	}

	public void setImageName(String imageName) {
		this.imageName = imageName;
	}

	public String getKubeConfigFile() {
		return kubeConfigFile;
	}

	public void setKubeConfigFile(String kubeConfigFileName) {
		this.kubeConfigFile = kubeConfigFileName;
	}

	public Integer getServicePort(ConfigOption<Integer> port) {
		return this.configuration.getInteger(port);
	}

	public static final Option IMAGE_OPTION = Option.builder("i")
		.longOpt("image")
		.required(false)
		.hasArg(true)
		.argName("image-name")
		.desc("the docker image name.")
		.build();

	public static final Option CLUSTERID_OPTION = Option.builder("id")
		.longOpt("clusterId")
		.required(false)
		.hasArg(true)
		.argName("clusterId")
		.desc("the cluster id that will be used for current session.")
		.build();

}
