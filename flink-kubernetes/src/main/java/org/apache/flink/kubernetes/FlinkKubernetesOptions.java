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
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.util.Preconditions;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;

import java.util.Properties;

import static org.apache.flink.runtime.entrypoint.parser.CommandLineOptions.DYNAMIC_PROPERTY_OPTION;
import static org.apache.flink.runtime.entrypoint.parser.CommandLineOptions.HOST_OPTION;
import static org.apache.flink.runtime.entrypoint.parser.CommandLineOptions.REST_PORT_OPTION;

/**
 * Parameters that will be used in Flink on k8s cluster.
 * */
public class FlinkKubernetesOptions {

	private final Configuration configuration;

	private String clusterId;

	private String imageName;

	private String nameSpace = "default";

	private String kubeConfigFileName;

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

	public String getKubeConfigFilePath() {
		return kubeConfigFileName;
	}

	public void setKubeConfigFileName(String kubeConfigFileName) {
		this.kubeConfigFileName = kubeConfigFileName;
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

	public static final Option CLUSTERID_OPTION = Option.builder("cid")
		.longOpt("clusterid")
		.required(false)
		.hasArg(true)
		.argName("clusterid")
		.desc("the cluster id that will be used for current session.")
		.build();

	public static final Option KUBERNETES_MODE_OPTION = Option.builder("k8s")
		.longOpt("KubernetesMode")
		.required(false)
		.hasArg(false)
		.desc("Whether use Kubernetes as resource manager.")
		.build();

	public static final Option KUBERNETES_CONFIG_FILE_OPTION = Option.builder("kc")
		.longOpt("kubeConfig")
		.required(false)
		.hasArg(true)
		.argName("ConfigFilePath")
		.desc("The config file to for K8s API client.")
		.build();

	public static final Option HELP_OPTION = Option.builder("h")
		.longOpt("help")
		.required(false)
		.hasArg(false)
		.desc("Help for Kubernetes session CLI.")
		.build();

	public static final Option JAR_OPTION = Option.builder("j")
		.longOpt("jar")
		.required(false)
		.hasArg(true)
		.desc("Path to Flink jar file")
		.build();

	public static final Option DETACHED_OPTION = Option.builder("d")
		.longOpt("detached")
		.required(false)
		.hasArg(false)
		.desc("If present, runs the job in detached mode")
		.build();

	/**
	 * build FlinkKubernetesOption from commandline.
	 * */
	public static FlinkKubernetesOptions fromCommandLine(CommandLine commandLine){
		final Properties dynamicProperties = commandLine.getOptionProperties(DYNAMIC_PROPERTY_OPTION.getOpt());
		final String restPortString = commandLine.getOptionValue(REST_PORT_OPTION.getOpt(), "-1");
		int restPort = Integer.parseInt(restPortString);
		String hostname = commandLine.getOptionValue(HOST_OPTION.getOpt());
		final String imageName = commandLine.getOptionValue(IMAGE_OPTION.getOpt());
		final String clusterId = commandLine.getOptionValue(CLUSTERID_OPTION.getOpt());

		//hostname = hostname == null ? clusterId : hostname;
		Configuration configuration = GlobalConfiguration
			.loadConfiguration(ConfigurationUtils.createConfiguration(dynamicProperties));

		if (hostname != null) {
			System.out.print("rest.address is: " + hostname);
			configuration.setString(RestOptions.ADDRESS, hostname);
		}

		if (restPort == -1) {
			restPort = RestOptions.PORT.defaultValue();
			configuration.setInteger(RestOptions.PORT, restPort);
		}

		FlinkKubernetesOptions options = new FlinkKubernetesOptions(configuration);
		options.setClusterId(clusterId);
		options.setImageName(imageName);

		return options;
	}
}
