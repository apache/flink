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

package org.apache.flink.kubernetes.taskmanager;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.kubernetes.resourcemanager.KubernetesResourceManager;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.entrypoint.ClusterConfiguration;
import org.apache.flink.runtime.entrypoint.ClusterConfigurationParserFactory;
import org.apache.flink.runtime.entrypoint.FlinkParseException;
import org.apache.flink.runtime.entrypoint.parser.CommandLineParser;
import org.apache.flink.runtime.taskexecutor.TaskManagerRunner;
import org.apache.flink.util.Preconditions;

/**
 * Kubernetes specific implementation of the {@link TaskManagerRunner}.
 */
public class KubernetesTaskManagerRunner extends TaskManagerRunner {
	public KubernetesTaskManagerRunner(Configuration configuration, ResourceID resourceId) throws Exception {
		super(configuration, resourceId);
	}

	public static void main(String[] args) throws Exception {

		final CommandLineParser<ClusterConfiguration>
			commandLineParser = new CommandLineParser<>(new ClusterConfigurationParserFactory());

		final ClusterConfiguration clusterConfiguration;

		try {
			clusterConfiguration = commandLineParser.parse(args);
		} catch (FlinkParseException e) {
			commandLineParser.printHelp(TaskManagerRunner.class.getSimpleName());
			throw e;
		}

		final Configuration dynamicProperties = ConfigurationUtils.createConfiguration(clusterConfiguration.getDynamicProperties());
		Configuration configuration = GlobalConfiguration
			.loadConfiguration(clusterConfiguration.getConfigDir(), dynamicProperties);

		FileSystem.initialize(configuration, null);

		final String resourceID = System.getenv().get(KubernetesResourceManager.ENV_RESOURCE_ID);
		Preconditions.checkArgument(resourceID != null,
			"Resource ID variable %s not set", KubernetesResourceManager.ENV_RESOURCE_ID);

		TaskManagerRunner.runTaskManager(configuration, new ResourceID(resourceID));
	}
}
