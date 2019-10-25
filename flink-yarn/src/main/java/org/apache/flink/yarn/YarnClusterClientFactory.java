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

package org.apache.flink.yarn;

import org.apache.flink.client.deployment.ClusterClientFactory;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.yarn.cli.YarnConfigUtils;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.configuration.YarnConfigOptionsInternal;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link ClusterClientFactory} for a YARN cluster.
 */
public class YarnClusterClientFactory implements ClusterClientFactory<ApplicationId> {

	private static final Logger LOG = LoggerFactory.getLogger(YarnClusterClientFactory.class);

	public static final String ID = "yarn-cluster";

	@Override
	public boolean isCompatibleWith(Configuration configuration) {
		checkNotNull(configuration);
		return ID.equals(configuration.getString(DeploymentOptions.TARGET));
	}

	@Override
	public YarnClusterDescriptor createClusterDescriptor(Configuration configuration) {
		checkNotNull(configuration);

		final YarnClusterDescriptor yarnClusterDescriptor = getClusterDescriptor(configuration);
		yarnClusterDescriptor.setDetachedMode(!configuration.getBoolean(DeploymentOptions.ATTACHED));

		getLocalFlinkDistPath(configuration, yarnClusterDescriptor)
				.ifPresent(yarnClusterDescriptor::setLocalJarPath);

		decodeDirsToShipToCluster(configuration)
				.ifPresent(yarnClusterDescriptor::addShipFiles);

		handleConfigOption(configuration, YarnConfigOptions.APPLICATION_QUEUE, yarnClusterDescriptor::setQueue);
		handleConfigOption(configuration, YarnConfigOptionsInternal.DYNAMIC_PROPERTIES, yarnClusterDescriptor::setDynamicPropertiesEncoded);
		handleConfigOption(configuration, YarnConfigOptions.APPLICATION_NAME, yarnClusterDescriptor::setName);
		handleConfigOption(configuration, YarnConfigOptions.APPLICATION_TYPE, yarnClusterDescriptor::setApplicationType);
		handleConfigOption(configuration, YarnConfigOptions.NODE_LABEL, yarnClusterDescriptor::setNodeLabel);
		handleConfigOption(configuration, HighAvailabilityOptions.HA_CLUSTER_ID, yarnClusterDescriptor::setZookeeperNamespace);
		return yarnClusterDescriptor;
	}

	@Nullable
	@Override
	public ApplicationId getClusterId(Configuration configuration) {
		checkNotNull(configuration);
		final String clusterId = configuration.getString(YarnConfigOptions.APPLICATION_ID);
		return clusterId != null ? ConverterUtils.toApplicationId(clusterId) : null;
	}

	@Override
	public ClusterSpecification getClusterSpecification(Configuration configuration) {
		checkNotNull(configuration);

		// JobManager Memory
		final int jobManagerMemoryMB = ConfigurationUtils.getJobManagerHeapMemory(configuration).getMebiBytes();

		// Task Managers memory
		final int taskManagerMemoryMB = ConfigurationUtils.getTaskManagerHeapMemory(configuration).getMebiBytes();

		int slotsPerTaskManager = configuration.getInteger(TaskManagerOptions.NUM_TASK_SLOTS);

		return new ClusterSpecification.ClusterSpecificationBuilder()
				.setMasterMemoryMB(jobManagerMemoryMB)
				.setTaskManagerMemoryMB(taskManagerMemoryMB)
				.setSlotsPerTaskManager(slotsPerTaskManager)
				.createClusterSpecification();
	}

	private Optional<List<File>> decodeDirsToShipToCluster(final Configuration configuration) {
		checkNotNull(configuration);

		final List<File> files = YarnConfigUtils.decodeListFromConfig(configuration, YarnConfigOptions.SHIP_DIRECTORIES, File::new);
		return files.isEmpty() ? Optional.empty() : Optional.of(files);
	}

	private Optional<Path> getLocalFlinkDistPath(final Configuration configuration, final YarnClusterDescriptor yarnClusterDescriptor) {
		final String localJarPath = configuration.getString(YarnConfigOptions.FLINK_DIST_JAR);
		if (localJarPath != null) {
			return Optional.of(new Path(localJarPath));
		}

		LOG.info("No path for the flink jar passed. Using the location of " + yarnClusterDescriptor.getClass() + " to locate the jar");

		// check whether it's actually a jar file --> when testing we execute this class without a flink-dist jar
		final String decodedPath = getDecodedJarPath(yarnClusterDescriptor);
		return decodedPath.endsWith(".jar")
				? Optional.of(new Path(new File(decodedPath).toURI()))
				: Optional.empty();
	}

	private String getDecodedJarPath(final YarnClusterDescriptor yarnClusterDescriptor) {
		final String encodedJarPath = yarnClusterDescriptor
				.getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
		try {
			return URLDecoder.decode(encodedJarPath, Charset.defaultCharset().name());
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException("Couldn't decode the encoded Flink dist jar path: " + encodedJarPath +
					" You can supply a path manually via the command line.");
		}
	}

	private void handleConfigOption(final Configuration configuration, final ConfigOption<String> option, final Consumer<String> consumer) {
		checkNotNull(configuration);
		checkNotNull(option);
		checkNotNull(consumer);

		final String value = configuration.getString(option);
		if (value != null) {
			consumer.accept(value);
		}
	}

	private YarnClusterDescriptor getClusterDescriptor(Configuration configuration) {
		final YarnClient yarnClient = YarnClient.createYarnClient();
		final YarnConfiguration yarnConfiguration = new YarnConfiguration();

		yarnClient.init(yarnConfiguration);
		yarnClient.start();

		return new YarnClusterDescriptor(
				configuration,
				yarnConfiguration,
				yarnClient,
				false);
	}
}
