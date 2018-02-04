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

package org.apache.flink.mesos.entrypoint;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.mesos.configuration.MesosOptions;
import org.apache.flink.mesos.runtime.clusterframework.MesosConfigKeys;
import org.apache.flink.mesos.runtime.clusterframework.MesosTaskManagerParameters;
import org.apache.flink.mesos.util.MesosConfiguration;
import org.apache.flink.runtime.clusterframework.ContainerSpecification;
import org.apache.flink.runtime.clusterframework.overlays.CompositeContainerOverlay;
import org.apache.flink.runtime.clusterframework.overlays.FlinkDistributionOverlay;
import org.apache.flink.runtime.clusterframework.overlays.HadoopConfOverlay;
import org.apache.flink.runtime.clusterframework.overlays.HadoopUserOverlay;
import org.apache.flink.runtime.clusterframework.overlays.KeytabOverlay;
import org.apache.flink.runtime.clusterframework.overlays.Krb5ConfOverlay;
import org.apache.flink.runtime.clusterframework.overlays.SSLStoreOverlay;

import org.apache.mesos.Protos;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

/**
 * Utils for Mesos entry points.
 */
public class MesosEntrypointUtils {

	/**
	 * Loads and validates the Mesos scheduler configuration.
	 * @param flinkConfig the global configuration.
	 * @param hostname the hostname to advertise to the Mesos master.
	 */
	public static MesosConfiguration createMesosSchedulerConfiguration(Configuration flinkConfig, String hostname) {

		Protos.FrameworkInfo.Builder frameworkInfo = Protos.FrameworkInfo.newBuilder()
			.setHostname(hostname);
		Protos.Credential.Builder credential = null;

		if (!flinkConfig.contains(MesosOptions.MASTER_URL)) {
			throw new IllegalConfigurationException(MesosOptions.MASTER_URL.key() + " must be configured.");
		}
		String masterUrl = flinkConfig.getString(MesosOptions.MASTER_URL);

		Duration failoverTimeout = FiniteDuration.apply(
			flinkConfig.getInteger(
				MesosOptions.FAILOVER_TIMEOUT_SECONDS),
				TimeUnit.SECONDS);
		frameworkInfo.setFailoverTimeout(failoverTimeout.toSeconds());

		frameworkInfo.setName(flinkConfig.getString(
			MesosOptions.RESOURCEMANAGER_FRAMEWORK_NAME));

		frameworkInfo.setRole(flinkConfig.getString(
			MesosOptions.RESOURCEMANAGER_FRAMEWORK_ROLE));

		frameworkInfo.setUser(flinkConfig.getString(
			MesosOptions.RESOURCEMANAGER_FRAMEWORK_USER));

		if (flinkConfig.contains(MesosOptions.RESOURCEMANAGER_FRAMEWORK_PRINCIPAL)) {
			frameworkInfo.setPrincipal(flinkConfig.getString(
				MesosOptions.RESOURCEMANAGER_FRAMEWORK_PRINCIPAL));

			credential = Protos.Credential.newBuilder();
			credential.setPrincipal(frameworkInfo.getPrincipal());

			// some environments use a side-channel to communicate the secret to Mesos,
			// and thus don't set the 'secret' configuration setting
			if (flinkConfig.contains(MesosOptions.RESOURCEMANAGER_FRAMEWORK_SECRET)) {
				credential.setSecret(flinkConfig.getString(
					MesosOptions.RESOURCEMANAGER_FRAMEWORK_SECRET));
			}
		}

		MesosConfiguration mesos =
			new MesosConfiguration(masterUrl, frameworkInfo, scala.Option.apply(credential));

		return mesos;
	}

	public static MesosTaskManagerParameters createTmParameters(Configuration configuration, Logger log) {
		// TM configuration
		final MesosTaskManagerParameters taskManagerParameters = MesosTaskManagerParameters.create(configuration);

		log.info("TaskManagers will be created with {} task slots",
			taskManagerParameters.containeredParameters().numSlots());
		log.info("TaskManagers will be started with container size {} MB, JVM heap size {} MB, " +
				"JVM direct memory limit {} MB, {} cpus, {} gpus",
			taskManagerParameters.containeredParameters().taskManagerTotalMemoryMB(),
			taskManagerParameters.containeredParameters().taskManagerHeapSizeMB(),
			taskManagerParameters.containeredParameters().taskManagerDirectMemoryLimitMB(),
			taskManagerParameters.cpus(),
			taskManagerParameters.gpus());

		return taskManagerParameters;
	}

	public static ContainerSpecification createContainerSpec(Configuration configuration, Configuration dynamicProperties)
		throws Exception {
		// generate a container spec which conveys the artifacts/vars needed to launch a TM
		ContainerSpecification spec = new ContainerSpecification();

		// propagate the AM dynamic configuration to the TM
		spec.getDynamicConfiguration().addAll(dynamicProperties);

		applyOverlays(configuration, spec);

		return spec;
	}

	/**
	 * Generate a container specification as a TaskManager template.
	 *
	 * <p>This code is extremely Mesos-specific and registers all the artifacts that the TaskManager
	 * needs (such as JAR file, config file, ...) and all environment variables into a container specification.
	 * The Mesos fetcher then ensures that those artifacts will be copied into the task's sandbox directory.
	 * A lightweight HTTP server serves the artifacts to the fetcher.
	 */
	public static void applyOverlays(
		Configuration configuration, ContainerSpecification containerSpec) throws IOException {

		// create the overlays that will produce the specification
		CompositeContainerOverlay overlay = new CompositeContainerOverlay(
			FlinkDistributionOverlay.newBuilder().fromEnvironment(configuration).build(),
			HadoopConfOverlay.newBuilder().fromEnvironment(configuration).build(),
			HadoopUserOverlay.newBuilder().fromEnvironment(configuration).build(),
			KeytabOverlay.newBuilder().fromEnvironment(configuration).build(),
			Krb5ConfOverlay.newBuilder().fromEnvironment(configuration).build(),
			SSLStoreOverlay.newBuilder().fromEnvironment(configuration).build()
		);

		// apply the overlays
		overlay.configure(containerSpec);
	}

	/**
	 * Loads the global configuration, adds the given dynamic properties configuration, and sets
	 * the temp directory paths.
	 *
	 * @param dynamicProperties dynamic properties to integrate
	 * @param log logger instance
	 * @return the loaded and adapted global configuration
	 */
	public static Configuration loadConfiguration(Configuration dynamicProperties, Logger log) {
		Configuration configuration =
			GlobalConfiguration.loadConfigurationWithDynamicProperties(dynamicProperties);

		// read the environment variables
		final Map<String, String> envs = System.getenv();
		final String tmpDirs = envs.get(MesosConfigKeys.ENV_FLINK_TMP_DIR);

		// configure local directory
		if (configuration.contains(CoreOptions.TMP_DIRS)) {
			log.info("Overriding Mesos' temporary file directories with those " +
				"specified in the Flink config: " + configuration.getValue(CoreOptions.TMP_DIRS));
		}
		else if (tmpDirs != null) {
			log.info("Setting directories for temporary files to: {}", tmpDirs);
			configuration.setString(CoreOptions.TMP_DIRS, tmpDirs);
		}

		return configuration;
	}
}
