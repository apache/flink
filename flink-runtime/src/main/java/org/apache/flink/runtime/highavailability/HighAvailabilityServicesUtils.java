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

package org.apache.flink.runtime.highavailability;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.blob.BlobStoreService;
import org.apache.flink.runtime.blob.BlobUtils;
import org.apache.flink.runtime.highavailability.nonha.embedded.EmbeddedHaServices;
import org.apache.flink.runtime.highavailability.nonha.standalone.StandaloneHaServices;
import org.apache.flink.runtime.highavailability.zookeeper.ZooKeeperHaServices;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.runtime.jobmaster.JobMaster;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.rpc.akka.AkkaRpcServiceUtils;
import org.apache.flink.runtime.util.LeaderRetrievalUtils;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.util.ConfigurationException;

import java.util.concurrent.Executor;

/**
 * Utils class to instantiate {@link HighAvailabilityServices} implementations.
 */
public class HighAvailabilityServicesUtils {

	public static HighAvailabilityServices createAvailableOrEmbeddedServices(
		Configuration config,
		Executor executor) throws Exception {
		HighAvailabilityMode highAvailabilityMode = LeaderRetrievalUtils.getRecoveryMode(config);

		switch (highAvailabilityMode) {
			case NONE:
				return new EmbeddedHaServices(executor);

			case ZOOKEEPER:
				BlobStoreService blobStoreService = BlobUtils.createBlobStoreFromConfig(config);

				return new ZooKeeperHaServices(
					ZooKeeperUtils.startCuratorFramework(config),
					executor,
					config,
					blobStoreService);

			default:
				throw new Exception("High availability mode " + highAvailabilityMode + " is not supported.");
		}
	}
	
	public static HighAvailabilityServices createHighAvailabilityServices(
		Configuration configuration,
		Executor executor,
		AddressResolution addressResolution) throws Exception {

		HighAvailabilityMode highAvailabilityMode = LeaderRetrievalUtils.getRecoveryMode(configuration);

		switch(highAvailabilityMode) {
			case NONE:
				final Tuple2<String, Integer> hostnamePort = getJobManagerAddress(configuration);

				final String jobManagerRpcUrl = AkkaRpcServiceUtils.getRpcUrl(
					hostnamePort.f0,
					hostnamePort.f1,
					JobMaster.JOB_MANAGER_NAME,
					addressResolution,
					configuration);
				final String resourceManagerRpcUrl = AkkaRpcServiceUtils.getRpcUrl(
					hostnamePort.f0,
					hostnamePort.f1,
					ResourceManager.RESOURCE_MANAGER_NAME,
					addressResolution,
					configuration);

				return new StandaloneHaServices(resourceManagerRpcUrl, jobManagerRpcUrl);
			case ZOOKEEPER:
				BlobStoreService blobStoreService = BlobUtils.createBlobStoreFromConfig(configuration);

				return new ZooKeeperHaServices(
					ZooKeeperUtils.startCuratorFramework(configuration),
					executor,
					configuration,
					blobStoreService);
			default:
				throw new Exception("Recovery mode " + highAvailabilityMode + " is not supported.");
		}
	}

	/**
	 * Returns the JobManager's hostname and port extracted from the given
	 * {@link Configuration}.
	 *
	 * @param configuration Configuration to extract the JobManager's address from
	 * @return The JobManager's hostname and port
	 * @throws ConfigurationException if the JobManager's address cannot be extracted from the configuration
	 */
	public static Tuple2<String, Integer> getJobManagerAddress(Configuration configuration) throws ConfigurationException {

		final String hostname = configuration.getString(JobManagerOptions.ADDRESS);
		final int port = configuration.getInteger(JobManagerOptions.PORT);

		if (hostname == null) {
			throw new ConfigurationException("Config parameter '" + JobManagerOptions.ADDRESS +
				"' is missing (hostname/address of JobManager to connect to).");
		}

		if (port <= 0 || port >= 65536) {
			throw new ConfigurationException("Invalid value for '" + JobManagerOptions.PORT +
				"' (port of the JobManager actor system) : " + port +
				".  it must be greater than 0 and less than 65536.");
		}

		return Tuple2.of(hostname, port);
	}

	public enum AddressResolution {
		TRY_ADDRESS_RESOLUTION,
		NO_ADDRESS_RESOLUTION
	}
}
