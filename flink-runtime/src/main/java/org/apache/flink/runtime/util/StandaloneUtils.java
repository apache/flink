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

package org.apache.flink.runtime.util;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils.AddressResolution;
import org.apache.flink.runtime.jobmaster.JobMaster;
import org.apache.flink.runtime.leaderretrieval.StandaloneLeaderRetrievalService;
import org.apache.flink.runtime.rpc.akka.AkkaRpcServiceUtils;
import org.apache.flink.util.ConfigurationException;

import java.net.UnknownHostException;

/**
 * Utility class to work with Flink standalone mode.
 */
public final class StandaloneUtils {

	/**
	 * Creates a {@link StandaloneLeaderRetrievalService} from the given configuration. The
	 * host and port for the remote Akka URL are retrieved from the provided configuration.
	 *
	 * @param configuration Configuration instance containing the host and port information
	 * @return StandaloneLeaderRetrievalService
	 * @throws ConfigurationException
	 * @throws UnknownHostException
	 */
	public static StandaloneLeaderRetrievalService createLeaderRetrievalService(Configuration configuration)
		throws ConfigurationException, UnknownHostException {
		return createLeaderRetrievalService(
			configuration,
			false,
			null);
	}

	/**
	 * Creates a {@link StandaloneLeaderRetrievalService} form the given configuration and the
	 * JobManager name. The host and port for the remote Akka URL are retrieved from the provided
	 * configuration. Instead of using the standard JobManager Akka name, the provided one is used
	 * for the remote Akka URL.
	 *
	 * @param configuration Configuration instance containing hte host and port information
	 * @param resolveInitialHostName If true, resolves the hostname of the StandaloneLeaderRetrievalService
	 * @param jobManagerName Name of the JobManager actor
	 * @return StandaloneLeaderRetrievalService
	 * @throws ConfigurationException if the job manager address cannot be retrieved from the configuration
	 * @throws UnknownHostException if the job manager address cannot be resolved
	 */
	public static StandaloneLeaderRetrievalService createLeaderRetrievalService(
			Configuration configuration,
			boolean resolveInitialHostName,
			String jobManagerName)
		throws ConfigurationException, UnknownHostException {
		Tuple2<String, Integer> hostnamePort = HighAvailabilityServicesUtils.getJobManagerAddress(configuration);

		String jobManagerAkkaUrl = AkkaRpcServiceUtils.getRpcUrl(
			hostnamePort.f0,
			hostnamePort.f1,
			jobManagerName != null ? jobManagerName : JobMaster.JOB_MANAGER_NAME,
			resolveInitialHostName ? AddressResolution.TRY_ADDRESS_RESOLUTION : AddressResolution.NO_ADDRESS_RESOLUTION,
			configuration);

		return new StandaloneLeaderRetrievalService(jobManagerAkkaUrl);
	}

	/**
	 * Private constructor to prevent instantiation.
	 */
	private StandaloneUtils() {
		throw new RuntimeException();
	}
}
