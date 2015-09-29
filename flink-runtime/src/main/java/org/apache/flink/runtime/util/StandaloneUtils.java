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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobmanager.JobManager;
import org.apache.flink.runtime.leaderretrieval.StandaloneLeaderRetrievalService;
import org.apache.flink.runtime.taskmanager.TaskManager;
import scala.Option;
import scala.Tuple2;

import java.net.InetAddress;
import java.net.InetSocketAddress;
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
	 * @throws UnknownHostException
	 */
	public static StandaloneLeaderRetrievalService createLeaderRetrievalService(
			Configuration configuration)
		throws UnknownHostException {
		return createLeaderRetrievalService(configuration, null);
	}

	/**
	 * Creates a {@link StandaloneLeaderRetrievalService} form the given configuration and the
	 * JobManager name. The host and port for the remote Akka URL are retrieved from the provided
	 * configuration. Instead of using the standard JobManager Akka name, the provided one is used
	 * for the remote Akka URL.
	 *
	 * @param configuration Configuration instance containing hte host and port information
	 * @param jobManagerName Name of the JobManager actor
	 * @return StandaloneLeaderRetrievalService
	 * @throws UnknownHostException
	 */
	public static StandaloneLeaderRetrievalService createLeaderRetrievalService(
			Configuration configuration,
			String jobManagerName)
		throws UnknownHostException {
		Tuple2<String, Object> stringIntPair = TaskManager.getAndCheckJobManagerAddress(configuration);

		String jobManagerHostname = stringIntPair._1();
		int jobManagerPort = (Integer) stringIntPair._2();
		InetSocketAddress hostPort;

		try {
			InetAddress inetAddress = InetAddress.getByName(jobManagerHostname);
			hostPort = new InetSocketAddress(inetAddress, jobManagerPort);
		}
		catch (UnknownHostException e) {
			throw new UnknownHostException("Cannot resolve the JobManager hostname '" + jobManagerHostname
					+ "' specified in the configuration");
		}

		String jobManagerAkkaUrl = JobManager.getRemoteJobManagerAkkaURL(
				hostPort,
				Option.apply(jobManagerName));

		return new StandaloneLeaderRetrievalService(jobManagerAkkaUrl);
	}

	/**
	 * Private constructor to prevent instantiation.
	 */
	private StandaloneUtils() {
		throw new RuntimeException();
	}
}
