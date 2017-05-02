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

package org.apache.flink.runtime.rpc;

import akka.actor.ActorSystem;
import com.typesafe.config.Config;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.net.SSLUtils;
import org.apache.flink.runtime.rpc.akka.AkkaRpcService;
import org.apache.flink.util.NetUtils;

import org.apache.flink.util.Preconditions;
import org.jboss.netty.channel.ChannelException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * These RPC utilities contain helper methods around RPC use, such as starting an RPC service,
 * or constructing RPC addresses.
 */
public class RpcServiceUtils {

	private static final Logger LOG = LoggerFactory.getLogger(RpcServiceUtils.class);

	private static final AtomicLong nextNameOffset = new AtomicLong(0L);

	// ------------------------------------------------------------------------
	//  RPC instantiation
	// ------------------------------------------------------------------------

	/**
	 * Utility method to create RPC service from configuration and hostname, port.
	 *
	 * @param hostname   The hostname/address that describes the TaskManager's data location.
	 * @param port           If true, the TaskManager will not initiate the TCP network stack.
	 * @param configuration                 The configuration for the TaskManager.
	 * @return   The rpc service which is used to start and connect to the TaskManager RpcEndpoint .
	 * @throws IOException      Thrown, if the actor system can not bind to the address
	 * @throws Exception      Thrown is some other error occurs while creating akka actor system
	 */
	public static RpcService createRpcService(String hostname, int port, Configuration configuration) throws Exception {
		LOG.info("Starting AkkaRpcService at {}.", NetUtils.hostAndPortToUrlString(hostname, port));

		final ActorSystem actorSystem;

		try {
			Config akkaConfig;

			if (hostname != null && !hostname.isEmpty()) {
				// remote akka config
				akkaConfig = AkkaUtils.getAkkaConfig(configuration, hostname, port);
			} else {
				// local akka config
				akkaConfig = AkkaUtils.getAkkaConfig(configuration);
			}

			LOG.debug("Using akka configuration \n {}.", akkaConfig);

			actorSystem = AkkaUtils.createActorSystem(akkaConfig);
		} catch (Throwable t) {
			if (t instanceof ChannelException) {
				Throwable cause = t.getCause();
				if (cause != null && t.getCause() instanceof java.net.BindException) {
					String address = NetUtils.hostAndPortToUrlString(hostname, port);
					throw new IOException("Unable to bind AkkaRpcService actor system to address " +
						address + " - " + cause.getMessage(), t);
				}
			}
			throw new Exception("Could not create TaskManager actor system", t);
		}

		final Time timeout = Time.milliseconds(AkkaUtils.getTimeout(configuration).toMillis());
		return new AkkaRpcService(actorSystem, timeout);
	}

	// ------------------------------------------------------------------------
	//  RPC endpoint addressing
	// ------------------------------------------------------------------------

	/**
	 *
	 * @param hostname     The hostname or address where the target RPC service is listening.
	 * @param port         The port where the target RPC service is listening.
	 * @param endpointName The name of the RPC endpoint.
	 * @param config       The configuration from which to deduce further settings.
	 *
	 * @return The RPC URL of the specified RPC endpoint.
	 */
	public static String getRpcUrl(String hostname, int port, String endpointName, Configuration config)
			throws UnknownHostException {

		checkNotNull(config, "config is null");

		final boolean sslEnabled = config.getBoolean(
					ConfigConstants.AKKA_SSL_ENABLED,
					ConfigConstants.DEFAULT_AKKA_SSL_ENABLED) &&
				SSLUtils.getSSLEnabled(config);

		return getRpcUrl(hostname, port, endpointName, sslEnabled);
	}

	/**
	 * 
	 * @param hostname     The hostname or address where the target RPC service is listening.
	 * @param port         The port where the target RPC service is listening.
	 * @param endpointName The name of the RPC endpoint.
	 * @param secure       True, if security/encryption is enabled, false otherwise.
	 * 
	 * @return The RPC URL of the specified RPC endpoint.
	 */
	public static String getRpcUrl(String hostname, int port, String endpointName, boolean secure)
			throws UnknownHostException {

		checkNotNull(hostname, "hostname is null");
		checkNotNull(endpointName, "endpointName is null");
		checkArgument(port > 0 && port <= 65535, "port must be in [1, 65535]");

		final String protocol = secure ? "akka.ssl.tcp" : "akka.tcp";
		final String hostPort = NetUtils.hostAndPortToUrlString(hostname, port);

		return String.format("%s://flink@%s/user/%s", protocol, hostPort, endpointName);
	}

	/**
	 * Creates a random name of the form prefix_X, where X is an increasing number.
	 *
	 * @param prefix Prefix string to prepend to the monotonically increasing name offset number
	 * @return A random name of the form prefix_X where X is an increasing number
	 */
	public static String createRandomName(String prefix) {
		Preconditions.checkNotNull(prefix, "Prefix must not be null.");

		long nameOffset;

		// obtain the next name offset by incrementing it atomically
		do {
			nameOffset = nextNameOffset.get();
		} while (!nextNameOffset.compareAndSet(nameOffset, nameOffset + 1L));

		return prefix + '_' + nameOffset;
	}

	// ------------------------------------------------------------------------

	/** This class is not meant to be instantiated */
	private RpcServiceUtils() {}
}
