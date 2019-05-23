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

package org.apache.flink.runtime.rpc.akka;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils.AddressResolution;
import org.apache.flink.runtime.net.SSLUtils;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.util.NetUtils;
import org.apache.flink.util.Preconditions;

import akka.actor.ActorSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * These RPC utilities contain helper methods around RPC use, such as starting an RPC service,
 * or constructing RPC addresses.
 */
public class AkkaRpcServiceUtils {

	private static final Logger LOG = LoggerFactory.getLogger(AkkaRpcServiceUtils.class);

	private static final String AKKA_TCP = "akka.tcp";
	private static final String AKKA_SSL_TCP = "akka.ssl.tcp";

	private static final String SIMPLE_AKKA_CONFIG_TEMPLATE =
		"akka {remote {netty.tcp {maximum-frame-size = %s}}}";

	private static final String MAXIMUM_FRAME_SIZE_PATH =
		"akka.remote.netty.tcp.maximum-frame-size";

	private static final AtomicLong nextNameOffset = new AtomicLong(0L);

	// ------------------------------------------------------------------------
	//  RPC instantiation
	// ------------------------------------------------------------------------

	/**
	 * Utility method to create RPC service from configuration and hostname, port.
	 *
	 * @param hostname   The hostname/address that describes the TaskManager's data location.
	 * @param portRangeDefinition   The port range to start TaskManager on.
	 * @param configuration                 The configuration for the TaskManager.
	 * @return   The rpc service which is used to start and connect to the TaskManager RpcEndpoint .
	 * @throws IOException      Thrown, if the actor system can not bind to the address
	 * @throws Exception      Thrown is some other error occurs while creating akka actor system
	 */
	public static RpcService createRpcService(
			String hostname,
			String portRangeDefinition,
			Configuration configuration) throws Exception {
		final ActorSystem actorSystem = BootstrapTools.startActorSystem(configuration, hostname, portRangeDefinition, LOG);
		return instantiateAkkaRpcService(configuration, actorSystem);
	}

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
	public static RpcService createRpcService(
			String hostname,
			int port,
			Configuration configuration) throws Exception {
		final ActorSystem actorSystem = BootstrapTools.startActorSystem(configuration, hostname, port, LOG);
		return instantiateAkkaRpcService(configuration, actorSystem);
	}

	/**
	 * Utility method to create RPC service from configuration and hostname, port.
	 *
	 * @param hostname The hostname/address that describes the TaskManager's data location.
	 * @param portRangeDefinition The port range to start TaskManager on.
	 * @param configuration The configuration for the TaskManager.
	 * @param actorSystemName The actor system name of the RpcService.
	 * @param actorSystemExecutorConfiguration The configuration of the executor of the actor system.
	 * @return The rpc service which is used to start and connect to the TaskManager RpcEndpoint .
	 * @throws IOException Thrown, if the actor system can not bind to the address
	 * @throws Exception Thrown is some other error occurs while creating akka actor system
	 */
	public static RpcService createRpcService(
		String hostname,
		String portRangeDefinition,
		Configuration configuration,
		String actorSystemName,
		@Nonnull BootstrapTools.ActorSystemExecutorConfiguration actorSystemExecutorConfiguration) throws Exception {

		final ActorSystem actorSystem = BootstrapTools.startActorSystem(
			configuration,
			actorSystemName,
			hostname,
			portRangeDefinition,
			LOG,
			actorSystemExecutorConfiguration);

		return instantiateAkkaRpcService(configuration, actorSystem);
	}

	@Nonnull
	private static RpcService instantiateAkkaRpcService(Configuration configuration, ActorSystem actorSystem) {
		return new AkkaRpcService(actorSystem, AkkaRpcServiceConfiguration.fromConfiguration(configuration));
	}

	// ------------------------------------------------------------------------
	//  RPC endpoint addressing
	// ------------------------------------------------------------------------

	/**
	 *
	 * @param hostname The hostname or address where the target RPC service is listening.
	 * @param port The port where the target RPC service is listening.
	 * @param endpointName The name of the RPC endpoint.
	 * @param addressResolution Whether to try address resolution of the given hostname or not.
	 *                          This allows to fail fast in case that the hostname cannot be resolved.
	 * @param config The configuration from which to deduce further settings.
	 *
	 * @return The RPC URL of the specified RPC endpoint.
	 */
	public static String getRpcUrl(
		String hostname,
		int port,
		String endpointName,
		HighAvailabilityServicesUtils.AddressResolution addressResolution,
		Configuration config) throws UnknownHostException {

		checkNotNull(config, "config is null");

		final boolean sslEnabled = config.getBoolean(AkkaOptions.SSL_ENABLED) &&
				SSLUtils.isInternalSSLEnabled(config);

		return getRpcUrl(
			hostname,
			port,
			endpointName,
			addressResolution,
			sslEnabled ? AkkaProtocol.SSL_TCP : AkkaProtocol.TCP);
	}

	/**
	 * @param hostname The hostname or address where the target RPC service is listening.
	 * @param port The port where the target RPC service is listening.
	 * @param endpointName The name of the RPC endpoint.
	 * @param addressResolution Whether to try address resolution of the given hostname or not.
	 *                          This allows to fail fast in case that the hostname cannot be resolved.
	 * @param akkaProtocol True, if security/encryption is enabled, false otherwise.
	 *
	 * @return The RPC URL of the specified RPC endpoint.
	 */
	public static String getRpcUrl(
			String hostname,
			int port,
			String endpointName,
			HighAvailabilityServicesUtils.AddressResolution addressResolution,
			AkkaProtocol akkaProtocol) throws UnknownHostException {

		checkNotNull(hostname, "hostname is null");
		checkNotNull(endpointName, "endpointName is null");
		checkArgument(port > 0 && port <= 65535, "port must be in [1, 65535]");

		final String protocolPrefix = akkaProtocol == AkkaProtocol.SSL_TCP ? AKKA_SSL_TCP : AKKA_TCP;

		if (addressResolution == AddressResolution.TRY_ADDRESS_RESOLUTION) {
			// Fail fast if the hostname cannot be resolved
			//noinspection ResultOfMethodCallIgnored
			InetAddress.getByName(hostname);
		}

		final String hostPort = NetUtils.unresolvedHostAndPortToNormalizedString(hostname, port);

		return String.format("%s://flink@%s/user/%s", protocolPrefix, hostPort, endpointName);
	}

	/**
	 * Whether to use TCP or encrypted TCP for Akka.
	 */
	public enum AkkaProtocol {
		TCP,
		SSL_TCP
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
	//  RPC service configuration
	// ------------------------------------------------------------------------

	public static long extractMaximumFramesize(Configuration configuration) {
		String maxFrameSizeStr = configuration.getString(AkkaOptions.FRAMESIZE);
		String akkaConfigStr = String.format(SIMPLE_AKKA_CONFIG_TEMPLATE, maxFrameSizeStr);
		Config akkaConfig = ConfigFactory.parseString(akkaConfigStr);
		return akkaConfig.getBytes(MAXIMUM_FRAME_SIZE_PATH);
	}

	// ------------------------------------------------------------------------

	/** This class is not meant to be instantiated. */
	private AkkaRpcServiceUtils() {}
}
