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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.rpc.akka.AkkaRpcService;
import org.apache.flink.util.NetUtils;
import org.jboss.netty.channel.ChannelException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class RpcServiceUtils {
	private static final Logger LOG = LoggerFactory.getLogger(RpcServiceUtils.class);

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
			Config akkaConfig = AkkaUtils.getAkkaConfig(configuration, hostname, port);

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
}
