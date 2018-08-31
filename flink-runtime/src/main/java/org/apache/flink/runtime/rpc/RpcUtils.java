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

import org.apache.flink.api.common.time.Time;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Utility functions for Flink's RPC implementation.
 */
public class RpcUtils {

	/**
	 * <b>HACK:</b> Set to 21474835 seconds, Akka's maximum delay (Akka 2.4.20). The value cannot be
	 * higher or an {@link IllegalArgumentException} will be thrown during an RPC. Check the private
	 * method {@code checkMaxDelay()} in {@link akka.actor.LightArrayRevolverScheduler}.
	 */
	public static final Time INF_TIMEOUT = Time.seconds(21474835);

	/**
	 * Extracts all {@link RpcGateway} interfaces implemented by the given clazz.
	 *
	 * @param clazz from which to extract the implemented RpcGateway interfaces
	 * @return A set of all implemented RpcGateway interfaces
	 */
	public static Set<Class<? extends RpcGateway>> extractImplementedRpcGateways(Class<?> clazz) {
		HashSet<Class<? extends RpcGateway>> interfaces = new HashSet<>();

		while (clazz != null) {
			for (Class<?> interfaze : clazz.getInterfaces()) {
				if (RpcGateway.class.isAssignableFrom(interfaze)) {
					interfaces.add((Class<? extends RpcGateway>) interfaze);
				}
			}

			clazz = clazz.getSuperclass();
		}

		return interfaces;
	}

	/**
	 * Shuts the given {@link RpcEndpoint} down and awaits its termination.
	 *
	 * @param rpcEndpoint to terminate
	 * @param timeout for this operation
	 * @throws ExecutionException if a problem occurred
	 * @throws InterruptedException if the operation has been interrupted
	 * @throws TimeoutException if a timeout occurred
	 */
	public static void terminateRpcEndpoint(RpcEndpoint rpcEndpoint, Time timeout) throws ExecutionException, InterruptedException, TimeoutException {
		rpcEndpoint.shutDown();
		rpcEndpoint.getTerminationFuture().get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);
	}

	/**
	 * Shuts the given rpc service down and waits for its termination.
	 *
	 * @param rpcService to shut down
	 * @param timeout for this operation
	 * @throws InterruptedException if the operation has been interrupted
	 * @throws ExecutionException if a problem occurred
	 * @throws TimeoutException if a timeout occurred
	 */
	public static void terminateRpcService(RpcService rpcService, Time timeout) throws InterruptedException, ExecutionException, TimeoutException {
		rpcService.stopService().get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);
	}

	// We don't want this class to be instantiable
	private RpcUtils() {}
}
