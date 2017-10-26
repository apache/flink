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

package org.apache.flink.runtime.query;

import org.apache.flink.runtime.query.netty.KvStateRequestStats;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;

/**
 * Utility class to initialize entities used in queryable state.
 */
public final class QueryableStateUtils {

	private static final Logger LOG = LoggerFactory.getLogger(QueryableStateUtils.class);

	/**
	 * Initializes the {@link KvStateServer server} responsible for sending the
	 * requested internal state to the Queryable State Client.
	 *
	 * @param address the address to bind to.
	 * @param port the port to listen to.
	 * @param eventLoopThreads the number of threads to be used to process incoming requests.
	 * @param queryThreads the number of threads to be used to send the actual state.
	 * @param kvStateRegistry the registry with the queryable state.
	 * @param stats statistics to be gathered about the incoming requests.
	 * @return the {@link KvStateServer state server}.
	 */
	public static KvStateServer createKvStateServer(
			final InetAddress address,
			final int port,
			final int eventLoopThreads,
			final int queryThreads,
			final KvStateRegistry kvStateRegistry,
			final KvStateRequestStats stats) {

		Preconditions.checkNotNull(address, "address");
		Preconditions.checkNotNull(kvStateRegistry, "registry");
		Preconditions.checkNotNull(stats, "stats");

		Preconditions.checkArgument(eventLoopThreads >= 1);
		Preconditions.checkArgument(queryThreads >= 1);

		try {
			String classname = "org.apache.flink.queryablestate.server.KvStateServerImpl";
			Class<? extends KvStateServer> clazz = Class.forName(classname).asSubclass(KvStateServer.class);
			Constructor<? extends KvStateServer> constructor = clazz.getConstructor(
					InetAddress.class,
					Integer.class,
					Integer.class,
					Integer.class,
					KvStateRegistry.class,
					KvStateRequestStats.class);
			return constructor.newInstance(address, port, eventLoopThreads, queryThreads, kvStateRegistry, stats);
		} catch (ClassNotFoundException e) {
			LOG.info("Could not load Queryable State Server. " +
					"Probable reason: flink-queryable-state is not in the classpath");
			LOG.debug("Caught exception", e);
			return null;
		} catch (InvocationTargetException e) {
			LOG.error("Queryable State Server could not be created", e.getTargetException());
			return null;
		} catch (Throwable t) {
			LOG.error("Failed to instantiate the Queryable State Server.", t);
			return null;
		}
	}
}
