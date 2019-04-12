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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.api.common.JobID;
import org.apache.flink.queryablestate.network.stats.DisabledKvStateRequestStats;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.query.KvStateClientProxy;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.query.KvStateServer;
import org.apache.flink.runtime.query.QueryableStateUtils;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * KvState related components of each {@link TaskExecutor} instance. This service can
 * create the kvState registration for a single task.
 */
public class KvStateService {
	private static final Logger LOG = LoggerFactory.getLogger(KvStateService.class);

	private final Object lock = new Object();

	/** Registry for {@link InternalKvState} instances. */
	private final KvStateRegistry kvStateRegistry;

	/** Server for {@link InternalKvState} requests. */
	private KvStateServer kvStateServer;

	/** Proxy for the queryable state client. */
	private KvStateClientProxy kvStateClientProxy;

	private boolean isShutdown;

	public KvStateService(KvStateRegistry kvStateRegistry, KvStateServer kvStateServer, KvStateClientProxy kvStateClientProxy) {
		this.kvStateRegistry = Preconditions.checkNotNull(kvStateRegistry);
		this.kvStateServer = kvStateServer;
		this.kvStateClientProxy = kvStateClientProxy;
	}

	// --------------------------------------------------------------------------------------------
	//  Getter/Setter
	// --------------------------------------------------------------------------------------------

	public KvStateRegistry getKvStateRegistry() {
		return kvStateRegistry;
	}

	public KvStateServer getKvStateServer() {
		return kvStateServer;
	}

	public KvStateClientProxy getKvStateClientProxy() {
		return kvStateClientProxy;
	}

	public TaskKvStateRegistry createKvStateTaskRegistry(JobID jobId, JobVertexID jobVertexId) {
		return kvStateRegistry.createTaskRegistry(jobId, jobVertexId);
	}

	// --------------------------------------------------------------------------------------------
	//  Start and shut down methods
	// --------------------------------------------------------------------------------------------

	public void start() {
		synchronized (lock) {
			Preconditions.checkState(!isShutdown, "The KvStateService has already been shut down.");

			LOG.info("Starting the kvState service and its components.");

			if (kvStateServer != null) {
				try {
					kvStateServer.start();
				} catch (Throwable ie) {
					kvStateServer.shutdown();
					kvStateServer = null;
					LOG.error("Failed to start the Queryable State Data Server.", ie);
				}
			}

			if (kvStateClientProxy != null) {
				try {
					kvStateClientProxy.start();
				} catch (Throwable ie) {
					kvStateClientProxy.shutdown();
					kvStateClientProxy = null;
					LOG.error("Failed to start the Queryable State Client Proxy.", ie);
				}
			}
		}
	}

	public void shutdown() {
		synchronized (lock) {
			if (isShutdown) {
				return;
			}

			LOG.info("Shutting down the kvState service and its components.");

			if (kvStateClientProxy != null) {
				try {
					LOG.debug("Shutting down Queryable State Client Proxy.");
					kvStateClientProxy.shutdown();
				} catch (Throwable t) {
					LOG.warn("Cannot shut down Queryable State Client Proxy.", t);
				}
			}

			if (kvStateServer != null) {
				try {
					LOG.debug("Shutting down Queryable State Data Server.");
					kvStateServer.shutdown();
				} catch (Throwable t) {
					LOG.warn("Cannot shut down Queryable State Data Server.", t);
				}
			}

			isShutdown = true;
		}
	}

	public boolean isShutdown() {
		synchronized (lock) {
			return isShutdown;
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Static factory methods for kvState service
	// --------------------------------------------------------------------------------------------

	/**
	 * Creates and returns the KvState service.
	 *
	 * @param taskManagerServicesConfiguration task manager configuration
	 * @return service for kvState related components
	 */
	public static KvStateService fromConfiguration(TaskManagerServicesConfiguration taskManagerServicesConfiguration) {
		KvStateRegistry kvStateRegistry = new KvStateRegistry();

		QueryableStateConfiguration qsConfig = taskManagerServicesConfiguration.getQueryableStateConfig();

		KvStateClientProxy kvClientProxy = null;
		KvStateServer kvStateServer = null;

		if (qsConfig != null) {
			int numProxyServerNetworkThreads = qsConfig.numProxyServerThreads() == 0 ?
				taskManagerServicesConfiguration.getNumberOfSlots() : qsConfig.numProxyServerThreads();
			int numProxyServerQueryThreads = qsConfig.numProxyQueryThreads() == 0 ?
				taskManagerServicesConfiguration.getNumberOfSlots() : qsConfig.numProxyQueryThreads();
			kvClientProxy = QueryableStateUtils.createKvStateClientProxy(
				taskManagerServicesConfiguration.getTaskManagerAddress(),
				qsConfig.getProxyPortRange(),
				numProxyServerNetworkThreads,
				numProxyServerQueryThreads,
				new DisabledKvStateRequestStats());

			int numStateServerNetworkThreads = qsConfig.numStateServerThreads() == 0 ?
				taskManagerServicesConfiguration.getNumberOfSlots() : qsConfig.numStateServerThreads();
			int numStateServerQueryThreads = qsConfig.numStateQueryThreads() == 0 ?
				taskManagerServicesConfiguration.getNumberOfSlots() : qsConfig.numStateQueryThreads();
			kvStateServer = QueryableStateUtils.createKvStateServer(
				taskManagerServicesConfiguration.getTaskManagerAddress(),
				qsConfig.getStateServerPortRange(),
				numStateServerNetworkThreads,
				numStateServerQueryThreads,
				kvStateRegistry,
				new DisabledKvStateRequestStats());
		}

		return new KvStateService(kvStateRegistry, kvStateServer, kvClientProxy);
	}
}
