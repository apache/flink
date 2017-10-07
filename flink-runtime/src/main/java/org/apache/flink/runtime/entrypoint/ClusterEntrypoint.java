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

package org.apache.flink.runtime.entrypoint;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.akka.AkkaRpcService;
import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.security.SecurityContext;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import akka.actor.ActorSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import scala.concurrent.duration.FiniteDuration;

/**
 * Base class for the Flink cluster entry points.
 *
 * <p>Specialization of this class can be used for the session mode and the per-job mode
 */
public abstract class ClusterEntrypoint implements FatalErrorHandler {

	protected static final Logger LOG = LoggerFactory.getLogger(ClusterEntrypoint.class);

	protected static final int SUCCESS_RETURN_CODE = 0;
	protected static final int STARTUP_FAILURE_RETURN_CODE = 1;
	protected static final int RUNTIME_FAILURE_RETURN_CODE = 2;

	/** The lock to guard startup / shutdown / manipulation methods. */
	private final Object lock = new Object();

	private final Configuration configuration;

	private final CompletableFuture<Boolean> terminationFuture;

	@GuardedBy("lock")
	private MetricRegistry metricRegistry = null;

	@GuardedBy("lock")
	private HighAvailabilityServices haServices = null;

	@GuardedBy("lock")
	private BlobServer blobServer = null;

	@GuardedBy("lock")
	private HeartbeatServices heartbeatServices = null;

	@GuardedBy("lock")
	private RpcService commonRpcService = null;

	protected ClusterEntrypoint(Configuration configuration) {
		this.configuration = Preconditions.checkNotNull(configuration);
		this.terminationFuture = new CompletableFuture<>();
	}

	public CompletableFuture<Boolean> getTerminationFuture() {
		return terminationFuture;
	}

	protected void startCluster() {
		LOG.info("Starting {}.", getClass().getSimpleName());

		try {
			configureFileSystems(configuration);

			SecurityContext securityContext = installSecurityContext(configuration);

			securityContext.runSecured(new Callable<Void>() {
				@Override
				public Void call() throws Exception {
					runCluster(configuration);

					return null;
				}
			});
		} catch (Throwable t) {
			LOG.error("Cluster initialization failed.", t);

			try {
				shutDown(false);
			} catch (Throwable st) {
				LOG.error("Could not properly shut down cluster entrypoint.", st);
			}

			System.exit(STARTUP_FAILURE_RETURN_CODE);
		}
	}

	protected void configureFileSystems(Configuration configuration) throws Exception {
		LOG.info("Install default filesystem.");

		try {
			FileSystem.initialize(configuration);
		} catch (IOException e) {
			throw new IOException("Error while setting the default " +
				"filesystem scheme from configuration.", e);
		}
	}

	protected SecurityContext installSecurityContext(Configuration configuration) throws Exception {
		LOG.info("Install security context.");

		SecurityUtils.install(new SecurityConfiguration(configuration));

		return SecurityUtils.getInstalledContext();
	}

	protected void runCluster(Configuration configuration) throws Exception {
		synchronized (lock) {
			initializeServices(configuration);

			// write host information into configuration
			configuration.setString(JobManagerOptions.ADDRESS, commonRpcService.getAddress());
			configuration.setInteger(JobManagerOptions.PORT, commonRpcService.getPort());

			startClusterComponents(
				configuration,
				commonRpcService,
				haServices,
				blobServer,
				heartbeatServices,
				metricRegistry);
		}
	}

	protected void initializeServices(Configuration configuration) throws Exception {
		assert(Thread.holdsLock(lock));

		LOG.info("Initializing cluster services.");

		final String bindAddress = configuration.getString(JobManagerOptions.ADDRESS);
		// TODO: Add support for port ranges
		final String portRange = String.valueOf(configuration.getInteger(JobManagerOptions.PORT));

		commonRpcService = createRpcService(configuration, bindAddress, portRange);
		haServices = createHaServices(configuration, commonRpcService.getExecutor());
		blobServer = new BlobServer(configuration, haServices.createBlobStore());
		blobServer.start();
		heartbeatServices = createHeartbeatServices(configuration);
		metricRegistry = createMetricRegistry(configuration);
	}

	protected RpcService createRpcService(
			Configuration configuration,
			String bindAddress,
			String portRange) throws Exception {
		ActorSystem actorSystem = BootstrapTools.startActorSystem(configuration, bindAddress, portRange, LOG);
		FiniteDuration duration = AkkaUtils.getTimeout(configuration);
		return new AkkaRpcService(actorSystem, Time.of(duration.length(), duration.unit()));
	}

	protected HighAvailabilityServices createHaServices(
		Configuration configuration,
		Executor executor) throws Exception {
		return HighAvailabilityServicesUtils.createHighAvailabilityServices(
			configuration,
			executor,
			HighAvailabilityServicesUtils.AddressResolution.NO_ADDRESS_RESOLUTION);
	}

	protected HeartbeatServices createHeartbeatServices(Configuration configuration) {
		return HeartbeatServices.fromConfiguration(configuration);
	}

	protected MetricRegistry createMetricRegistry(Configuration configuration) {
		return new MetricRegistry(MetricRegistryConfiguration.fromConfiguration(configuration));
	}

	protected void shutDown(boolean cleanupHaData) throws FlinkException {
		LOG.info("Stopping {}.", getClass().getSimpleName());

		Throwable exception = null;

		synchronized (lock) {

			try {
				stopClusterComponents(cleanupHaData);
			} catch (Throwable t) {
				exception = ExceptionUtils.firstOrSuppressed(t, exception);
			}

			if (metricRegistry != null) {
				try {
					metricRegistry.shutdown();
				} catch (Throwable t) {
					exception = t;
				}
			}

			if (blobServer != null) {
				try {
					blobServer.close();
				} catch (Throwable t) {
					exception = ExceptionUtils.firstOrSuppressed(t, exception);
				}
			}

			if (haServices != null) {
				try {
					if (cleanupHaData) {
						haServices.closeAndCleanupAllData();
					} else {
						haServices.close();
					}
				} catch (Throwable t) {
					exception = ExceptionUtils.firstOrSuppressed(t, exception);
				}
			}

			if (commonRpcService != null) {
				try {
					commonRpcService.stopService();
				} catch (Throwable t) {
					exception = ExceptionUtils.firstOrSuppressed(t, exception);
				}
			}

			terminationFuture.complete(true);
		}

		if (exception != null) {
			throw new FlinkException("Could not properly shut down the cluster services.", exception);
		}
	}

	@Override
	public void onFatalError(Throwable exception) {
		LOG.error("Fatal error occurred in the cluster entrypoint.", exception);

		System.exit(RUNTIME_FAILURE_RETURN_CODE);
	}

	protected abstract void startClusterComponents(
		Configuration configuration,
		RpcService rpcService,
		HighAvailabilityServices highAvailabilityServices,
		BlobServer blobServer,
		HeartbeatServices heartbeatServices,
		MetricRegistry metricRegistry) throws Exception;

	protected void stopClusterComponents(boolean cleanupHaData) throws Exception {
	}

	protected static ClusterConfiguration parseArguments(String[] args) {
		ParameterTool parameterTool = ParameterTool.fromArgs(args);

		final String configDir = parameterTool.get("configDir", "");

		return new ClusterConfiguration(configDir);
	}

	protected static Configuration loadConfiguration(ClusterConfiguration clusterConfiguration) {
		return GlobalConfiguration.loadConfiguration(clusterConfiguration.getConfigDir());
	}
}
