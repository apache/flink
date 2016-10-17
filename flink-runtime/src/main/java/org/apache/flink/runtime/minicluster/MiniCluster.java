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

package org.apache.flink.runtime.minicluster;

import akka.actor.ActorSystem;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.UnmodifiableConfiguration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.akka.AkkaRpcService;
import org.apache.flink.util.ExceptionUtils;

import javax.annotation.concurrent.GuardedBy;

import static org.apache.flink.util.ExceptionUtils.firstOrSuppressed;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;


public class MiniCluster {

	/** The lock to guard startup / shutdown / manipulation methods */
	private final Object lock = new Object();

	/** The configuration for this mini cluster */
	private final MiniClusterConfiguration config;

	@GuardedBy("lock")
	private MetricRegistry metricRegistry;

	@GuardedBy("lock")
	private RpcService commonRpcService;

	@GuardedBy("lock")
	private RpcService[] jobManagerRpcServices;

	@GuardedBy("lock")
	private RpcService[] taskManagerRpcServices;

	@GuardedBy("lock")
	private HighAvailabilityServices haServices;

	@GuardedBy("lock")
	private MiniClusterJobDispatcher jobDispatcher;

	/** Flag marking the mini cluster as started/running */
	@GuardedBy("lock")
	private boolean running;

	// ------------------------------------------------------------------------

	/**
	 * Creates a new mini cluster with the default configuration:
	 * <ul>
	 *     <li>One JobManager</li>
	 *     <li>One TaskManager</li>
	 *     <li>One task slot in the TaskManager</li>
	 *     <li>All components share the same RPC subsystem (minimizes communication overhead)</li>
	 * </ul>
	 */
	public MiniCluster() {
		this(new MiniClusterConfiguration());
	}

	/**
	 * 
	 * @param config The configuration for the mini cluster
	 */
	public MiniCluster(MiniClusterConfiguration config) {
		this.config = checkNotNull(config, "config may not be null");
	}

	/**
	 * Creates a mini cluster based on the given configuration.
	 * 
	 * @deprecated Use {@link #MiniCluster(MiniClusterConfiguration)} instead. 
	 * @see #MiniCluster(MiniClusterConfiguration)
	 */
	@Deprecated
	public MiniCluster(Configuration config) {
		this(createConfig(config, true));
	}

	/**
	 * Creates a mini cluster based on the given configuration, starting one or more
	 * RPC services, depending on the given flag.
	 *
	 * @deprecated Use {@link #MiniCluster(MiniClusterConfiguration)} instead. 
	 * @see #MiniCluster(MiniClusterConfiguration)
	 */
	@Deprecated
	public MiniCluster(Configuration config, boolean singleRpcService) {
		this(createConfig(config, singleRpcService));
	}

	// ------------------------------------------------------------------------
	//  life cycle
	// ------------------------------------------------------------------------

	/**
	 * Checks if the mini cluster was started and is running.
	 */
	public boolean isRunning() {
		return running;
	}

	/**
	 * Starts the mini cluster, based on the configured properties.
	 * 
	 * @throws Exception This method passes on any exception that occurs during the startup of
	 *                   the mini cluster.
	 */
	public void start() throws Exception {
		synchronized (lock) {
			checkState(!running, "FlinkMiniCluster is already running");

			final Configuration configuration = new UnmodifiableConfiguration(config.getConfiguration());
			final Time rpcTimeout = config.getRpcTimeout();
			final int numJobManagers = config.getNumJobManagers();
			final int numTaskManagers = config.getNumTaskManagers();
			final boolean singleRpc = config.getUseSingleRpcSystem();

			try {
				metricRegistry = createMetricRegistry(configuration);

				RpcService[] jobManagerRpcServices = new RpcService[numJobManagers];
				RpcService[] taskManagerRpcServices = new RpcService[numTaskManagers];

				// bring up all the RPC services
				if (singleRpc) {
					// one common RPC for all
					commonRpcService = createRpcService(configuration, rpcTimeout, false, null);

					// set that same RPC service for all JobManagers and TaskManagers
					for (int i = 0; i < numJobManagers; i++) {
						jobManagerRpcServices[i] = commonRpcService;
					}
					for (int i = 0; i < numTaskManagers; i++) {
						taskManagerRpcServices[i] = commonRpcService;
					}
				}
				else {
					// start a new service per component, possibly with custom bind addresses
					final String jobManagerBindAddress = config.getJobManagerBindAddress();
					final String taskManagerBindAddress = config.getTaskManagerBindAddress();

					for (int i = 0; i < numJobManagers; i++) {
						jobManagerRpcServices[i] = createRpcService(
								configuration, rpcTimeout, true, jobManagerBindAddress);
					}

					for (int i = 0; i < numTaskManagers; i++) {
						taskManagerRpcServices[i] = createRpcService(
								configuration, rpcTimeout, true, taskManagerBindAddress);
					}

					this.jobManagerRpcServices = jobManagerRpcServices;
					this.taskManagerRpcServices = taskManagerRpcServices;
				}

				// create the high-availability services
				haServices = HighAvailabilityServicesUtils.createAvailableOrEmbeddedServices(configuration);

				// bring up the dispatcher that launches JobManagers when jobs submitted
				jobDispatcher = new MiniClusterJobDispatcher(
						configuration, haServices, metricRegistry, numJobManagers, jobManagerRpcServices);
			}
			catch (Exception e) {
				// cleanup everything
				try {
					shutdownInternally();
				} catch (Exception ee) {
					e.addSuppressed(ee);
				}
				throw e;
			}

			// now officially mark this as running
			running = true;
		}
	}

	/**
	 * Shuts down the mini cluster, failing all currently executing jobs.
	 * The mini cluster can be started again by calling the {@link #start()} method again.
	 * 
	 * <p>This method shuts down all started services and components,
	 * even if an exception occurs in the process of shutting down some component. 
	 * 
	 * @throws Exception Thrown, if the shutdown did not complete cleanly.
	 */
	public void shutdown() throws Exception {
		synchronized (lock) {
			if (running) {
				try {
					shutdownInternally();
				} finally {
					running = false;
				}
			}
		}
	}

	@GuardedBy("lock")
	private void shutdownInternally() throws Exception {
		// this should always be called under the lock
		assert Thread.holdsLock(lock);

		// collect the first exception, but continue and add all successive
		// exceptions as suppressed
		Throwable exception = null;

		// cancel all jobs and shut down the job dispatcher
		if (jobDispatcher != null) {
			try {
				jobDispatcher.shutdown();
			} catch (Exception e) {
				exception = firstOrSuppressed(e, exception);
			}
			jobDispatcher = null;
		}

		// shut down high-availability services
		if (haServices != null) {
			try {
				haServices.shutdown();
			} catch (Exception e) {
				exception = firstOrSuppressed(e, exception);
			}
			haServices = null;
		}

		// shut down the RpcServices
		if (commonRpcService != null) {
			exception = shutDownRpc(commonRpcService, exception);
			commonRpcService = null;
		}
		if (jobManagerRpcServices != null) {
			for (RpcService service : jobManagerRpcServices) {
				exception = shutDownRpc(service, exception);
			}
			jobManagerRpcServices = null;
		}
		if (taskManagerRpcServices != null) {
			for (RpcService service : taskManagerRpcServices) {
				exception = shutDownRpc(service, exception);
			}
			taskManagerRpcServices = null;
		}

		// metrics shutdown
		if (metricRegistry != null) {
			metricRegistry.shutdown();
			metricRegistry = null;
		}

		// if anything went wrong, throw the first error with all the additional suppressed exceptions
		if (exception != null) {
			ExceptionUtils.rethrowException(exception, "Error while shutting down mini cluster");
		}
	}

	// ------------------------------------------------------------------------
	//  running jobs
	// ------------------------------------------------------------------------

	/**
	 * This method executes a job in detached mode. The method returns immediately after the job
	 * has been added to the
	 *
	 * @param job  The Flink job to execute
	 *
	 * @throws JobExecutionException Thrown if anything went amiss during initial job launch,
	 *         or if the job terminally failed.
	 */
	public void runDetached(JobGraph job) throws JobExecutionException {
		checkNotNull(job, "job is null");

		synchronized (lock) {
			checkState(running, "mini cluster is not running");
			jobDispatcher.runDetached(job);
		}
	}

	/**
	 * This method runs a job in blocking mode. The method returns only after the job
	 * completed successfully, or after it failed terminally.
	 *
	 * @param job  The Flink job to execute 
	 * @return The result of the job execution
	 *
	 * @throws JobExecutionException Thrown if anything went amiss during initial job launch,
	 *         or if the job terminally failed.
	 */
	public JobExecutionResult runJobBlocking(JobGraph job) throws JobExecutionException, InterruptedException {
		checkNotNull(job, "job is null");

		MiniClusterJobDispatcher dispatcher;
		synchronized (lock) {
			checkState(running, "mini cluster is not running");
			dispatcher = this.jobDispatcher;
		}

		return dispatcher.runJobBlocking(job);
	}

	// ------------------------------------------------------------------------
	//  factories - can be overridden by subclasses to alter behavior
	// ------------------------------------------------------------------------

	/**
	 * Factory method to create the metric registry for the mini cluster
	 * 
	 * @param config The configuration of the mini cluster
	 */
	protected MetricRegistry createMetricRegistry(Configuration config) {
		return new MetricRegistry(MetricRegistryConfiguration.fromConfiguration(config));
	}

	/**
	 * Factory method to instantiate the RPC service.
	 * 
	 * @param configuration
	 *            The configuration of the mini cluster
	 * @param askTimeout
	 *            The default RPC timeout for asynchronous "ask" requests.
	 * @param remoteEnabled
	 *            True, if the RPC service should be reachable from other (remote) RPC services.
	 * @param bindAddress
	 *            The address to bind the RPC service to. Only relevant when "remoteEnabled" is true.
	 * 
	 * @return The instantiated RPC service
	 */
	protected RpcService createRpcService(
			Configuration configuration,
			Time askTimeout,
			boolean remoteEnabled,
			String bindAddress) {

		ActorSystem actorSystem;
		if (remoteEnabled) {
			actorSystem = AkkaUtils.createActorSystem(configuration, bindAddress, 0);
		} else {
			actorSystem = AkkaUtils.createLocalActorSystem(configuration);
		}

		return new AkkaRpcService(actorSystem, askTimeout);
	}

	// ------------------------------------------------------------------------
	//  miscellaneous utilities
	// ------------------------------------------------------------------------

	private static Throwable shutDownRpc(RpcService rpcService, Throwable priorException) {
		try {
			if (rpcService != null) {
				rpcService.stopService();
			}
			return priorException;
		}
		catch (Throwable t) {
			return firstOrSuppressed(t, priorException);
		}
	}

	private static MiniClusterConfiguration createConfig(Configuration cfg, boolean singleActorSystem) {
		MiniClusterConfiguration config = cfg == null ?
				new MiniClusterConfiguration() :
				new MiniClusterConfiguration(cfg);

		if (!singleActorSystem) {
			config.setUseRpcServicePerComponent();
		}

		return config;
	}
}
