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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.jobmanager.JobManager;
import org.apache.flink.runtime.jobmanager.MemoryArchivist;
import org.apache.flink.runtime.messages.TaskManagerMessages;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.taskmanager.TaskManager;
import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.pattern.Patterns;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import scala.Option;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

/**
 * Mini cluster to run the old JobManager code without embedded high availability services. This
 * class has been implemented because the normal {@link FlinkMiniCluster} has been changed to use
 * the {@link HighAvailabilityServices}. With this change we can no longer use the
 * {@link org.apache.flink.api.java.RemoteEnvironment} to connect against the
 * {@link FlinkMiniCluster}, because the remote environment cannot retrieve the current leader
 * session id.
 */
public class StandaloneMiniCluster implements AutoCloseableAsync {

	private static final String LOCAL_HOSTNAME = "localhost";

	private final Configuration configuration;

	private final ActorSystem actorSystem;

	private final ScheduledExecutorService scheduledExecutorService;

	private final HighAvailabilityServices highAvailabilityServices;

	private final MetricRegistryImpl metricRegistry;

	private final FiniteDuration timeout;

	private final int port;

	public StandaloneMiniCluster(Configuration configuration) throws Exception {
		this.configuration = Preconditions.checkNotNull(configuration);

		timeout = AkkaUtils.getTimeout(configuration);

		actorSystem = JobManager.startActorSystem(
			configuration,
			LOCAL_HOSTNAME,
			0);

		port = configuration.getInteger(JobManagerOptions.PORT);

		scheduledExecutorService = new ScheduledThreadPoolExecutor(1);

		highAvailabilityServices = HighAvailabilityServicesUtils.createHighAvailabilityServices(
			configuration,
			Executors.directExecutor(),
			HighAvailabilityServicesUtils.AddressResolution.TRY_ADDRESS_RESOLUTION);

		metricRegistry = new MetricRegistryImpl(
			MetricRegistryConfiguration.fromConfiguration(configuration));

		metricRegistry.startQueryService(actorSystem, null);

		JobManager.startJobManagerActors(
			configuration,
			actorSystem,
			scheduledExecutorService,
			scheduledExecutorService,
			highAvailabilityServices,
			metricRegistry,
			Option.empty(),
			JobManager.class,
			MemoryArchivist.class);

		final ResourceID taskManagerResourceId = ResourceID.generate();

		ActorRef taskManager = TaskManager.startTaskManagerComponentsAndActor(
			configuration,
			taskManagerResourceId,
			actorSystem,
			highAvailabilityServices,
			metricRegistry,
			LOCAL_HOSTNAME,
			Option.<String>empty(),
			true,
			TaskManager.class);

		Future<Object> registrationFuture = Patterns.ask(
			taskManager,
			TaskManagerMessages.getNotifyWhenRegisteredAtJobManagerMessage(),
			timeout.toMillis());

		Await.ready(registrationFuture, timeout);
	}

	public String getHostname() {
		return LOCAL_HOSTNAME;
	}

	public int getPort() {
		return port;
	}

	public Configuration getConfiguration() {
		return configuration;
	}

	@Override
	public CompletableFuture<Void> closeAsync() {
		Exception exception = null;

		try {
			metricRegistry.shutdown();
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(e, exception);
		}

		actorSystem.shutdown();
		actorSystem.awaitTermination();

		try {
			highAvailabilityServices.closeAndCleanupAllData();
		} catch (Exception e) {
			exception = e;
		}

		scheduledExecutorService.shutdownNow();

		try {
			scheduledExecutorService.awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();

			exception = ExceptionUtils.firstOrSuppressed(e, exception);
		}

		if (exception != null) {
			return FutureUtils.completedExceptionally(exception);
		} else {
			return CompletableFuture.completedFuture(null);
		}
	}
}
