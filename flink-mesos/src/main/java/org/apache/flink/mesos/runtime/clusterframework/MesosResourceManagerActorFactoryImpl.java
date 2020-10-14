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

package org.apache.flink.mesos.runtime.clusterframework;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.mesos.scheduler.ConnectionMonitor;
import org.apache.flink.mesos.scheduler.LaunchCoordinator;
import org.apache.flink.mesos.scheduler.ReconciliationCoordinator;
import org.apache.flink.mesos.scheduler.TaskMonitor;
import org.apache.flink.mesos.scheduler.TaskSchedulerBuilder;
import org.apache.flink.mesos.scheduler.Tasks;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.util.Preconditions;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.pattern.Patterns;
import org.apache.mesos.SchedulerDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;

import scala.concurrent.duration.FiniteDuration;

/**
 * Implementation of {@link MesosResourceManagerActorFactory}.
 */
public class MesosResourceManagerActorFactoryImpl implements MesosResourceManagerActorFactory {

	private static final Logger LOG = LoggerFactory.getLogger(MesosResourceManagerActorFactoryImpl.class);

	private final ActorSystem actorSystem;

	public MesosResourceManagerActorFactoryImpl(ActorSystem actorSystem) {
		this.actorSystem = Preconditions.checkNotNull(actorSystem);
	}

	@Override
	public ActorRef createSelfActorForMesosResourceManagerDriver(MesosResourceManagerDriver self) {
		return actorSystem.actorOf(
				Props.create(MesosResourceManagerDriver.AkkaAdapter.class, self),
				"MesosResourceManagerDriver");
	}

	@Override
	public ActorRef createConnectionMonitor(Configuration flinkConfig) {
		return actorSystem.actorOf(
				ConnectionMonitor.createActorProps(ConnectionMonitor.class, flinkConfig),
				"connectionMonitor");
	}

	@Override
	public ActorRef createTaskMonitor(
			Configuration flinkConfig, ActorRef resourceManagerActor, SchedulerDriver schedulerDriver) {
		return actorSystem.actorOf(
				Tasks.createActorProps(Tasks.class, resourceManagerActor, flinkConfig, schedulerDriver, TaskMonitor.class),
				"tasks");
	}

	@Override
	public ActorRef createLaunchCoordinator(
			Configuration flinkConfig, ActorRef resourceManagerActor, SchedulerDriver schedulerDriver, TaskSchedulerBuilder optimizer) {
		return actorSystem.actorOf(
				LaunchCoordinator.createActorProps(LaunchCoordinator.class, resourceManagerActor, flinkConfig, schedulerDriver, optimizer),
				"launchCoordinator");
	}

	@Override
	public ActorRef createReconciliationCoordinator(Configuration flinkConfig, SchedulerDriver schedulerDriver) {
		return actorSystem.actorOf(
				ReconciliationCoordinator.createActorProps(ReconciliationCoordinator.class, flinkConfig, schedulerDriver),
				"reconciliationCoordinator");
	}

	@Override
	public CompletableFuture<Boolean> stopActor(@Nullable final ActorRef actorRef, FiniteDuration timeout) {
		if (actorRef == null) {
			return CompletableFuture.completedFuture(true);
		}

		return FutureUtils.toJava(Patterns.gracefulStop(actorRef, timeout))
				.exceptionally((Throwable throwable) -> {
					// The actor did not stop gracefully in time, try to directly stop it
					actorSystem.stop(actorRef);

					LOG.warn("Could not stop actor {} gracefully.", actorRef.path(), throwable);

					return false;
				});
	}
}
