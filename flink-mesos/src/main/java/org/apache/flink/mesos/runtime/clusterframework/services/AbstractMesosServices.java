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

package org.apache.flink.mesos.runtime.clusterframework.services;

import org.apache.flink.mesos.runtime.clusterframework.MesosResourceManagerActorFactory;
import org.apache.flink.mesos.runtime.clusterframework.MesosResourceManagerActorFactoryImpl;
import org.apache.flink.mesos.util.MesosArtifactServer;
import org.apache.flink.mesos.util.MesosConfiguration;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;

import akka.actor.ActorSystem;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An abstract implementation of {@link MesosServices}.
 */
public abstract class AbstractMesosServices implements MesosServices {

	private final ActorSystem actorSystem;

	private final MesosArtifactServer artifactServer;

	protected AbstractMesosServices(ActorSystem actorSystem, MesosArtifactServer artifactServer) {
		this.actorSystem = checkNotNull(actorSystem);
		this.artifactServer = checkNotNull(artifactServer);
	}

	@Override
	public MesosResourceManagerActorFactory createMesosResourceManagerActorFactory() {
		return new MesosResourceManagerActorFactoryImpl(actorSystem);
	}

	@Override
	public MesosArtifactServer getArtifactServer() {
		return artifactServer;
	}

	@Override
	public SchedulerDriver createMesosSchedulerDriver(
			MesosConfiguration mesosConfig, Scheduler scheduler, boolean implicitAcknowledgements) {
		if (mesosConfig.credential().isDefined()) {
			return new MesosSchedulerDriver(
				scheduler,
				mesosConfig.frameworkInfo().build(),
				mesosConfig.masterUrl(),
				implicitAcknowledgements,
				mesosConfig.credential().get().build());
		}
		else {
			return new MesosSchedulerDriver(
				scheduler,
				mesosConfig.frameworkInfo().build(),
				mesosConfig.masterUrl(),
				implicitAcknowledgements);
		}
	}

	@Override
	public void close(boolean cleanup) throws Exception {
		Throwable exception = null;

		try {
			actorSystem.terminate();
		} catch (Throwable t) {
			exception = ExceptionUtils.firstOrSuppressed(t, exception);
		}

		try {
			artifactServer.stop();
		} catch (Throwable t) {
			exception = ExceptionUtils.firstOrSuppressed(t, exception);
		}

		if (exception != null) {
			throw new FlinkException("Could not properly shut down the Mesos services.", exception);
		}
	}
}
