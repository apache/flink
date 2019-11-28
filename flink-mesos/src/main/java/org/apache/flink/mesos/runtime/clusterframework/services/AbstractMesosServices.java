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

import org.apache.flink.mesos.util.MesosArtifactServer;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;

import akka.actor.ActorSystem;

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
	public ActorSystem getLocalActorSystem() {
		return actorSystem;
	}

	@Override
	public MesosArtifactServer getArtifactServer() {
		return artifactServer;
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
