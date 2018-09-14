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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.mesos.runtime.clusterframework.store.MesosWorkerStore;
import org.apache.flink.mesos.util.MesosArtifactServer;

import akka.actor.ActorSystem;

import java.util.concurrent.Executor;

/**
 * Service factory interface for Mesos.
 */
public interface MesosServices {

	/**
	 * Creates a {@link MesosWorkerStore} which is used to persist mesos worker in high availability
	 * mode.
	 *
	 * @param configuration to be used
	 * @param executor to run asynchronous tasks
	 * @return a mesos worker store
	 * @throws Exception if the mesos worker store could not be created
	 */
	MesosWorkerStore createMesosWorkerStore(
		Configuration configuration,
		Executor executor) throws Exception;

	/**
	 * Gets a local {@link ActorSystem} which is used for child actors within
	 * {@link org.apache.flink.mesos.runtime.clusterframework.MesosResourceManager}.
	 *
	 * @return a reference to an actor system.
	 */
	ActorSystem getLocalActorSystem();

	/**
	 * Gets the artifact server with which to serve essential resources to task managers.
	 * @return a reference to an artifact server.
	 */
	MesosArtifactServer getArtifactServer();

	/**
	 * Closes all state maintained by the mesos services implementation.
	 *
	 * @param cleanup is true if a cleanup shall be performed
	 * @throws Exception if the closing operation failed
	 */
	void close(boolean cleanup) throws Exception;
}
