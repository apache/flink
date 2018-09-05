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

package org.apache.flink.runtime.akka;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.util.Preconditions;

import akka.actor.ActorSystem;
import akka.actor.Address;
import org.slf4j.Logger;

import java.util.concurrent.TimeoutException;

import scala.concurrent.Await;
import scala.concurrent.duration.FiniteDuration;

/**
 * Default quarantine handler which logs the quarantining events, then shuts down the given
 * actor system by sending Kill to all actors and then shutting the JVM down with the given
 * exit code.
 */
public class DefaultQuarantineHandler implements QuarantineHandler {

	private final FiniteDuration timeout;
	private final int exitCode;
	private final Logger log;

	public DefaultQuarantineHandler(Time timeout, int exitCode, Logger log) {
		Preconditions.checkNotNull(timeout);
		this.timeout = new FiniteDuration(timeout.getSize(), timeout.getUnit());
		this.exitCode = exitCode;
		this.log = Preconditions.checkNotNull(log);
	}

	@Override
	public void wasQuarantinedBy(String remoteSystem, ActorSystem actorSystem) {
		Address actorSystemAddress = AkkaUtils.getAddress(actorSystem);
		log.error("The actor system {} has been quarantined by {}. Shutting the actor system " +
			"down to be able to reestablish a connection!", actorSystemAddress, remoteSystem);

		shutdownActorSystem(actorSystem);
	}

	@Override
	public void hasQuarantined(String remoteSystem, ActorSystem actorSystem) {
		Address actorSystemAddress = AkkaUtils.getAddress(actorSystem);
		log.error("The actor system {} has quarantined the remote actor system {}. Shutting " +
			"the actor system down to be able to reestablish a connection!", actorSystemAddress, remoteSystem);

		shutdownActorSystem(actorSystem);
	}

	private void shutdownActorSystem(ActorSystem actorSystem) {
		// shut the actor system down
		actorSystem.terminate();

		try {
			// give it some time to complete the shutdown
			Await.ready(actorSystem.whenTerminated(), timeout);
		} catch (InterruptedException | TimeoutException e) {
			log.error("Exception thrown when terminating the actor system", e);
		} finally {
			// now let's crash the JVM
			System.exit(exitCode);
		}
	}
}
