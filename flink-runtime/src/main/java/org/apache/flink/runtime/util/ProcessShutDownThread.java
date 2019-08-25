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

package org.apache.flink.runtime.util;

import akka.actor.ActorSystem;
import org.slf4j.Logger;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

import java.util.concurrent.TimeoutException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * JVM shut down thread awaiting actor system shut down for a certain amount
 * of time before exiting the JVM.
 *
 * <p>On some Linux distributions, YARN is not able to stop containers, because
 * the <code>kill</code> command has different arguments. For example when
 * running Flink on GCE ("Debian GNU/Linux 7.9 (wheezy)"), YARN containers will
 * not properly shut down when we don't call <code>System.exit()</code>.
 */
public class ProcessShutDownThread extends Thread {

	/** Log of the corresponding YARN process. */
	private final Logger log;

	/** Actor system to await termination of. */
	private final ActorSystem actorSystem;

	/** Actor system termination timeout before shutting down the JVM. */
	private final Duration terminationTimeout;

	/**
	 * Creates a shut down thread.
	 *
	 * @param log                Log of the corresponding YARN process.
	 * @param actorSystem        Actor system to await termination of.
	 * @param terminationTimeout Actor system termination timeout before
	 *                           shutting down the JVM.
	 */
	public ProcessShutDownThread(
			Logger log,
			ActorSystem actorSystem,
			Duration terminationTimeout) {

		this.log = checkNotNull(log, "Logger");
		this.actorSystem = checkNotNull(actorSystem, "Actor system");
		this.terminationTimeout = checkNotNull(terminationTimeout, "Termination timeout");
	}

	@Override
	public void run() {
		try {
			Await.ready(actorSystem.whenTerminated(), terminationTimeout);
		} catch (Exception e) {
			if (e instanceof TimeoutException) {
				log.error("Actor system shut down timed out.", e);
			} else {
				log.error("Failure during actor system shut down.", e);
			}
		} finally {
			log.info("Shutdown completed. Stopping JVM.");
			System.exit(0);
		}
	}
}
