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

package org.apache.flink.runtime.process;

import akka.actor.ActorRef;
import akka.actor.Terminated;
import akka.actor.UntypedActor;
import org.slf4j.Logger;

/**
 * Utility actors that monitors other actors and kills the JVM upon
 * actor termination.
 */
public class ProcessReaper extends UntypedActor {

	private final Logger log;
	private final int exitCode;

	public ProcessReaper(ActorRef watchTarget, Logger log, int exitCode) {
		if (watchTarget == null || watchTarget.equals(ActorRef.noSender())) {
			throw new IllegalArgumentException("Target may not be null or 'noSender'");
		}
		this.log = log;
		this.exitCode = exitCode;

		getContext().watch(watchTarget);
	}

	@Override
	public void onReceive(Object message) {
		if (message instanceof Terminated) {
			try {
				Terminated term = (Terminated) message;
				String name = term.actor().path().toSerializationFormat();
				if (log != null) {
					log.error("Actor " + name + " terminated, stopping process...");

					// give the log some time to reach disk
					try {
						Thread.sleep(100);
					}
					catch (InterruptedException e) {
						// not really a problem if we don't sleep...
					}
				}
			}
			finally {
				System.exit(exitCode);
			}
		}
	}
}
