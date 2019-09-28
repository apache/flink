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

import akka.actor.ActorKilledException;
import akka.actor.OneForOneStrategy;
import akka.actor.SupervisorStrategy;
import akka.actor.SupervisorStrategyConfigurator;
import akka.japi.pf.PFBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stopping supervisor strategy which logs {@link ActorKilledException} only on debug log level.
 */
public class StoppingSupervisorWithoutLoggingActorKilledExceptionStrategy implements SupervisorStrategyConfigurator {

	private static final Logger LOG = LoggerFactory.getLogger(StoppingSupervisorWithoutLoggingActorKilledExceptionStrategy.class);

	@Override
	public SupervisorStrategy create() {
		return new OneForOneStrategy(
			false,
			new PFBuilder<Throwable, SupervisorStrategy.Directive>()
				.match(
					Exception.class,
					(Exception e) -> {
						if (e instanceof ActorKilledException) {
							LOG.debug("Actor was killed. Stopping it now.", e);
						} else {
							LOG.error("Actor failed with exception. Stopping it now.", e);
						}
						return SupervisorStrategy.Stop$.MODULE$;
					})
				.build());
	}
}
