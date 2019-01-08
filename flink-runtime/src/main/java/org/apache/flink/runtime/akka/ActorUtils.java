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

import org.apache.flink.runtime.concurrent.FutureUtils;

import akka.actor.ActorRef;
import akka.actor.Kill;
import akka.pattern.Patterns;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

/**
 * Utility functions for the interaction with Akka {@link akka.actor.Actor}.
 */
public class ActorUtils {

	private static final Logger LOG = LoggerFactory.getLogger(ActorUtils.class);

	/**
	 * Shuts the given {@link akka.actor.Actor} down in a non blocking fashion. The method first tries to
	 * gracefully shut them down. If this is not successful, then the actors will be terminated by sending
	 * a {@link akka.actor.Kill} message.
	 *
	 * @param gracePeriod for the graceful shutdown
	 * @param timeUnit time unit of the grace period
	 * @param actors to shut down
	 * @return Future which is completed once all actors have been shut down gracefully or forceful
	 * kill messages have been sent to all actors. Occurring errors will be suppressed into one error.
	 */
	public static CompletableFuture<Void> nonBlockingShutDown(long gracePeriod, TimeUnit timeUnit, ActorRef... actors) {
		final Collection<CompletableFuture<Void>> terminationFutures = new ArrayList<>(actors.length);
		final FiniteDuration timeout = new FiniteDuration(gracePeriod, timeUnit);

		for (ActorRef actor : actors) {
			try {
				final Future<Boolean> booleanFuture = Patterns.gracefulStop(actor, timeout);
				final CompletableFuture<Void> terminationFuture = FutureUtils.toJava(booleanFuture)
					.<Void>thenApply(ignored -> null)
					.exceptionally((Throwable throwable) -> {
						if (throwable instanceof TimeoutException) {
							// the actor did not gracefully stop within the grace period --> Let's kill him
							actor.tell(Kill.getInstance(), ActorRef.noSender());
							return null;
						} else {
							throw new CompletionException(throwable);
						}
					});

				terminationFutures.add(terminationFuture);
			} catch (IllegalStateException ignored) {
				// this can happen if the underlying actor system has been stopped before shutting
				// the actor down
				LOG.debug("The actor {} has already been stopped because the " +
					"underlying ActorSystem has already been shut down.", actor.path());
			}
		}

		return FutureUtils.completeAll(terminationFutures);
	}

	private ActorUtils() {}
}
