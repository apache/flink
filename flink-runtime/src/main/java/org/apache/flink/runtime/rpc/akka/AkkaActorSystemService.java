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

package org.apache.flink.runtime.rpc.akka;

import akka.actor.ActorSystem;
import akka.actor.Terminated;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;

/**
 * Service provide access, start, stop methods to an actor system.
 */
public class AkkaActorSystemService {

	private static final Logger LOG = LoggerFactory.getLogger(AkkaActorSystemService.class);

	private ActorSystem actorSystem;

	private final Object lock = new Object();

	private volatile boolean stopped;

	private CompletableFuture<Void> terminationFuture;

	public AkkaActorSystemService(
		Configuration configuration,
		String listeningAddress,
		String portRangeDefinition,
		@Nonnull AkkaExecutorMode executorMode) throws Exception {
		actorSystem = BootstrapTools.startActorSystem(
			configuration,
			listeningAddress,
			"0",
			LOG,
			AkkaExecutorMode.SINGLE_THREAD_EXECUTOR);

		stopped = false;
		terminationFuture = new CompletableFuture<>();
	}

	public ActorSystem getActorSystem() {
		return actorSystem;
	}

	public CompletableFuture<Void> stopActorSystem() {
		synchronized (lock) {
			if (stopped) {
				return terminationFuture;
			}
			stopped = true;
		}

		LOG.info("Stopping Akka actor system.");

		final CompletableFuture<Terminated> actorSystemTerminationFuture = FutureUtils.toJava(actorSystem.terminate());

		actorSystemTerminationFuture.whenComplete(
			(Terminated ignored, Throwable throwable) -> {
				if (throwable != null) {
					terminationFuture.completeExceptionally(throwable);
				} else {
					terminationFuture.complete(null);
				}

				LOG.info("Stopped Akka actor system.");
			}
		);

		return terminationFuture;
	}
}
