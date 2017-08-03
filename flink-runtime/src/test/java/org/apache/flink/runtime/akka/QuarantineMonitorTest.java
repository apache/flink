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

import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLogger;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Address;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.dispatch.OnComplete;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import scala.concurrent.duration.FiniteDuration;

/**
 * Tests for {@link QuarantineMonitor}.
 */
public class QuarantineMonitorTest extends TestLogger {

	private static final Logger LOG = LoggerFactory.getLogger(QuarantineMonitorTest.class);

	private static final FiniteDuration zeroDelay = new FiniteDuration(0L, TimeUnit.SECONDS);

	// we need two actor systems because we're quarantining one of them
	private static ActorSystem actorSystem1;
	private ActorSystem actorSystem2;

	@BeforeClass
	public static void setup() {
		Properties properties = new Properties();
		properties.setProperty("akka.remote.watch-failure-detector.threshold", "0.00001");
		properties.setProperty("akka.remote.watch-failure-detector.heartbeat-interval", "1 ms");
		properties.setProperty("akka.remote.watch-failure-detector.acceptable-heartbeat-pause", "1 ms");
		Config deathWatch = ConfigFactory.parseProperties(properties);
		Config defaultConfig = AkkaUtils.getDefaultAkkaConfig();

		actorSystem1 = AkkaUtils.createActorSystem(deathWatch.withFallback(defaultConfig));
	}

	@AfterClass
	public static void tearDown() {
		if (actorSystem1 != null) {
			actorSystem1.shutdown();
			actorSystem1.awaitTermination();
		}
	}

	@Before
	public void setupTest() {
		actorSystem2 = AkkaUtils.createDefaultActorSystem();
	}

	@After
	public void tearDownTest() {
		if (actorSystem2 != null) {
			actorSystem2.shutdown();
			actorSystem2.awaitTermination();
		}
	}

	/**
	 * Tests that the quarantine monitor detects if an actor system has been quarantined by another
	 * actor system.
	 */
	@Test(timeout = 5000L)
	public void testWatcheeQuarantined() throws ExecutionException, InterruptedException {
		TestingQuarantineHandler quarantineHandler = new TestingQuarantineHandler();

		ActorRef watchee = null;
		ActorRef watcher = null;
		ActorRef monitor = null;

		FiniteDuration timeout = new FiniteDuration(5, TimeUnit.SECONDS);
		FiniteDuration interval = new FiniteDuration(200, TimeUnit.MILLISECONDS);

		try {
			// start the quarantine monitor in the watchee actor system
			monitor = actorSystem2.actorOf(getQuarantineMonitorProps(quarantineHandler), "quarantineMonitor");

			watchee = actorSystem2.actorOf(getWatcheeProps(timeout, interval, quarantineHandler), "watchee");
			watcher = actorSystem1.actorOf(getWatcherProps(timeout, interval, quarantineHandler), "watcher");

			final Address actorSystem1Address = AkkaUtils.getAddress(actorSystem1);
			final String watcheeAddress = AkkaUtils.getAkkaURL(actorSystem2, watchee);
			final String watcherAddress = AkkaUtils.getAkkaURL(actorSystem1, watcher);

			// ping the watcher continuously
			watchee.tell(new Ping(watcherAddress), ActorRef.noSender());
			// start watching the watchee
			watcher.tell(new Watch(watcheeAddress), ActorRef.noSender());

			CompletableFuture<String> quarantineFuture = quarantineHandler.getWasQuarantinedByFuture();

			Assert.assertEquals(actorSystem1Address.toString(), quarantineFuture.get());
		} finally {
			TestingUtils.stopActor(watchee);
			TestingUtils.stopActor(watcher);
			TestingUtils.stopActor(monitor);
		}
	}

	/**
	 * Tests that the quarantine monitor detects if an actor system quarantines another actor
	 * system.
	 */
	@Test(timeout = 5000L)
	public void testWatcherQuarantining() throws ExecutionException, InterruptedException {
		TestingQuarantineHandler quarantineHandler = new TestingQuarantineHandler();

		ActorRef watchee = null;
		ActorRef watcher = null;
		ActorRef monitor = null;

		FiniteDuration timeout = new FiniteDuration(5, TimeUnit.SECONDS);
		FiniteDuration interval = new FiniteDuration(200, TimeUnit.MILLISECONDS);

		try {
			// start the quarantine monitor in the watcher actor system
			monitor = actorSystem1.actorOf(getQuarantineMonitorProps(quarantineHandler), "quarantineMonitor");

			watchee = actorSystem2.actorOf(getWatcheeProps(timeout, interval, quarantineHandler), "watchee");
			watcher = actorSystem1.actorOf(getWatcherProps(timeout, interval, quarantineHandler), "watcher");

			final Address actorSystem1Address = AkkaUtils.getAddress(actorSystem2);
			final String watcheeAddress = AkkaUtils.getAkkaURL(actorSystem2, watchee);
			final String watcherAddress = AkkaUtils.getAkkaURL(actorSystem1, watcher);

			// ping the watcher continuously
			watchee.tell(new Ping(watcherAddress), ActorRef.noSender());
			// start watching the watchee
			watcher.tell(new Watch(watcheeAddress), ActorRef.noSender());

			CompletableFuture<String> quarantineFuture = quarantineHandler.getHasQuarantinedFuture();

			Assert.assertEquals(actorSystem1Address.toString(), quarantineFuture.get());
		} finally {
			TestingUtils.stopActor(watchee);
			TestingUtils.stopActor(watcher);
			TestingUtils.stopActor(monitor);
		}
	}

	private static class TestingQuarantineHandler implements QuarantineHandler, ErrorHandler {

		private final CompletableFuture<String> wasQuarantinedByFuture;
		private final CompletableFuture<String> hasQuarantinedFuture;

		public TestingQuarantineHandler() {
			this.wasQuarantinedByFuture = new CompletableFuture<>();
			this.hasQuarantinedFuture = new CompletableFuture<>();
		}

		@Override
		public void wasQuarantinedBy(String remoteSystem, ActorSystem actorSystem) {
			wasQuarantinedByFuture.complete(remoteSystem);
		}

		@Override
		public void hasQuarantined(String remoteSystem, ActorSystem actorSystem) {
			hasQuarantinedFuture.complete(remoteSystem);
		}

		public CompletableFuture<String> getWasQuarantinedByFuture() {
			return wasQuarantinedByFuture;
		}

		public CompletableFuture<String> getHasQuarantinedFuture() {
			return hasQuarantinedFuture;
		}

		@Override
		public void handleError(Throwable failure) {
			wasQuarantinedByFuture.completeExceptionally(failure);
			hasQuarantinedFuture.completeExceptionally(failure);
		}
	}

	private interface ErrorHandler {
		void handleError(Throwable failure);
	}

	static class Watcher extends UntypedActor {

		private final FiniteDuration timeout;
		private final FiniteDuration interval;
		private final ErrorHandler errorHandler;

		Watcher(FiniteDuration timeout, FiniteDuration interval, ErrorHandler errorHandler) {
			this.timeout = Preconditions.checkNotNull(timeout);
			this.interval = Preconditions.checkNotNull(interval);
			this.errorHandler = Preconditions.checkNotNull(errorHandler);
		}

		@Override
		public void onReceive(Object message) throws Exception {
			if (message instanceof Watch) {
				Watch watch = (Watch) message;

				getContext().actorSelection(watch.getTarget()).resolveOne(timeout).onComplete(new OnComplete<ActorRef>() {
					@Override
					public void onComplete(Throwable failure, ActorRef success) throws Throwable {
						if (success != null) {
							getContext().watch(success);
							// constantly ping the watchee
							getContext().system().scheduler().schedule(
								zeroDelay,
								interval,
								success,
								"Watcher message",
								getContext().dispatcher(),
								getSelf());
						} else {
							errorHandler.handleError(failure);
						}
					}
				}, getContext().dispatcher());
			}
		}
	}

	static class Watchee extends UntypedActor {

		private final FiniteDuration timeout;
		private final FiniteDuration interval;
		private final ErrorHandler errorHandler;

		Watchee(FiniteDuration timeout, FiniteDuration interval, ErrorHandler errorHandler) {
			this.timeout = Preconditions.checkNotNull(timeout);
			this.interval = Preconditions.checkNotNull(interval);
			this.errorHandler = Preconditions.checkNotNull(errorHandler);
		}

		@Override
		public void onReceive(Object message) throws Exception {
			if (message instanceof Ping) {
				final Ping ping = (Ping) message;

				getContext().actorSelection(ping.getTarget()).resolveOne(timeout).onComplete(new OnComplete<ActorRef>() {
					@Override
					public void onComplete(Throwable failure, ActorRef success) throws Throwable {
						if (success != null) {
							// constantly ping the target
							getContext().system().scheduler().schedule(
								zeroDelay,
								interval,
								success,
								"Watchee message",
								getContext().dispatcher(),
								getSelf());
						} else {
							errorHandler.handleError(failure);
						}
					}
				}, getContext().dispatcher());
			}
		}
	}

	static class Watch {
		private final String target;

		Watch(String target) {
			this.target = target;
		}

		public String getTarget() {
			return target;
		}
	}

	static class Ping {
		private final String target;

		Ping(String target) {
			this.target = target;
		}

		public String getTarget() {
			return target;
		}
	}

	static Props getWatcheeProps(FiniteDuration timeout, FiniteDuration interval, ErrorHandler errorHandler) {
		return Props.create(Watchee.class, timeout, interval, errorHandler);
	}

	static Props getWatcherProps(FiniteDuration timeout, FiniteDuration interval, ErrorHandler errorHandler) {
		return Props.create(Watcher.class, timeout, interval, errorHandler);
	}

	static Props getQuarantineMonitorProps(QuarantineHandler handler) {
		return Props.create(QuarantineMonitor.class, handler, LOG);
	}

}
