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

package org.apache.flink.runtime.webmonitor;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.dispatch.Futures;
import akka.dispatch.Mapper;
import akka.dispatch.OnComplete;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.instance.AkkaActorGateway;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.messages.JobManagerMessages.ResponseWebMonitorPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.Tuple2;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.Promise;
import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;

import java.util.UUID;
import java.util.concurrent.TimeoutException;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Retrieves and stores the actor gateway to the current leading JobManager. In case of an error,
 * the {@link WebRuntimeMonitor} to which this instance is associated will be stopped.
 *
 * <p>The job manager gateway only works if the web monitor and the job manager run in the same
 * actor system, because many execution graph structures are not serializable. This breaks the nice
 * leader retrieval abstraction and we have a special code path in case that another job manager is
 * leader (see {@link org.apache.flink.runtime.webmonitor.handlers.HandlerRedirectUtils}. In such a
 * case, we get the address of the web monitor of the leading job manager and redirect to it
 * (instead of directly communicating with it).
 */
public class JobManagerRetriever implements LeaderRetrievalListener {

	private static final Logger LOG = LoggerFactory.getLogger(JobManagerRetriever.class);

	private final Object waitLock = new Object();

	private final WebMonitor webMonitor;
	private final ActorSystem actorSystem;
	private final FiniteDuration lookupTimeout;
	private final FiniteDuration timeout;

	private volatile Future<Tuple2<ActorGateway, Integer>> leaderGatewayPortFuture;

	public JobManagerRetriever(
			WebMonitor webMonitor,
			ActorSystem actorSystem,
			FiniteDuration lookupTimeout,
			FiniteDuration timeout) {

		this.webMonitor = checkNotNull(webMonitor);
		this.actorSystem = checkNotNull(actorSystem);
		this.lookupTimeout = checkNotNull(lookupTimeout);
		this.timeout = checkNotNull(timeout);
	}

	/**
	 * Returns the currently known leading job manager gateway and its web monitor port.
	 */
	public Option<Tuple2<ActorGateway, Integer>> getJobManagerGatewayAndWebPort() throws Exception {
		if (leaderGatewayPortFuture != null) {
			Future<Tuple2<ActorGateway, Integer>> gatewayPortFuture = leaderGatewayPortFuture;

			if (gatewayPortFuture.isCompleted()) {
				Tuple2<ActorGateway, Integer> gatewayPort = Await.result(gatewayPortFuture, timeout);

				return Option.apply(gatewayPort);
			} else {
				return Option.empty();
			}
		} else {
			return Option.empty();
		}
	}

	/**
	 * Awaits the leading job manager gateway and its web monitor port.
	 */
	public Tuple2<ActorGateway, Integer> awaitJobManagerGatewayAndWebPort() throws Exception {
		Future<Tuple2<ActorGateway, Integer>> gatewayPortFuture = null;
		Deadline deadline = timeout.fromNow();

		while(!deadline.isOverdue()) {
			synchronized (waitLock) {
				gatewayPortFuture = leaderGatewayPortFuture;

				if (gatewayPortFuture != null) {
					break;
				}

				waitLock.wait(deadline.timeLeft().toMillis());
			}
		}

		if (gatewayPortFuture == null) {
			throw new TimeoutException("There is no JobManager available.");
		} else {
			return Await.result(gatewayPortFuture, deadline.timeLeft());
		}
	}

	@Override
	public void notifyLeaderAddress(final String leaderAddress, final UUID leaderSessionID) {
		if (leaderAddress != null && !leaderAddress.equals("")) {
			try {
				final Promise<Tuple2<ActorGateway, Integer>> leaderGatewayPortPromise = new scala.concurrent.impl.Promise.DefaultPromise<>();

				synchronized (waitLock) {
					leaderGatewayPortFuture = leaderGatewayPortPromise.future();
					waitLock.notifyAll();
				}

				LOG.info("New leader reachable under {}:{}.", leaderAddress, leaderSessionID);

				AkkaUtils.getActorRefFuture(leaderAddress, actorSystem, lookupTimeout)
						// Resolve the actor ref
						.flatMap(new Mapper<ActorRef, Future<Tuple2<ActorGateway, Object>>>() {
							@Override
							public Future<Tuple2<ActorGateway, Object>> apply(ActorRef jobManagerRef) {
								ActorGateway leaderGateway = new AkkaActorGateway(
										jobManagerRef, leaderSessionID);

								Future<Object> webMonitorPort = leaderGateway.ask(
									JobManagerMessages.getRequestWebMonitorPort(),
									timeout);

								return Futures.successful(leaderGateway).zip(webMonitorPort);
							}
						}, actorSystem.dispatcher())
								// Request the web monitor port
						.onComplete(new OnComplete<Tuple2<ActorGateway, Object>>() {
							@Override
							public void onComplete(Throwable failure, Tuple2<ActorGateway, Object> success) throws Throwable {
								if (failure == null) {
									if (success._2() instanceof ResponseWebMonitorPort) {
										int webMonitorPort = ((ResponseWebMonitorPort) success._2()).port();

										leaderGatewayPortPromise.success(new Tuple2<>(success._1(), webMonitorPort));
									} else {
										leaderGatewayPortPromise.failure(new Exception("Received the message " +
										success._2() + " as response to " + JobManagerMessages.getRequestWebMonitorPort() +
											". But a message of type " + ResponseWebMonitorPort.class + " was expected."));
									}
								} else {
									LOG.warn("Failed to retrieve leader gateway and port.", failure);
									leaderGatewayPortPromise.failure(failure);
								}
							}
						}, actorSystem.dispatcher());
			}
			catch (Exception e) {
				handleError(e);
			}
		}
	}

	@Override
	public void handleError(Exception exception) {
		LOG.error("Received error from LeaderRetrievalService.", exception);

		try {
			// stop associated webMonitor
			webMonitor.stop();
		}
		catch (Exception e) {
			LOG.error("Error while stopping the web server due to a LeaderRetrievalService error.", e);
		}
	}
}
