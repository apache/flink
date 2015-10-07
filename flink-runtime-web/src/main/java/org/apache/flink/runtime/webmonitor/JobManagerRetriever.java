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
import scala.concurrent.duration.FiniteDuration;

import java.util.UUID;

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

	private final Object lock = new Object();

	private final WebMonitor webMonitor;
	private final ActorSystem actorSystem;
	private final FiniteDuration lookupTimeout;
	private final FiniteDuration timeout;

	private volatile Tuple2<Promise<ActorGateway>, Promise<Integer>> leaderPromise =
			new Tuple2<Promise<ActorGateway>, Promise<Integer>>(
					new scala.concurrent.impl.Promise.DefaultPromise<ActorGateway>(),
					new scala.concurrent.impl.Promise.DefaultPromise<Integer>());

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
	 * Returns the leading job manager gateway and its web monitor port.
	 */
	public Option<Tuple2<ActorGateway, Integer>> getJobManagerGatewayAndWebPort() throws Exception {
		Tuple2<Promise<ActorGateway>, Promise<Integer>> promise = leaderPromise;

		if (!promise._1().isCompleted() || !promise._1().isCompleted()) {
			return Option.empty();
		}
		else {
			Promise<ActorGateway> leaderGatewayPromise = promise._1();
			Promise<Integer> leaderWebPortPromise = promise._2();

			ActorGateway leaderGateway = Await.result(leaderGatewayPromise.future(), timeout);
			int leaderWebPort = Await.result(leaderWebPortPromise.future(), timeout);

			return Option.apply(new Tuple2<>(leaderGateway, leaderWebPort));
		}
	}

	/**
	 * Awaits the leading job manager gateway and its web monitor port.
	 */
	public Tuple2<ActorGateway, Integer> awaitJobManagerGatewayAndWebPort() throws Exception {
		Tuple2<Promise<ActorGateway>, Promise<Integer>> promise = leaderPromise;

		Promise<ActorGateway> leaderGatewayPromise = promise._1();
		Promise<Integer> leaderWebPortPromise = promise._2();

		ActorGateway leaderGateway = Await.result(leaderGatewayPromise.future(), timeout);
		int leaderWebPort = Await.result(leaderWebPortPromise.future(), timeout);

		return new Tuple2<>(leaderGateway, leaderWebPort);
	}

	@Override
	public void notifyLeaderAddress(final String leaderAddress, final UUID leaderSessionID) {
		if (leaderAddress != null && !leaderAddress.equals("")) {
			try {
				final Promise<ActorGateway> gatewayPromise = new scala.concurrent.impl.Promise.DefaultPromise<>();
				final Promise<Integer> webPortPromise = new scala.concurrent.impl.Promise.DefaultPromise<>();

				final Tuple2<Promise<ActorGateway>, Promise<Integer>> newPromise = new Tuple2<>(
						gatewayPromise, webPortPromise);

				LOG.info("Retrieved leader notification {}:{}.", leaderAddress, leaderSessionID);

				AkkaUtils.getActorRefFuture(leaderAddress, actorSystem, lookupTimeout)
						// Resolve the actor ref
						.flatMap(new Mapper<ActorRef, Future<Object>>() {
							@Override
							public Future<Object> apply(ActorRef jobManagerRef) {
								ActorGateway leaderGateway = new AkkaActorGateway(
										jobManagerRef, leaderSessionID);

								gatewayPromise.success(leaderGateway);

								return leaderGateway.ask(JobManagerMessages
										.getRequestWebMonitorPort(), timeout);
							}
						}, actorSystem.dispatcher())
								// Request the web monitor port
						.onComplete(new OnComplete<Object>() {
							@Override
							public void onComplete(Throwable failure, Object success) throws Throwable {
								if (failure == null) {
									int webMonitorPort = ((ResponseWebMonitorPort) success).port();
									webPortPromise.success(webMonitorPort);

									// Complete the promise
									synchronized (lock) {
										Tuple2<Promise<ActorGateway>, Promise<Integer>>
												previousPromise = leaderPromise;

										leaderPromise = newPromise;

										if (!previousPromise._2().isCompleted()) {
											previousPromise._1().completeWith(gatewayPromise.future());
											previousPromise._2().completeWith(webPortPromise.future());
										}
									}
								}
								else {
									LOG.warn("Failed to retrieve leader gateway and port.");
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
