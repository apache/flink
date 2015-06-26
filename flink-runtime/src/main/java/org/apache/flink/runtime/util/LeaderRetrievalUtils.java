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

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.instance.AkkaActorGateway;
import org.apache.flink.runtime.jobmanager.RecoveryMode;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalException;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.net.NetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.Promise;
import scala.concurrent.duration.FiniteDuration;

import java.net.InetAddress;
import java.util.UUID;

public class LeaderRetrievalUtils {

	private static final Logger LOG = LoggerFactory.getLogger(LeaderRetrievalUtils.class);

	/**
	 * Creates a {@link LeaderRetrievalService} based on the provided {@link Configuration} object.
	 *
	 * @param configuration Configuration containing the settings for the {@link LeaderRetrievalService}
	 * @return The {@link LeaderRetrievalService} specified in the configuration object
	 * @throws Exception
	 */
	public static LeaderRetrievalService createLeaderRetrievalService(Configuration configuration)
		throws Exception {

		RecoveryMode recoveryMode = RecoveryMode.valueOf(
				configuration.getString(
						ConfigConstants.RECOVERY_MODE,
						ConfigConstants.DEFAULT_RECOVERY_MODE).toUpperCase());

		switch (recoveryMode) {
			case STANDALONE:
				return StandaloneUtils.createLeaderRetrievalService(configuration);
			case ZOOKEEPER:
				return ZooKeeperUtils.createLeaderRetrievalService(configuration);
			default:
				throw new Exception("Recovery mode " + recoveryMode + " is not supported.");
		}
	}

	/**
	 * Retrieves the current leader gateway using the given {@link LeaderRetrievalService}. If the
	 * current leader could not be retrieved after the given timeout, then a
	 * {@link LeaderRetrievalException} is thrown.
	 *
	 * @param leaderRetrievalService {@link LeaderRetrievalService} which is used for the leader retrieval
	 * @param actorSystem ActorSystem which is used for the {@link LeaderRetrievalListener} implementation
	 * @param timeout Timeout value for the retrieval call
	 * @return The current leader gateway
	 * @throws LeaderRetrievalException If the actor gateway could not be retrieved or the timeout has been exceeded
	 */
	public static ActorGateway retrieveLeaderGateway(
			LeaderRetrievalService leaderRetrievalService,
			ActorSystem actorSystem,
			FiniteDuration timeout)
		throws LeaderRetrievalException {
		LeaderGatewayListener listener = new LeaderGatewayListener(actorSystem, timeout);

		try {
			leaderRetrievalService.start(listener);

			Future<ActorGateway> actorGatewayFuture = listener.getActorGatewayFuture();

			ActorGateway gateway = Await.result(actorGatewayFuture, timeout);

			return gateway;
		} catch (Exception e) {
			throw new LeaderRetrievalException("Could not retrieve the leader gateway", e);
		} finally {
			try {
				leaderRetrievalService.stop();
			} catch (Exception fe) {
				LOG.warn("Could not stop the leader retrieval service.", fe);
			}
		}
	}

	/**
	 * Retrieves the leader akka url and the current leader session ID. The values are stored in a
	 * {@link LeaderConnectionInfo} instance.
	 *
	 * @param leaderRetrievalService Leader retrieval service to retrieve the leader connection
	 *                               information
	 * @param timeout Timeout when to give up looking for the leader
	 * @return LeaderConnectionInfo containing the leader's akka URL and the current leader session
	 * ID
	 * @throws LeaderRetrievalException
	 */
	public static LeaderConnectionInfo retrieveLeaderConnectionInfo(
			LeaderRetrievalService leaderRetrievalService,
			FiniteDuration timeout
	) throws LeaderRetrievalException {
		LeaderConnectionInfoListener listener = new LeaderConnectionInfoListener();

		try {
			leaderRetrievalService.start(listener);

			Future<LeaderConnectionInfo> connectionInfoFuture = listener.getLeaderConnectionInfoFuture();

			LeaderConnectionInfo result = Await.result(connectionInfoFuture, timeout);

			return result;
		} catch (Exception e) {
			throw new LeaderRetrievalException("Could not retrieve the leader address and leader " +
					"session ID.", e);
		} finally {
			try {
				leaderRetrievalService.stop();
			} catch (Exception fe) {
				LOG.warn("Could not stop the leader retrieval service.", fe);
			}
		}
	}

	public static InetAddress findConnectingAddress(
			LeaderRetrievalService leaderRetrievalService,
			FiniteDuration timeout) throws LeaderRetrievalException {
		NetUtils.LeaderConnectingAddressListener listener = new NetUtils.LeaderConnectingAddressListener();

		try {
			leaderRetrievalService.start(listener);

			LOG.info("Trying to select the network interface and address to use " +
					"by connecting to the leading JobManager.");

			LOG.info("TaskManager will try to connect for " + timeout +
					" before falling back to heuristics");

			InetAddress result =  listener.findConnectingAddress(timeout);

			return result;
		} catch (Exception e) {
			throw new LeaderRetrievalException("Could not find the connecting address by " +
					"connecting to the current leader.", e);
		} finally {
			try {
				leaderRetrievalService.stop();
			} catch (Exception fe) {
				LOG.warn("Could not stop the leader retrieval service.", fe);
			}
		}
	}

	/**
	 * Helper class which is used by the retrieveLeaderGateway method as the
	 * {@link LeaderRetrievalListener}.
	 */
	public static class LeaderGatewayListener implements LeaderRetrievalListener {

		private final ActorSystem actorSystem;
		private final FiniteDuration timeout;

		private final Promise<ActorGateway> futureActorGateway = new scala.concurrent.impl.Promise.DefaultPromise<ActorGateway>();

		public LeaderGatewayListener(ActorSystem actorSystem, FiniteDuration timeout) {
			this.actorSystem = actorSystem;
			this.timeout = timeout;
		}

		@Override
		public void notifyLeaderAddress(String leaderAddress, UUID leaderSessionID) {
			if(leaderAddress != null && !leaderAddress.equals("") && !futureActorGateway.isCompleted()) {
				try {
					ActorRef actorRef = AkkaUtils.getActorRef(leaderAddress, actorSystem, timeout);

					ActorGateway gateway = new AkkaActorGateway(actorRef, leaderSessionID);

					futureActorGateway.success(gateway);

				} catch(Exception e){
					futureActorGateway.failure(e);
				}
			}
		}

		@Override
		public void handleError(Exception exception) {
			if (!futureActorGateway.isCompleted()) {
				futureActorGateway.failure(exception);
			}
		}

		public Future<ActorGateway> getActorGatewayFuture() {
			return futureActorGateway.future();
		}
	}

	/**
	 * Helper class which is used by the retrieveLeaderConnectionInfo method to retrieve the
	 * leader's akka URL and the current leader session ID.
	 */
	public static class LeaderConnectionInfoListener implements  LeaderRetrievalListener {
		private final Promise<LeaderConnectionInfo> connectionInfo = new scala.concurrent.impl.Promise.DefaultPromise<>();

		public Future<LeaderConnectionInfo> getLeaderConnectionInfoFuture() {
			return connectionInfo.future();
		}

		@Override
		public void notifyLeaderAddress(String leaderAddress, UUID leaderSessionID) {
			if(leaderAddress != null && !leaderAddress.equals("") && !connectionInfo.isCompleted()) {
				connectionInfo.success(new LeaderConnectionInfo(leaderAddress, leaderSessionID));
			}
		}

		@Override
		public void handleError(Exception exception) {
			if (!connectionInfo.isCompleted()) {
				connectionInfo.failure(exception);
			}
		}
	}
}
