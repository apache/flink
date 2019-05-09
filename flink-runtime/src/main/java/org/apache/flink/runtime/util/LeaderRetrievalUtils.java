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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalException;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.net.ConnectionUtils;
import org.apache.flink.util.FlinkException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.UUID;

import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.Promise;
import scala.concurrent.duration.FiniteDuration;

/**
 * Utility class to work with {@link LeaderRetrievalService} class.
 */
public class LeaderRetrievalUtils {

	private static final Logger LOG = LoggerFactory.getLogger(LeaderRetrievalUtils.class);

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
			Time timeout) throws LeaderRetrievalException {
		return retrieveLeaderConnectionInfo(leaderRetrievalService, FutureUtils.toFiniteDuration(timeout));
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

			return Await.result(connectionInfoFuture, timeout);
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
			Time timeout) throws LeaderRetrievalException {
		return findConnectingAddress(leaderRetrievalService, new FiniteDuration(timeout.getSize(), timeout.getUnit()));
	}

	public static InetAddress findConnectingAddress(
			LeaderRetrievalService leaderRetrievalService,
			FiniteDuration timeout) throws LeaderRetrievalException {
		ConnectionUtils.LeaderConnectingAddressListener listener = new ConnectionUtils.LeaderConnectingAddressListener();

		try {
			leaderRetrievalService.start(listener);

			LOG.info("Trying to select the network interface and address to use " +
					"by connecting to the leading JobManager.");

			LOG.info("TaskManager will try to connect for " + timeout +
					" before falling back to heuristics");

			return listener.findConnectingAddress(timeout);
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
			if (leaderAddress != null && !leaderAddress.equals("") && !connectionInfo.isCompleted()) {
				try {
					final LeaderConnectionInfo leaderConnectionInfo = new LeaderConnectionInfo(leaderAddress, leaderSessionID);
					connectionInfo.success(leaderConnectionInfo);
				} catch (FlinkException e) {
					connectionInfo.failure(e);
				}

			}
		}

		@Override
		public void handleError(Exception exception) {
			if (!connectionInfo.isCompleted()) {
				connectionInfo.failure(exception);
			}
		}
	}
	
	// ------------------------------------------------------------------------
	
	/**
	 * Private constructor to prevent instantiation.
	 */
	private LeaderRetrievalUtils() {
		throw new RuntimeException();
	}
}
