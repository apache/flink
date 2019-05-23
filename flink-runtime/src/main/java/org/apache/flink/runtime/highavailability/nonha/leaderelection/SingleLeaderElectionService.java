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

package org.apache.flink.runtime.highavailability.nonha.leaderelection;

import org.apache.flink.runtime.leaderelection.LeaderContender;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.util.HashSet;
import java.util.UUID;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * An implementation of the {@link LeaderElectionService} interface that handles a single
 * leader contender. When started, this service immediately grants the contender the leadership.
 *
 * <p>The implementation accepts a single static leader session ID and is hence compatible with
 * pre-configured single leader (no leader failover) setups.
 *
 * <p>This implementation supports a series of leader listeners that receive notifications about
 * the leader contender.
 */
public class SingleLeaderElectionService implements LeaderElectionService {

	private static final Logger LOG = LoggerFactory.getLogger(SingleLeaderElectionService.class);

	// ------------------------------------------------------------------------

	/** lock for all operations on this instance. */
	private final Object lock = new Object();

	/** The executor service that dispatches notifications. */
	private final Executor notificationExecutor;

	/** The leader ID assigned to the immediate leader. */
	private final UUID leaderId;

	@GuardedBy("lock")
	private final HashSet<EmbeddedLeaderRetrievalService> listeners;

	/** The currently proposed leader. */
	@GuardedBy("lock")
	private volatile LeaderContender proposedLeader;

	/** The confirmed leader. */
	@GuardedBy("lock")
	private volatile LeaderContender leader;

	/** The address of the confirmed leader. */
	@GuardedBy("lock")
	private volatile String leaderAddress;

	/** Flag marking this service as shutdown, meaning it cannot be started again. */
	@GuardedBy("lock")
	private volatile boolean shutdown;

	// ------------------------------------------------------------------------

	/**
	 * Creates a new leader election service. The service assigns the given leader ID
	 * to the leader contender.
	 *
	 * @param leaderId The constant leader ID assigned to the leader.
	 */
	public SingleLeaderElectionService(Executor notificationsDispatcher, UUID leaderId) {
		this.notificationExecutor = checkNotNull(notificationsDispatcher);
		this.leaderId = checkNotNull(leaderId);
		this.listeners = new HashSet<>();

		shutdown = false;
	}

	// ------------------------------------------------------------------------
	//  leader election service
	// ------------------------------------------------------------------------

	@Override
	public void start(LeaderContender contender) throws Exception {
		checkNotNull(contender, "contender");

		synchronized (lock) {
			checkState(!shutdown, "service is shut down");
			checkState(proposedLeader == null, "service already started");

			// directly grant leadership to the given contender
			proposedLeader = contender;
			notificationExecutor.execute(new GrantLeadershipCall(contender, leaderId));
		}
	}

	@Override
	public void stop() {
		synchronized (lock) {
			// notify all listeners that there is no leader
			for (EmbeddedLeaderRetrievalService listener : listeners) {
				notificationExecutor.execute(
						new NotifyOfLeaderCall(null, null, listener.listener, LOG));
			}

			// if there was a leader, revoke its leadership
			if (leader != null) {
				try {
					leader.revokeLeadership();
				} catch (Throwable t) {
					leader.handleError(t instanceof Exception ? (Exception) t : new Exception(t));
				}
			}

			proposedLeader = null;
			leader = null;
			leaderAddress = null;
		}
	}

	@Override
	public void confirmLeaderSessionID(UUID leaderSessionID) {
		checkNotNull(leaderSessionID, "leaderSessionID");
		checkArgument(leaderSessionID.equals(leaderId), "confirmed wrong leader session id");

		synchronized (lock) {
			checkState(!shutdown, "service is shut down");
			checkState(proposedLeader != null, "no leader proposed yet");
			checkState(leader == null, "leader already confirmed");

			// accept the confirmation
			final String address = proposedLeader.getAddress();
			leaderAddress = address;
			leader = proposedLeader;

			// notify all listeners
			for (EmbeddedLeaderRetrievalService listener : listeners) {
				notificationExecutor.execute(
						new NotifyOfLeaderCall(address, leaderId, listener.listener, LOG));
			}
		}
	}

	@Override
	public boolean hasLeadership(@Nonnull UUID leaderSessionId) {
		synchronized (lock) {
			return proposedLeader != null && leaderSessionId.equals(leaderId);
		}
	}

	void errorOnGrantLeadership(LeaderContender contender, Throwable error) {
		LOG.warn("Error granting leadership to contender", error);
		contender.handleError(error instanceof Exception ? (Exception) error : new Exception(error));

		synchronized (lock) {
			if (proposedLeader == contender) {
				proposedLeader = null;
				leader = null;
			}
		}
	}

	// ------------------------------------------------------------------------
	//  shutdown
	// ------------------------------------------------------------------------

	public boolean isShutdown() {
		synchronized (lock) {
			return shutdown;
		}
	}

	public void shutdown() {
		shutdownInternally(new Exception("The leader service is shutting down"));
	}

	private void shutdownInternally(Exception exceptionForHandlers) {
		synchronized (lock) {
			if (shutdown) {
				return;
			}

			shutdown = true;

			// fail the leader (if there is one)
			if (leader != null) {
				try {
					leader.handleError(exceptionForHandlers);
				} catch (Throwable ignored) {}
			}

			// clear all leader status
			leader = null;
			proposedLeader = null;
			leaderAddress = null;

			// fail all registered listeners
			for (EmbeddedLeaderRetrievalService service : listeners) {
				service.shutdown(exceptionForHandlers);
			}
			listeners.clear();
		}
	}

	private void fatalError(Throwable error) {
		LOG.error("Embedded leader election service encountered a fatal error. Shutting down service.", error);

		shutdownInternally(new Exception("Leader election service is shutting down after a fatal error", error));
	}

	// ------------------------------------------------------------------------
	//  leader listeners
	// ------------------------------------------------------------------------

	public LeaderRetrievalService createLeaderRetrievalService() {
		synchronized (lock) {
			checkState(!shutdown, "leader election service is shut down");
			return new EmbeddedLeaderRetrievalService();
		}
	}

	void addListener(EmbeddedLeaderRetrievalService service, LeaderRetrievalListener listener) {
		synchronized (lock) {
			checkState(!shutdown, "leader election service is shut down");
			checkState(!service.running, "leader retrieval service is already started");

			try {
				if (!listeners.add(service)) {
					throw new IllegalStateException("leader retrieval service was added to this service multiple times");
				}

				service.listener = listener;
				service.running = true;

				// if we already have a leader, immediately notify this new listener
				if (leader != null) {
					notificationExecutor.execute(
							new NotifyOfLeaderCall(leaderAddress, leaderId, listener, LOG));
				}
			}
			catch (Throwable t) {
				fatalError(t);
			}
		}
	}

	void removeListener(EmbeddedLeaderRetrievalService service) {
		synchronized (lock) {
			// if the service was not even started, simply do nothing
			if (!service.running || shutdown) {
				return;
			}

			try {
				if (!listeners.remove(service)) {
					throw new IllegalStateException("leader retrieval service does not belong to this service");
				}

				// stop the service
				service.listener = null;
				service.running = false;
			}
			catch (Throwable t) {
				fatalError(t);
			}
		}
	}

	// ------------------------------------------------------------------------

	private class EmbeddedLeaderRetrievalService implements LeaderRetrievalService {

		volatile LeaderRetrievalListener listener;

		volatile boolean running;

		@Override
		public void start(LeaderRetrievalListener listener) throws Exception {
			checkNotNull(listener);
			addListener(this, listener);
		}

		@Override
		public void stop() throws Exception {
			removeListener(this);
		}

		void shutdown(Exception cause) {
			if (running) {
				final LeaderRetrievalListener lst = listener;
				running = false;
				listener = null;

				try {
					lst.handleError(cause);
				} catch (Throwable ignored) {}
			}
		}
	}

	// ------------------------------------------------------------------------
	//  asynchronous notifications
	// ------------------------------------------------------------------------

	/**
	 * This runnable informs a leader contender that it gained leadership.
	 */
	private class GrantLeadershipCall implements Runnable {

		private final LeaderContender contender;
		private final UUID leaderSessionId;

		GrantLeadershipCall(LeaderContender contender, UUID leaderSessionId) {

			this.contender = checkNotNull(contender);
			this.leaderSessionId = checkNotNull(leaderSessionId);
		}

		@Override
		public void run() {
			try {
				contender.grantLeadership(leaderSessionId);
			}
			catch (Throwable t) {
				errorOnGrantLeadership(contender, t);
			}
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * This runnable informs a leader listener of a new leader.
	 */
	private static class NotifyOfLeaderCall implements Runnable {

		@Nullable
		private final String address;       // null if leader revoked without new leader
		@Nullable
		private final UUID leaderSessionId; // null if leader revoked without new leader

		private final LeaderRetrievalListener listener;
		private final Logger logger;

		NotifyOfLeaderCall(
				@Nullable String address,
				@Nullable UUID leaderSessionId,
				LeaderRetrievalListener listener,
				Logger logger) {

			this.address = address;
			this.leaderSessionId = leaderSessionId;
			this.listener = checkNotNull(listener);
			this.logger = checkNotNull(logger);
		}

		@Override
		public void run() {
			try {
				listener.notifyLeaderAddress(address, leaderSessionId);
			}
			catch (Throwable t) {
				logger.warn("Error notifying leader listener about new leader", t);
				listener.handleError(t instanceof Exception ? (Exception) t : new Exception(t));
			}
		}
	}
}
