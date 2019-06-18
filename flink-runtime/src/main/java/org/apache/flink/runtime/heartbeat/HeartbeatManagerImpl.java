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

package org.apache.flink.runtime.heartbeat;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;

import javax.annotation.concurrent.ThreadSafe;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Heartbeat manager implementation. The heartbeat manager maintains a map of heartbeat monitors
 * and resource IDs. Each monitor will be updated when a new heartbeat of the associated machine has
 * been received. If the monitor detects that a heartbeat has timed out, it will notify the
 * {@link HeartbeatListener} about it. A heartbeat times out iff no heartbeat signal has been
 * received within a given timeout interval.
 *
 * @param <I> Type of the incoming heartbeat payload
 * @param <O> Type of the outgoing heartbeat payload
 */
@ThreadSafe
public class HeartbeatManagerImpl<I, O> implements HeartbeatManager<I, O> {

	/** Heartbeat timeout interval in milli seconds. */
	private final long heartbeatTimeoutIntervalMs;

	/** Resource ID which is used to mark one own's heartbeat signals. */
	private final ResourceID ownResourceID;

	/** Heartbeat listener with which the heartbeat manager has been associated. */
	private final HeartbeatListener<I, O> heartbeatListener;

	/** Executor service used to run heartbeat timeout notifications. */
	private final ScheduledExecutor mainThreadExecutor;

	protected final Logger log;

	/** Map containing the heartbeat monitors associated with the respective resource ID. */
	private final ConcurrentHashMap<ResourceID, HeartbeatManagerImpl.HeartbeatMonitor<O>> heartbeatTargets;

	/** Running state of the heartbeat manager. */
	protected volatile boolean stopped;

	public HeartbeatManagerImpl(
			long heartbeatTimeoutIntervalMs,
			ResourceID ownResourceID,
			HeartbeatListener<I, O> heartbeatListener,
			ScheduledExecutor mainThreadExecutor,
			Logger log) {
		Preconditions.checkArgument(heartbeatTimeoutIntervalMs > 0L, "The heartbeat timeout has to be larger than 0.");

		this.heartbeatTimeoutIntervalMs = heartbeatTimeoutIntervalMs;
		this.ownResourceID = Preconditions.checkNotNull(ownResourceID);
		this.heartbeatListener = Preconditions.checkNotNull(heartbeatListener, "heartbeatListener");
		this.mainThreadExecutor = Preconditions.checkNotNull(mainThreadExecutor);
		this.log = Preconditions.checkNotNull(log);
		this.heartbeatTargets = new ConcurrentHashMap<>(16);

		stopped = false;
	}

	//----------------------------------------------------------------------------------------------
	// Getters
	//----------------------------------------------------------------------------------------------

	ResourceID getOwnResourceID() {
		return ownResourceID;
	}

	HeartbeatListener<I, O> getHeartbeatListener() {
		return heartbeatListener;
	}

	Collection<HeartbeatMonitor<O>> getHeartbeatTargets() {
		return heartbeatTargets.values();
	}

	//----------------------------------------------------------------------------------------------
	// HeartbeatManager methods
	//----------------------------------------------------------------------------------------------

	@Override
	public void monitorTarget(ResourceID resourceID, HeartbeatTarget<O> heartbeatTarget) {
		if (!stopped) {
			if (heartbeatTargets.containsKey(resourceID)) {
				log.debug("The target with resource ID {} is already been monitored.", resourceID);
			} else {
				HeartbeatManagerImpl.HeartbeatMonitor<O> heartbeatMonitor = new HeartbeatManagerImpl.HeartbeatMonitor<>(
					resourceID,
					heartbeatTarget,
					mainThreadExecutor,
					heartbeatListener,
					heartbeatTimeoutIntervalMs);

				heartbeatTargets.put(
					resourceID,
					heartbeatMonitor);

				// check if we have stopped in the meantime (concurrent stop operation)
				if (stopped) {
					heartbeatMonitor.cancel();

					heartbeatTargets.remove(resourceID);
				}
			}
		}
	}

	@Override
	public void unmonitorTarget(ResourceID resourceID) {
		if (!stopped) {
			HeartbeatManagerImpl.HeartbeatMonitor<O> heartbeatMonitor = heartbeatTargets.remove(resourceID);

			if (heartbeatMonitor != null) {
				heartbeatMonitor.cancel();
			}
		}
	}

	@Override
	public void stop() {
		stopped = true;

		for (HeartbeatManagerImpl.HeartbeatMonitor<O> heartbeatMonitor : heartbeatTargets.values()) {
			heartbeatMonitor.cancel();
		}

		heartbeatTargets.clear();
	}

	@Override
	public long getLastHeartbeatFrom(ResourceID resourceId) {
		HeartbeatMonitor<O> heartbeatMonitor = heartbeatTargets.get(resourceId);

		if (heartbeatMonitor != null) {
			return heartbeatMonitor.getLastHeartbeat();
		} else {
			return -1L;
		}
	}

	ScheduledExecutor getMainThreadExecutor() {
		return mainThreadExecutor;
	}

	//----------------------------------------------------------------------------------------------
	// HeartbeatTarget methods
	//----------------------------------------------------------------------------------------------

	@Override
	public void receiveHeartbeat(ResourceID heartbeatOrigin, I heartbeatPayload) {
		if (!stopped) {
			log.debug("Received heartbeat from {}.", heartbeatOrigin);
			reportHeartbeat(heartbeatOrigin);

			if (heartbeatPayload != null) {
				heartbeatListener.reportPayload(heartbeatOrigin, heartbeatPayload);
			}
		}
	}

	@Override
	public void requestHeartbeat(final ResourceID requestOrigin, I heartbeatPayload) {
		if (!stopped) {
			log.debug("Received heartbeat request from {}.", requestOrigin);

			final HeartbeatTarget<O> heartbeatTarget = reportHeartbeat(requestOrigin);

			if (heartbeatTarget != null) {
				if (heartbeatPayload != null) {
					heartbeatListener.reportPayload(requestOrigin, heartbeatPayload);
				}

				CompletableFuture<O> futurePayload = heartbeatListener.retrievePayload(requestOrigin);

				if (futurePayload != null) {
					CompletableFuture<Void> sendHeartbeatFuture = FutureUtils.thenAcceptAsyncIfNotDone(
						futurePayload,
						mainThreadExecutor,
						retrievedPayload ->	heartbeatTarget.receiveHeartbeat(getOwnResourceID(), retrievedPayload));

					sendHeartbeatFuture.exceptionally((Throwable failure) -> {
							log.warn("Could not send heartbeat to target with id {}.", requestOrigin, failure);

							return null;
						});
				} else {
					heartbeatTarget.receiveHeartbeat(ownResourceID, null);
				}
			}
		}
	}

	HeartbeatTarget<O> reportHeartbeat(ResourceID resourceID) {
		if (heartbeatTargets.containsKey(resourceID)) {
			HeartbeatManagerImpl.HeartbeatMonitor<O> heartbeatMonitor = heartbeatTargets.get(resourceID);
			heartbeatMonitor.reportHeartbeat();

			return heartbeatMonitor.getHeartbeatTarget();
		} else {
			return null;
		}
	}

	//----------------------------------------------------------------------------------------------
	// Utility classes
	//----------------------------------------------------------------------------------------------

	/**
	 * Heartbeat monitor which manages the heartbeat state of the associated heartbeat target. The
	 * monitor notifies the {@link HeartbeatListener} whenever it has not seen a heartbeat signal
	 * in the specified heartbeat timeout interval. Each heartbeat signal resets this timer.
	 *
	 * @param <O> Type of the payload being sent to the associated heartbeat target
	 */
	static class HeartbeatMonitor<O> implements Runnable {

		/** Resource ID of the monitored heartbeat target. */
		private final ResourceID resourceID;

		/** Associated heartbeat target. */
		private final HeartbeatTarget<O> heartbeatTarget;

		private final ScheduledExecutor scheduledExecutor;

		/** Listener which is notified about heartbeat timeouts. */
		private final HeartbeatListener<?, ?> heartbeatListener;

		/** Maximum heartbeat timeout interval. */
		private final long heartbeatTimeoutIntervalMs;

		private volatile ScheduledFuture<?> futureTimeout;

		private final AtomicReference<State> state = new AtomicReference<>(State.RUNNING);

		private volatile long lastHeartbeat;

		HeartbeatMonitor(
			ResourceID resourceID,
			HeartbeatTarget<O> heartbeatTarget,
			ScheduledExecutor scheduledExecutor,
			HeartbeatListener<?, O> heartbeatListener,
			long heartbeatTimeoutIntervalMs) {

			this.resourceID = Preconditions.checkNotNull(resourceID);
			this.heartbeatTarget = Preconditions.checkNotNull(heartbeatTarget);
			this.scheduledExecutor = Preconditions.checkNotNull(scheduledExecutor);
			this.heartbeatListener = Preconditions.checkNotNull(heartbeatListener);

			Preconditions.checkArgument(heartbeatTimeoutIntervalMs > 0L, "The heartbeat timeout interval has to be larger than 0.");
			this.heartbeatTimeoutIntervalMs = heartbeatTimeoutIntervalMs;

			lastHeartbeat = 0L;

			resetHeartbeatTimeout(heartbeatTimeoutIntervalMs);
		}

		HeartbeatTarget<O> getHeartbeatTarget() {
			return heartbeatTarget;
		}

		ResourceID getHeartbeatTargetId() {
			return resourceID;
		}

		public long getLastHeartbeat() {
			return lastHeartbeat;
		}

		void reportHeartbeat() {
			lastHeartbeat = System.currentTimeMillis();
			resetHeartbeatTimeout(heartbeatTimeoutIntervalMs);
		}

		void resetHeartbeatTimeout(long heartbeatTimeout) {
			if (state.get() == State.RUNNING) {
				cancelTimeout();

				futureTimeout = scheduledExecutor.schedule(this, heartbeatTimeout, TimeUnit.MILLISECONDS);

				// Double check for concurrent accesses (e.g. a firing of the scheduled future)
				if (state.get() != State.RUNNING) {
					cancelTimeout();
				}
			}
		}

		void cancel() {
			// we can only cancel if we are in state running
			if (state.compareAndSet(State.RUNNING, State.CANCELED)) {
				cancelTimeout();
			}
		}

		private void cancelTimeout() {
			if (futureTimeout != null) {
				futureTimeout.cancel(true);
			}
		}

		public boolean isCanceled() {
			return state.get() == State.CANCELED;
		}

		@Override
		public void run() {
			// The heartbeat has timed out if we're in state running
			if (state.compareAndSet(State.RUNNING, State.TIMEOUT)) {
				heartbeatListener.notifyHeartbeatTimeout(resourceID);
			}
		}

		private enum State {
			RUNNING,
			TIMEOUT,
			CANCELED
		}
	}
}
