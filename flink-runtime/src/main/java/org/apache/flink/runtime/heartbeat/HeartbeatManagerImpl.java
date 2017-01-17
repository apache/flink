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
import org.apache.flink.runtime.concurrent.AcceptFunction;
import org.apache.flink.runtime.concurrent.Future;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
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
public class HeartbeatManagerImpl<I, O> implements HeartbeatManager<I, O>, HeartbeatTarget<I> {

	/** Heartbeat timeout interval in milli seconds */
	private final long heartbeatTimeoutIntervalMs;

	/** Resource ID which is used to mark one own's heartbeat signals */
	private final ResourceID ownResourceID;

	/** Executor service used to run heartbeat timeout notifications */
	private final ScheduledExecutorService scheduledExecutorService;

	protected final Logger log;

	/** Map containing the heartbeat monitors associated with the respective resource ID */
	private final ConcurrentHashMap<ResourceID, HeartbeatManagerImpl.HeartbeatMonitor<O>> heartbeatTargets;

	/** Execution context used to run future callbacks */
	private final Executor executor;

	/** Heartbeat listener with which the heartbeat manager has been associated */
	private HeartbeatListener<I, O> heartbeatListener;

	/** Running state of the heartbeat manager */
	protected volatile boolean stopped;

	public HeartbeatManagerImpl(
		long heartbeatTimeoutIntervalMs,
		ResourceID ownResourceID,
		Executor executor,
		ScheduledExecutorService scheduledExecutorService,
		Logger log) {
		Preconditions.checkArgument(heartbeatTimeoutIntervalMs > 0L, "The heartbeat timeout has to be larger than 0.");

		this.heartbeatTimeoutIntervalMs = heartbeatTimeoutIntervalMs;
		this.ownResourceID = Preconditions.checkNotNull(ownResourceID);
		this.scheduledExecutorService = Preconditions.checkNotNull(scheduledExecutorService);
		this.log = Preconditions.checkNotNull(log);
		this.executor = Preconditions.checkNotNull(executor);
		this.heartbeatTargets = new ConcurrentHashMap<>(16);

		stopped = true;
	}

	//----------------------------------------------------------------------------------------------
	// Getters
	//----------------------------------------------------------------------------------------------

	ResourceID getOwnResourceID() {
		return ownResourceID;
	}

	Executor getExecutor() {
		return executor;
	}

	HeartbeatListener<I, O> getHeartbeatListener() {
		return heartbeatListener;
	}

	Collection<HeartbeatManagerImpl.HeartbeatMonitor<O>> getHeartbeatTargets() {
		return heartbeatTargets.values();
	}

	//----------------------------------------------------------------------------------------------
	// HeartbeatManager methods
	//----------------------------------------------------------------------------------------------

	@Override
	public void monitorTarget(ResourceID resourceID, HeartbeatTarget<O> heartbeatTarget) {
		if (!stopped) {
			if (heartbeatTargets.containsKey(resourceID)) {
				log.info("The target with resource ID {} is already been monitored.", resourceID);
			} else {
				HeartbeatManagerImpl.HeartbeatMonitor<O> heartbeatMonitor = new HeartbeatManagerImpl.HeartbeatMonitor<>(
					resourceID,
					heartbeatTarget,
					scheduledExecutorService,
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
	public void start(HeartbeatListener<I, O> heartbeatListener) {
		Preconditions.checkState(stopped, "Cannot start an already started heartbeat manager.");

		stopped = false;

		this.heartbeatListener = Preconditions.checkNotNull(heartbeatListener);
	}

	@Override
	public void stop() {
		stopped = true;

		for (HeartbeatManagerImpl.HeartbeatMonitor<O> heartbeatMonitor : heartbeatTargets.values()) {
			heartbeatMonitor.cancel();
		}

		heartbeatTargets.clear();
	}

	//----------------------------------------------------------------------------------------------
	// HeartbeatTarget methods
	//----------------------------------------------------------------------------------------------

	@Override
	public void sendHeartbeat(ResourceID resourceID, I payload) {
		if (!stopped) {
			log.debug("Received heartbeat from {}.", resourceID);
			reportHeartbeat(resourceID);

			if (payload != null) {
				heartbeatListener.reportPayload(resourceID, payload);
			}
		}
	}

	@Override
	public void requestHeartbeat(ResourceID resourceID, I payload) {
		if (!stopped) {
			log.debug("Received heartbeat request from {}.", resourceID);

			final HeartbeatTarget<O> heartbeatTarget = reportHeartbeat(resourceID);

			if (heartbeatTarget != null) {
				if (payload != null) {
					heartbeatListener.reportPayload(resourceID, payload);
				}

				Future<O> futurePayload = heartbeatListener.retrievePayload();

				if (futurePayload != null) {
					futurePayload.thenAcceptAsync(new AcceptFunction<O>() {
						@Override
						public void accept(O retrievedPayload) {
							heartbeatTarget.sendHeartbeat(getOwnResourceID(), retrievedPayload);
						}
					}, executor);
				} else {
					heartbeatTarget.sendHeartbeat(ownResourceID, null);
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

		/** Resource ID of the monitored heartbeat target */
		private final ResourceID resourceID;

		/** Associated heartbeat target */
		private final HeartbeatTarget<O> heartbeatTarget;

		private final ScheduledExecutorService scheduledExecutorService;

		/** Listener which is notified about heartbeat timeouts */
		private final HeartbeatListener<?, ?> heartbeatListener;

		/** Maximum heartbeat timeout interval */
		private final long heartbeatTimeoutIntervalMs;

		private volatile ScheduledFuture<?> futureTimeout;

		private final AtomicReference<State> state = new AtomicReference<>(State.RUNNING);

		HeartbeatMonitor(
			ResourceID resourceID,
			HeartbeatTarget<O> heartbeatTarget,
			ScheduledExecutorService scheduledExecutorService,
			HeartbeatListener<?, O> heartbeatListener,
			long heartbeatTimeoutIntervalMs) {

			this.resourceID = Preconditions.checkNotNull(resourceID);
			this.heartbeatTarget = Preconditions.checkNotNull(heartbeatTarget);
			this.scheduledExecutorService = Preconditions.checkNotNull(scheduledExecutorService);
			this.heartbeatListener = Preconditions.checkNotNull(heartbeatListener);

			Preconditions.checkArgument(heartbeatTimeoutIntervalMs >= 0L, "The heartbeat timeout interval has to be larger than 0.");
			this.heartbeatTimeoutIntervalMs = heartbeatTimeoutIntervalMs;

			resetHeartbeatTimeout(heartbeatTimeoutIntervalMs);
		}

		HeartbeatTarget<O> getHeartbeatTarget() {
			return heartbeatTarget;
		}

		void reportHeartbeat() {
			resetHeartbeatTimeout(heartbeatTimeoutIntervalMs);
		}

		void resetHeartbeatTimeout(long heartbeatTimeout) {
			if (state.get() == State.RUNNING) {
				cancelTimeout();

				futureTimeout = scheduledExecutorService.schedule(this, heartbeatTimeout, TimeUnit.MILLISECONDS);

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
