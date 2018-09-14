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

package org.apache.flink.runtime.taskexecutor.slot;

import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Service to register timeouts for a given key. The timeouts are identified by a ticket so that
 * newly registered timeouts for the same key can be distinguished from older timeouts.
 *
 * @param <K> Type of the key
 */
public class TimerService<K> {

	private static final Logger LOG = LoggerFactory.getLogger(TimerService.class);

	/** Executor service for the scheduled timeouts. */
	private final ScheduledExecutorService scheduledExecutorService;

	/** Timeout for the shutdown of the service. */
	private final long shutdownTimeout;

	/** Map of currently active timeouts. */
	private final Map<K, Timeout<K>> timeouts;

	/** Listener which is notified about occurring timeouts. */
	private TimeoutListener<K> timeoutListener;

	public TimerService(
			final ScheduledExecutorService scheduledExecutorService,
			final long shutdownTimeout) {
		this.scheduledExecutorService = Preconditions.checkNotNull(scheduledExecutorService);

		Preconditions.checkArgument(shutdownTimeout >= 0L, "The shut down timeout must be larger than or equal than 0.");
		this.shutdownTimeout = shutdownTimeout;

		this.timeouts = new HashMap<>(16);
		this.timeoutListener = null;
	}

	public void start(TimeoutListener<K> initialTimeoutListener) {
		// sanity check; We only allow to assign a timeout listener once
		Preconditions.checkState(!scheduledExecutorService.isShutdown());
		Preconditions.checkState(timeoutListener == null);

		this.timeoutListener = Preconditions.checkNotNull(initialTimeoutListener);
	}

	public void stop() {
		unregisterAllTimeouts();

		timeoutListener = null;

		scheduledExecutorService.shutdown();

		try {
			if (!scheduledExecutorService.awaitTermination(shutdownTimeout, TimeUnit.MILLISECONDS)) {
				LOG.debug("The scheduled executor service did not properly terminate. Shutting " +
					"it down now.");
				scheduledExecutorService.shutdownNow();
			}
		} catch (InterruptedException e) {
			LOG.debug("Could not properly await the termination of the scheduled executor service.", e);
			scheduledExecutorService.shutdownNow();
		}
	}

	/**
	 * Register a timeout for the given key which shall occur in the given delay.
	 *
	 * @param key for which to register the timeout
	 * @param delay until the timeout
	 * @param unit of the timeout delay
	 */
	public void registerTimeout(final K key, final long delay, final TimeUnit unit) {
		Preconditions.checkState(timeoutListener != null, "The " + getClass().getSimpleName() +
			" has not been started.");

		if (timeouts.containsKey(key)) {
			unregisterTimeout(key);
		}

		timeouts.put(key, new Timeout<>(timeoutListener, key, delay, unit, scheduledExecutorService));
	}

	/**
	 * Unregister the timeout for the given key.
	 *
	 * @param key for which to unregister the timeout
	 */
	public void unregisterTimeout(K key) {
		Timeout<K> timeout = timeouts.remove(key);

		if (timeout != null) {
			timeout.cancel();
		}
	}

	/**
	 * Unregister all timeouts.
	 */
	protected void unregisterAllTimeouts() {
		for (Timeout<K> timeout : timeouts.values()) {
			timeout.cancel();
		}
		timeouts.clear();
	}

	/**
	 * Check whether the timeout for the given key and ticket is still valid (not yet unregistered
	 * and not yet overwritten).
	 *
	 * @param key for which to check the timeout
	 * @param ticket of the timeout
	 * @return True if the timeout ticket is still valid; otherwise false
	 */
	public boolean isValid(K key, UUID ticket) {
		if (timeouts.containsKey(key)) {
			Timeout<K> timeout = timeouts.get(key);

			return timeout.getTicket().equals(ticket);
		} else {
			return false;
		}
	}

	// ---------------------------------------------------------------------
	// Static utility classes
	// ---------------------------------------------------------------------

	private static final class Timeout<K> implements Runnable {

		private final TimeoutListener<K> timeoutListener;
		private final K key;
		private final ScheduledFuture<?> scheduledTimeout;
		private final UUID ticket;

		Timeout(
			final TimeoutListener<K> timeoutListener,
			final K key,
			final long delay,
			final TimeUnit unit,
			final ScheduledExecutorService scheduledExecutorService) {

			Preconditions.checkNotNull(scheduledExecutorService);

			this.timeoutListener = Preconditions.checkNotNull(timeoutListener);
			this.key = Preconditions.checkNotNull(key);
			this.scheduledTimeout = scheduledExecutorService.schedule(this, delay, unit);
			this.ticket = UUID.randomUUID();
		}

		UUID getTicket() {
			return ticket;
		}

		void cancel() {
			scheduledTimeout.cancel(true);
		}

		@Override
		public void run() {
			timeoutListener.notifyTimeout(key, ticket);
		}
	}
}
