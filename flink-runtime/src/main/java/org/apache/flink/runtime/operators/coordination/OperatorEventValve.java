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

package org.apache.flink.runtime.operators.coordination;

import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.SerializedValue;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

/**
 * The event value is the connection through which operator events are sent, from coordinator to
 * operator.It can temporarily block events from going through, buffering them, and releasing them
 * later.
 *
 * <p>The valve can also drop buffered events for all or selected targets.
 *
 * <p>This class is fully thread safe, under the assumption that the event sender is thread-safe.
 */
public final class OperatorEventValve {

	private final Object lock = new Object();

	@GuardedBy("lock")
	private final BiFunction<SerializedValue<OperatorEvent>, Integer, CompletableFuture<Acknowledge>> eventSender;

	@GuardedBy("lock")
	private final Map<Integer, List<BlockedEvent>> blockedEvents = new LinkedHashMap<>();

	@GuardedBy("lock")
	private boolean shut;

	/**
	 * Constructs a new OperatorEventValve, passing the events to the given function when the valve is open
	 * or opened again. The second parameter of the BiFunction is the target operator subtask index.
	 */
	public OperatorEventValve(BiFunction<SerializedValue<OperatorEvent>, Integer, CompletableFuture<Acknowledge>> eventSender) {
		this.eventSender = eventSender;
	}

	// ------------------------------------------------------------------------

	public boolean isShut() {
		// synchronized block for visibility
		synchronized (lock) {
			return shut;
		}
	}

	/**
	 * Send the event directly, if the valve is open, and returns the original sending result future.
	 *
	 * <p>If the valve is closed this buffers the event and returns an incomplete future. The future is completed
	 * with the original result once the valve is opened. If the event is never sent (because it gets dropped
	 * through a call to {@link #reset()} or {@link #resetForTask(int)}, then the returned future till be
	 * completed exceptionally.
	 */
	public CompletableFuture<Acknowledge> sendEvent(SerializedValue<OperatorEvent> event, int subtask) {
		synchronized (lock) {
			if (!shut) {
				return eventSender.apply(event, subtask);
			}

			final List<BlockedEvent> eventsForTask = blockedEvents.computeIfAbsent(subtask, (key) -> new ArrayList<>());
			final CompletableFuture<Acknowledge> future = new CompletableFuture<>();
			eventsForTask.add(new BlockedEvent(event, subtask, future));
			return future;
		}
	}

	/**
	 * Shuts the value. All events sent through this valve are blocked until the valve is re-opened.
	 * If the valve is already shut, this does nothing.
	 */
	public void shutValve() {
		// synchronized block for visibility
		synchronized (lock) {
			shut = true;
		}
	}

	/**
	 * Opens the value, releasing all buffered events.
	 */
	public void openValve() {
		final ArrayList<FuturePair> futures;

		// send all events under lock, so that no new event can sneak between
		synchronized (lock) {
			if (!shut) {
				return;
			}

			futures = new ArrayList<>(blockedEvents.size());

			for (List<BlockedEvent> eventsForTask : blockedEvents.values()) {
				for (BlockedEvent blockedEvent : eventsForTask) {
					final CompletableFuture<Acknowledge> ackFuture = eventSender.apply(blockedEvent.event, blockedEvent.subtask);
					futures.add(new FuturePair(blockedEvent.future, ackFuture));
				}
			}
			blockedEvents.clear();
			shut = false;
		}

		// apply the logic on the future outside the lock, to be safe
		for (FuturePair pair : futures) {
			final CompletableFuture<Acknowledge> originalFuture = pair.originalFuture;
			pair.ackFuture.whenComplete((success, failure) -> {
				if (failure != null) {
					originalFuture.completeExceptionally(failure);
				} else {
					originalFuture.complete(success);
				}
			});
		}
	}

	/**
	 * Drops all blocked events for a specific subtask.
	 */
	public void resetForTask(int subtask) {
		final List<BlockedEvent> events;
		synchronized (lock) {
			events = blockedEvents.remove(subtask);
		}

		failAllFutures(events);
	}

	/**
	 * Resets the valve, dropping all blocked events and opening the valve.
	 */
	public void reset() {
		final List<BlockedEvent> events = new ArrayList<>();
		synchronized (lock) {
			for (List<BlockedEvent> taskEvents : blockedEvents.values()) {
				if (taskEvents != null) {
					events.addAll(taskEvents);
				}
			}
			blockedEvents.clear();
			shut = false;
		}

		failAllFutures(events);
	}

	private static void failAllFutures(@Nullable List<BlockedEvent> events) {
		if (events == null || events.isEmpty()) {
			return;
		}

		final Exception failureCause = new FlinkException("Event discarded due to failure of target task");
		for (BlockedEvent evt : events) {
			evt.future.completeExceptionally(failureCause);
		}
	}

	// ------------------------------------------------------------------------

	private static final class BlockedEvent {

		final SerializedValue<OperatorEvent> event;
		final CompletableFuture<Acknowledge> future;
		final int subtask;

		BlockedEvent(SerializedValue<OperatorEvent> event, int subtask, CompletableFuture<Acknowledge> future) {
			this.event = event;
			this.future = future;
			this.subtask = subtask;
		}
	}

	private static final class FuturePair {

		final CompletableFuture<Acknowledge> originalFuture;
		final CompletableFuture<Acknowledge> ackFuture;

		FuturePair(CompletableFuture<Acknowledge> originalFuture, CompletableFuture<Acknowledge> ackFuture) {
			this.originalFuture = originalFuture;
			this.ackFuture = ackFuture;
		}
	}
}
