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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;

import java.io.IOException;
import java.util.Set;

/**
 * Interface for working with time and timers.
 *
 * <p>This is the internal version of {@link org.apache.flink.streaming.api.TimerService}
 * that allows to specify a key and a namespace to which timers should be scoped.
 *
 * @param <N> Type of the namespace to which timers are scoped.
 */
@Internal
public interface InternalTimerService<K, N> extends ProcessingTimeCallback {

	/**
	 * Starts the local {@link HeapInternalTimerService} by:
	 * <ol>
	 *     <li>Setting the {@code keySerialized} and {@code namespaceSerializer} for the timers it will contain.</li>
	 *     <li>Setting the {@code triggerTarget} which contains the action to be performed when a timer fires.</li>
	 *     <li>Re-registering timers that were retrieved after recovering from a node failure, if any.</li>
	 * </ol>
	 * This method can be called multiple times, as long as it is called with the same serializers.
	 */
	void startTimerService(
			TypeSerializer<K> keySerializer,
			TypeSerializer<N> namespaceSerializer,
			Triggerable<K, N> triggerTarget);

	/** Returns the current processing time. */
	long currentProcessingTime();

	/** Returns the current event-time watermark. */
	long currentWatermark();

	/** Advances the current event-time watermark. */
	void advanceWatermark(long timestamp) throws Exception;

	/**
	 * Registers a timer to be fired when processing time passes the given time. The namespace
	 * you pass here will be provided when the timer fires.
	 */
	void registerProcessingTimeTimer(N namespace, long time);

	/**
	 * Deletes the timer for the given key and namespace.
	 */
	void deleteProcessingTimeTimer(N namespace, long time);

	/**
	 * Registers a timer to be fired when event time watermark passes the given time. The namespace
	 * you pass here will be provided when the timer fires.
	 */
	void registerEventTimeTimer(N namespace, long time);

	/**
	 * Deletes the timer for the given key and namespace.
	 */
	void deleteEventTimeTimer(N namespace, long time);

	/**
	 * Snapshots the timers (both processing and event time ones) for a given {@code keyGroupIdx}.
	 *
	 * @param keyGroupIdx the id of the key-group to be put in the snapshot.
	 * @return a snapshot containing the timers for the given key-group, and the serializers for them
	 */
	InternalTimersSnapshot<K, N> snapshotTimersForKeyGroup(int keyGroupIdx) throws IOException;

	/**
	 * Restore the timers (both processing and event time ones) for a given {@code keyGroupIdx}.
	 *
	 * @param restoredTimersSnapshot the restored snapshot containing the key-group's timers,
	 *                       and the serializers that were used to write them
	 * @param keyGroupIdx the id of the key-group to be put in the snapshot.
	 */
	void restoreTimersForKeyGroup(InternalTimersSnapshot<K, N> restoredTimersSnapshot, int keyGroupIdx) throws IOException;

	/** Returns the key groups of the timers. */
	@VisibleForTesting
	KeyGroupRange getKeyGroupRange();

	/** Returns the number of processing-time timers. */
	@VisibleForTesting
	int numProcessingTimeTimers();

	/** Returns the number of event-time timers. */
	@VisibleForTesting
	int numEventTimeTimers();

	/** Returns the number of processing-time timers with the given namespace. */
	@VisibleForTesting
	int numProcessingTimeTimers(N namespace);

	/** Returns the number of event-time timers with the given namespace. */
	@VisibleForTesting
	int numEventTimeTimers(N namespace);

	/** Returns all the processing-time timers in the service. */
	Set<InternalTimer<K, N>>[] getProcessingTimeTimersPerKeyGroup();

	/** Returns all the event-time timers in the service. */
	Set<InternalTimer<K, N>>[] getEventTimeTimersPerKeyGroup();
}
