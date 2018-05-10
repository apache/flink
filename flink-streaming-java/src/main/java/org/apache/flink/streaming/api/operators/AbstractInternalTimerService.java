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

import org.apache.flink.api.common.typeutils.CompatibilityResult;
import org.apache.flink.api.common.typeutils.CompatibilityUtil;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * The base implementation of {@link InternalTimerService}.
 *
 * @param <K> The type of the keys in the timers.
 * @param <N> The type of the namespaces in the timers.
 */
public abstract class AbstractInternalTimerService<K, N> implements InternalTimerService<K, N> {

	/** The name of the service. */
	protected final String serviceName;

	/** The total number of key groups. */
	protected final int totalKeyGroups;

	/** The key groups assigned to the partition. */
	protected final KeyGroupRange localKeyGroupRange;

	/** The context information of the keys. */
	protected final KeyContext keyContext;

	/** The registry for processing-time timers. */
	protected final ProcessingTimeService processingTimeService;

	/** The serializer for the keys in the timers. */
	protected final TypeSerializer<K> keySerializer;

	/** The serializer for the namespaces in the timers. */
	protected final TypeSerializer<N> namespaceSerializer;

	/** The target to execute when timers are triggered. */
	protected Triggerable<K, N> triggerTarget;

	/** The restored timers snapshot, if any. */
	private InternalTimersSnapshot<K, N> restoredTimersSnapshot;

	/** True if the timer service is already initialized. */
	private volatile boolean isInitialized;

	/**
	 * The one and only Future (if any) registered to execute the
	 * next {@link Triggerable} action, when its (processing) time arrives.
	 * */
	protected ScheduledFuture<?> nextTimer;

	/**
	 * The local event time, as denoted by the last received
	 * {@link org.apache.flink.streaming.api.watermark.Watermark Watermark}.
	 */
	protected long currentWatermark = Long.MIN_VALUE;

	protected AbstractInternalTimerService(
			String serviceName,
			int totalKeyGroups,
			KeyGroupRange localKeyGroupRange,
			TypeSerializer<K> keySerializer,
			TypeSerializer<N> namespaceSerializer,
			KeyContext keyContext,
			ProcessingTimeService processingTimeService) {

		this.serviceName = Preconditions.checkNotNull(serviceName);
		this.totalKeyGroups = totalKeyGroups;
		this.localKeyGroupRange = Preconditions.checkNotNull(localKeyGroupRange);
		this.keySerializer = Preconditions.checkNotNull(keySerializer);
		this.namespaceSerializer = Preconditions.checkNotNull(namespaceSerializer);
		this.keyContext = Preconditions.checkNotNull(keyContext);
		this.processingTimeService = Preconditions.checkNotNull(processingTimeService);
	}

	/**
	 * Retrieve the first processing-time timer in the timer service.. Null if
	 * there does not exist any processing-time timer.
	 *
	 * @return The first processing-time timer in the timer service.
	 */
	protected abstract InternalTimer<K, N> getHeadProcessingTimeTimer();

	/**
	 * Retrieve the set of processing-time timers for the requested key-group.
	 *
	 * @param keyGroupIdx the index of the key group we are interested in.
	 * @return the set of registered timers for the key-group.
	 */
	protected abstract Set<InternalTimer<K, N>> getProcessingTimeTimerSetForKeyGroup(int keyGroupIdx);

	/**
	 * Retrieve the set of event-time timers for the requested key-group.
	 *
	 * @param keyGroupIdx the index of the key group we are interested in.
	 * @return the set of registered timers for the key-group.
	 */
	protected abstract Set<InternalTimer<K, N>> getEventTimeTimerSetForKeyGroup(int keyGroupIdx);

	/**
	 * Adds the set of processing-time timers in to the given key-group.
	 *
	 * @param keyGroupIdx the index of the key group into which the timers are added.
	 * @param timers The timers to be added.
	 */
	protected abstract void addProcessingTimeTimerSetForKeyGroup(int keyGroupIdx, Set<InternalTimer<K, N>> timers);

	/**
	 * Adds the set of event-time timers in to the given key-group.
	 *
	 * @param keyGroupIdx the index of the key group into which the timers are added.
	 * @param timers The timers to be added.
	 */
	protected abstract void addEventTimeTimerSetForKeyGroup(int keyGroupIdx, Set<InternalTimer<K, N>> timers);

	@Override
	public void startTimerService(
			TypeSerializer<K> keySerializer,
			TypeSerializer<N> namespaceSerializer,
			Triggerable<K, N> triggerTarget) {

		if (keySerializer == null || namespaceSerializer == null) {
			throw new IllegalArgumentException("The TimersService serializers cannot be null.");
		}

		if (restoredTimersSnapshot != null) {
			CompatibilityResult<K> keySerializerCompatibility = CompatibilityUtil.resolveCompatibilityResult(
					this.keySerializer,
					null,
					restoredTimersSnapshot.getKeySerializerConfigSnapshot(),
					keySerializer);

			CompatibilityResult<N> namespaceSerializerCompatibility = CompatibilityUtil.resolveCompatibilityResult(
					this.namespaceSerializer,
					null,
					restoredTimersSnapshot.getNamespaceSerializerConfigSnapshot(),
					namespaceSerializer);

			if (keySerializerCompatibility.isRequiresMigration() ||
				namespaceSerializerCompatibility.isRequiresMigration()) {
				throw new IllegalStateException(
					"Tried to initialize restored TimerService " +
						"with incompatible serializers than those used to snapshot its state.");
			}
		} else {
			if (!(this.keySerializer.equals(keySerializer) && this.namespaceSerializer.equals(namespaceSerializer))) {
				throw new IllegalArgumentException("Already initialized Timer Service " +
					"tried to be initialized with different key and namespace serializers.");
			}
		}

		if (!isInitialized) {

			this.triggerTarget = Preconditions.checkNotNull(triggerTarget);

			InternalTimer<K, N> headProcessingTimeTimer = getHeadProcessingTimeTimer();
			if (headProcessingTimeTimer != null) {
				nextTimer = processingTimeService.registerTimer(headProcessingTimeTimer.getTimestamp(), this);
			}

			this.isInitialized = true;
		}
	}

	@Override
	public long currentProcessingTime() {
		return processingTimeService.getCurrentProcessingTime();
	}

	@Override
	public long currentWatermark() {
		return currentWatermark;
	}

	/**
	 * Snapshots the timers (both processing and event time ones) for a given {@code keyGroupIdx}.
	 *
	 * @param keyGroupIdx the id of the key-group to be put in the snapshot.
	 * @return a snapshot containing the timers for the given key-group, and the serializers for them
	 */
	public InternalTimersSnapshot<K, N> snapshotTimersForKeyGroup(int keyGroupIdx) {
		return new InternalTimersSnapshot<>(
				keySerializer,
				keySerializer.snapshotConfiguration(),
				namespaceSerializer,
				namespaceSerializer.snapshotConfiguration(),
				getEventTimeTimerSetForKeyGroup(keyGroupIdx),
				getProcessingTimeTimerSetForKeyGroup(keyGroupIdx));
	}

	/**
	 * Restore the timers (both processing and event time ones) for a given {@code keyGroupIdx}.
	 *
	 * @param restoredTimersSnapshot the restored snapshot containing the key-group's timers,
	 *                       and the serializers that were used to write them
	 * @param keyGroupIdx the id of the key-group to be put in the snapshot.
	 */
	@SuppressWarnings("unchecked")
	public void restoreTimersForKeyGroup(InternalTimersSnapshot<K, N> restoredTimersSnapshot, int keyGroupIdx) throws IOException {
		this.restoredTimersSnapshot = restoredTimersSnapshot;

		if ((this.keySerializer != null && !this.keySerializer.equals(restoredTimersSnapshot.getKeySerializer())) ||
			(this.namespaceSerializer != null && !this.namespaceSerializer.equals(restoredTimersSnapshot.getNamespaceSerializer()))) {

			throw new IllegalArgumentException("Tried to restore timers " +
				"for the same service with different serializers.");
		}

		checkArgument(localKeyGroupRange.contains(keyGroupIdx),
			"Key Group " + keyGroupIdx + " does not belong to the local range.");

		// restore the event time timers
		addEventTimeTimerSetForKeyGroup(keyGroupIdx, this.restoredTimersSnapshot.getEventTimeTimers());

		// restore the processing time timers
		addProcessingTimeTimerSetForKeyGroup(keyGroupIdx, this.restoredTimersSnapshot.getProcessingTimeTimers());
	}
}

