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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.runtime.state.InternalPriorityQueue;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.BiConsumerWithException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** {@link InternalTimerService} that stores timers on the Java heap. */
public class InternalTimerServiceImpl<K, N> implements InternalTimerService<N> {

    private final ProcessingTimeService processingTimeService;

    private final KeyContext keyContext;

    /** Processing time timers that are currently in-flight. */
    private final KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<K, N>>
            processingTimeTimersQueue;

    /** Event time timers that are currently in-flight. */
    private final KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<K, N>>
            eventTimeTimersQueue;

    /** Information concerning the local key-group range. */
    private final KeyGroupRange localKeyGroupRange;

    private final int localKeyGroupRangeStartIdx;

    /**
     * The local event time, as denoted by the last received {@link
     * org.apache.flink.streaming.api.watermark.Watermark Watermark}.
     */
    private long currentWatermark = Long.MIN_VALUE;

    /**
     * The one and only Future (if any) registered to execute the next {@link Triggerable} action,
     * when its (processing) time arrives.
     */
    private ScheduledFuture<?> nextTimer;

    // Variables to be set when the service is started.

    private TypeSerializer<K> keySerializer;

    private TypeSerializer<N> namespaceSerializer;

    private Triggerable<K, N> triggerTarget;

    private volatile boolean isInitialized;

    private TypeSerializer<K> keyDeserializer;

    private TypeSerializer<N> namespaceDeserializer;

    /** The restored timers snapshot, if any. */
    private InternalTimersSnapshot<K, N> restoredTimersSnapshot;

    InternalTimerServiceImpl(
            KeyGroupRange localKeyGroupRange,
            KeyContext keyContext,
            ProcessingTimeService processingTimeService,
            KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<K, N>> processingTimeTimersQueue,
            KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<K, N>> eventTimeTimersQueue) {

        this.keyContext = checkNotNull(keyContext);
        this.processingTimeService = checkNotNull(processingTimeService);
        this.localKeyGroupRange = checkNotNull(localKeyGroupRange);
        this.processingTimeTimersQueue = checkNotNull(processingTimeTimersQueue);
        this.eventTimeTimersQueue = checkNotNull(eventTimeTimersQueue);

        // find the starting index of the local key-group range
        int startIdx = Integer.MAX_VALUE;
        for (Integer keyGroupIdx : localKeyGroupRange) {
            startIdx = Math.min(keyGroupIdx, startIdx);
        }
        this.localKeyGroupRangeStartIdx = startIdx;
    }

    /**
     * Starts the local {@link InternalTimerServiceImpl} by:
     *
     * <ol>
     *   <li>Setting the {@code keySerialized} and {@code namespaceSerializer} for the timers it
     *       will contain.
     *   <li>Setting the {@code triggerTarget} which contains the action to be performed when a
     *       timer fires.
     *   <li>Re-registering timers that were retrieved after recovering from a node failure, if any.
     * </ol>
     *
     * <p>This method can be called multiple times, as long as it is called with the same
     * serializers.
     */
    public void startTimerService(
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            Triggerable<K, N> triggerTarget) {

        if (!isInitialized) {

            if (keySerializer == null || namespaceSerializer == null) {
                throw new IllegalArgumentException("The TimersService serializers cannot be null.");
            }

            if (this.keySerializer != null
                    || this.namespaceSerializer != null
                    || this.triggerTarget != null) {
                throw new IllegalStateException("The TimerService has already been initialized.");
            }

            // the following is the case where we restore
            if (restoredTimersSnapshot != null) {
                TypeSerializerSchemaCompatibility<K> keySerializerCompatibility =
                        restoredTimersSnapshot
                                .getKeySerializerSnapshot()
                                .resolveSchemaCompatibility(keySerializer);

                if (keySerializerCompatibility.isIncompatible()
                        || keySerializerCompatibility.isCompatibleAfterMigration()) {
                    throw new IllegalStateException(
                            "Tried to initialize restored TimerService with new key serializer that requires migration or is incompatible.");
                }

                TypeSerializerSchemaCompatibility<N> namespaceSerializerCompatibility =
                        restoredTimersSnapshot
                                .getNamespaceSerializerSnapshot()
                                .resolveSchemaCompatibility(namespaceSerializer);

                restoredTimersSnapshot = null;

                if (namespaceSerializerCompatibility.isIncompatible()
                        || namespaceSerializerCompatibility.isCompatibleAfterMigration()) {
                    throw new IllegalStateException(
                            "Tried to initialize restored TimerService with new namespace serializer that requires migration or is incompatible.");
                }

                this.keySerializer =
                        keySerializerCompatibility.isCompatibleAsIs()
                                ? keySerializer
                                : keySerializerCompatibility.getReconfiguredSerializer();
                this.namespaceSerializer =
                        namespaceSerializerCompatibility.isCompatibleAsIs()
                                ? namespaceSerializer
                                : namespaceSerializerCompatibility.getReconfiguredSerializer();
            } else {
                this.keySerializer = keySerializer;
                this.namespaceSerializer = namespaceSerializer;
            }

            this.keyDeserializer = null;
            this.namespaceDeserializer = null;

            this.triggerTarget = Preconditions.checkNotNull(triggerTarget);

            // re-register the restored timers (if any)
            final InternalTimer<K, N> headTimer = processingTimeTimersQueue.peek();
            if (headTimer != null) {
                nextTimer =
                        processingTimeService.registerTimer(
                                headTimer.getTimestamp(), this::onProcessingTime);
            }
            this.isInitialized = true;
        } else {
            if (!(this.keySerializer.equals(keySerializer)
                    && this.namespaceSerializer.equals(namespaceSerializer))) {
                throw new IllegalArgumentException(
                        "Already initialized Timer Service "
                                + "tried to be initialized with different key and namespace serializers.");
            }
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

    @Override
    public void registerProcessingTimeTimer(N namespace, long time) {
        InternalTimer<K, N> oldHead = processingTimeTimersQueue.peek();
        if (processingTimeTimersQueue.add(
                new TimerHeapInternalTimer<>(time, (K) keyContext.getCurrentKey(), namespace))) {
            long nextTriggerTime = oldHead != null ? oldHead.getTimestamp() : Long.MAX_VALUE;
            // check if we need to re-schedule our timer to earlier
            if (time < nextTriggerTime) {
                if (nextTimer != null) {
                    nextTimer.cancel(false);
                }
                nextTimer = processingTimeService.registerTimer(time, this::onProcessingTime);
            }
        }
    }

    @Override
    public void registerEventTimeTimer(N namespace, long time) {
        eventTimeTimersQueue.add(
                new TimerHeapInternalTimer<>(time, (K) keyContext.getCurrentKey(), namespace));
    }

    @Override
    public void deleteProcessingTimeTimer(N namespace, long time) {
        processingTimeTimersQueue.remove(
                new TimerHeapInternalTimer<>(time, (K) keyContext.getCurrentKey(), namespace));
    }

    @Override
    public void deleteEventTimeTimer(N namespace, long time) {
        eventTimeTimersQueue.remove(
                new TimerHeapInternalTimer<>(time, (K) keyContext.getCurrentKey(), namespace));
    }

    @Override
    public void forEachEventTimeTimer(BiConsumerWithException<N, Long, Exception> consumer)
            throws Exception {
        foreachTimer(consumer, eventTimeTimersQueue);
    }

    @Override
    public void forEachProcessingTimeTimer(BiConsumerWithException<N, Long, Exception> consumer)
            throws Exception {
        foreachTimer(consumer, processingTimeTimersQueue);
    }

    private void foreachTimer(
            BiConsumerWithException<N, Long, Exception> consumer,
            KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<K, N>> queue)
            throws Exception {
        try (final CloseableIterator<TimerHeapInternalTimer<K, N>> iterator = queue.iterator()) {
            while (iterator.hasNext()) {
                final TimerHeapInternalTimer<K, N> timer = iterator.next();
                keyContext.setCurrentKey(timer.getKey());
                consumer.accept(timer.getNamespace(), timer.getTimestamp());
            }
        }
    }

    private void onProcessingTime(long time) throws Exception {
        // null out the timer in case the Triggerable calls registerProcessingTimeTimer()
        // inside the callback.
        nextTimer = null;

        InternalTimer<K, N> timer;

        while ((timer = processingTimeTimersQueue.peek()) != null && timer.getTimestamp() <= time) {
            processingTimeTimersQueue.poll();
            keyContext.setCurrentKey(timer.getKey());
            triggerTarget.onProcessingTime(timer);
        }

        if (timer != null && nextTimer == null) {
            nextTimer =
                    processingTimeService.registerTimer(
                            timer.getTimestamp(), this::onProcessingTime);
        }
    }

    public void advanceWatermark(long time) throws Exception {
        currentWatermark = time;

        InternalTimer<K, N> timer;

        while ((timer = eventTimeTimersQueue.peek()) != null && timer.getTimestamp() <= time) {
            eventTimeTimersQueue.poll();
            keyContext.setCurrentKey(timer.getKey());
            triggerTarget.onEventTime(timer);
        }
    }

    /**
     * Snapshots the timers (both processing and event time ones) for a given {@code keyGroupIdx}.
     *
     * @param keyGroupIdx the id of the key-group to be put in the snapshot.
     * @return a snapshot containing the timers for the given key-group, and the serializers for
     *     them
     */
    public InternalTimersSnapshot<K, N> snapshotTimersForKeyGroup(int keyGroupIdx) {
        return new InternalTimersSnapshot<>(
                keySerializer,
                namespaceSerializer,
                eventTimeTimersQueue.getSubsetForKeyGroup(keyGroupIdx),
                processingTimeTimersQueue.getSubsetForKeyGroup(keyGroupIdx));
    }

    public TypeSerializer<K> getKeySerializer() {
        return keySerializer;
    }

    public TypeSerializer<N> getNamespaceSerializer() {
        return namespaceSerializer;
    }

    /**
     * Restore the timers (both processing and event time ones) for a given {@code keyGroupIdx}.
     *
     * @param restoredSnapshot the restored snapshot containing the key-group's timers, and the
     *     serializers that were used to write them
     * @param keyGroupIdx the id of the key-group to be put in the snapshot.
     */
    @SuppressWarnings("unchecked")
    public void restoreTimersForKeyGroup(
            InternalTimersSnapshot<?, ?> restoredSnapshot, int keyGroupIdx) {
        this.restoredTimersSnapshot = (InternalTimersSnapshot<K, N>) restoredSnapshot;

        TypeSerializer<K> restoredKeySerializer =
                restoredTimersSnapshot.getKeySerializerSnapshot().restoreSerializer();
        if (this.keyDeserializer != null && !this.keyDeserializer.equals(restoredKeySerializer)) {
            throw new IllegalArgumentException(
                    "Tried to restore timers for the same service with different key serializers.");
        }
        this.keyDeserializer = restoredKeySerializer;

        TypeSerializer<N> restoredNamespaceSerializer =
                restoredTimersSnapshot.getNamespaceSerializerSnapshot().restoreSerializer();
        if (this.namespaceDeserializer != null
                && !this.namespaceDeserializer.equals(restoredNamespaceSerializer)) {
            throw new IllegalArgumentException(
                    "Tried to restore timers for the same service with different namespace serializers.");
        }
        this.namespaceDeserializer = restoredNamespaceSerializer;

        checkArgument(
                localKeyGroupRange.contains(keyGroupIdx),
                "Key Group " + keyGroupIdx + " does not belong to the local range.");

        // restore the event time timers
        eventTimeTimersQueue.addAll(restoredTimersSnapshot.getEventTimeTimers());

        // restore the processing time timers
        processingTimeTimersQueue.addAll(restoredTimersSnapshot.getProcessingTimeTimers());
    }

    @VisibleForTesting
    public int numProcessingTimeTimers() {
        return this.processingTimeTimersQueue.size();
    }

    @VisibleForTesting
    public int numEventTimeTimers() {
        return this.eventTimeTimersQueue.size();
    }

    @VisibleForTesting
    public int numProcessingTimeTimers(N namespace) {
        return countTimersInNamespaceInternal(namespace, processingTimeTimersQueue);
    }

    @VisibleForTesting
    public int numEventTimeTimers(N namespace) {
        return countTimersInNamespaceInternal(namespace, eventTimeTimersQueue);
    }

    private int countTimersInNamespaceInternal(
            N namespace, InternalPriorityQueue<TimerHeapInternalTimer<K, N>> queue) {
        int count = 0;
        try (final CloseableIterator<TimerHeapInternalTimer<K, N>> iterator = queue.iterator()) {
            while (iterator.hasNext()) {
                final TimerHeapInternalTimer<K, N> timer = iterator.next();
                if (timer.getNamespace().equals(namespace)) {
                    count++;
                }
            }
        } catch (Exception e) {
            throw new FlinkRuntimeException("Exception when closing iterator.", e);
        }
        return count;
    }

    @VisibleForTesting
    int getLocalKeyGroupRangeStartIdx() {
        return this.localKeyGroupRangeStartIdx;
    }

    @VisibleForTesting
    List<Set<TimerHeapInternalTimer<K, N>>> getEventTimeTimersPerKeyGroup() {
        return partitionElementsByKeyGroup(eventTimeTimersQueue);
    }

    @VisibleForTesting
    List<Set<TimerHeapInternalTimer<K, N>>> getProcessingTimeTimersPerKeyGroup() {
        return partitionElementsByKeyGroup(processingTimeTimersQueue);
    }

    private <T> List<Set<T>> partitionElementsByKeyGroup(
            KeyGroupedInternalPriorityQueue<T> keyGroupedQueue) {
        List<Set<T>> result = new ArrayList<>(localKeyGroupRange.getNumberOfKeyGroups());
        for (int keyGroup : localKeyGroupRange) {
            result.add(Collections.unmodifiableSet(keyGroupedQueue.getSubsetForKeyGroup(keyGroup)));
        }
        return result;
    }
}
