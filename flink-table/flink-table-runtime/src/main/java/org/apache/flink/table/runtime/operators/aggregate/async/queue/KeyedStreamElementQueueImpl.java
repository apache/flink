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

package org.apache.flink.table.runtime.operators.aggregate.async.queue;

import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;
import java.util.function.Supplier;

/**
 * Uses completion time to determine when to output results. Note that watermarks are output
 * immediately after being added since it's assumed that any timers have been processed and outputs
 * made by the time the watermark is added to this queue and potentially outputted.
 *
 * @param <OUT>
 */
public class KeyedStreamElementQueueImpl<K, OUT> implements KeyedStreamElementQueue<K, OUT> {

    private static final Logger LOG = LoggerFactory.getLogger(KeyedStreamElementQueueImpl.class);

    /** Queue for the inserted KeyedStreamElementQueueEntries. */
    private final TreeSet<CreationOrderEntry<K, OUT>> queue;

    /** Maximum active request capacity of the queue. */
    private final int activeCapacity;

    /** Flag indicating whether to wait for watermarks before emitting elements. */
    private final boolean waitForWatermark;

    /** Counter for the creation order of the StreamElementQueueEntries. */
    private long creationCount = 0;

    /** Number of active (in-flight) requests in the queue. */
    private int active = 0;

    /** Number of watermarks in the queue. */
    private int watermarksInQueue = 0;

    private KeyedStreamElementQueueImpl(
            int activeCapacity,
            Comparator<CreationOrderEntry<K, OUT>> comparator,
            boolean waitForWatermark) {
        this.activeCapacity = activeCapacity;
        this.queue = new TreeSet<>(comparator);
        this.waitForWatermark = waitForWatermark;
    }

    /**
     * Creates a new {@link KeyedStreamElementQueue} that uses the order of the input elements to
     * determine the order of output.
     *
     * @param activeCapacity The maximum number of in-flight requests that can be made at once.
     * @param <K> The type of the key.
     * @param <OUT> The type of the output elements.
     * @return A new {@link KeyedStreamElementQueue} that uses the order of the input elements to
     *     determine the order of the elements.
     */
    public static <K, OUT> KeyedStreamElementQueue<K, OUT> createOrderedQueue(int activeCapacity) {
        return new KeyedStreamElementQueueImpl<>(
                activeCapacity,
                Comparator.comparingLong(CreationOrderEntry::getCreationOrder),
                false);
    }

    @Override
    public boolean hasCompletedElements() {
        return !queue.isEmpty()
                && queue.first().isDone()
                && (!waitForWatermark || watermarksInQueue > 0);
    }

    @Override
    public void emitCompletedElement(TimestampedCollector<OUT> output) {
        if (hasCompletedElements()) {
            KeyedStreamElementQueueEntry<K, OUT> element = queue.pollFirst();
            if (element != null) {
                if (element.getInputElement().isWatermark()) {
                    watermarksInQueue--;
                }
                element.emitResult(output);
            }
        }
    }

    @Override
    public Map<K, List<StreamElement>> valuesByKey() {
        Map<K, List<StreamElement>> map = new HashMap<>(this.queue.size());
        for (KeyedStreamElementQueueEntry<K, OUT> e : queue) {
            if (e.getInputElement().isWatermark()) {
                continue;
            }
            List<StreamElement> list;
            if (!map.containsKey(e.getKey())) {
                list = new ArrayList<>();
                map.put(e.getKey(), list);
            } else {
                list = map.get(e.getKey());
            }
            list.add(e.getInputElement());
        }
        return map;
    }

    @Override
    public boolean isEmpty() {
        return queue.isEmpty();
    }

    @Override
    public int size() {
        return queue.size();
    }

    @Override
    public Optional<ResultFuture<OUT>> tryPut(K key, StreamElement streamElement) {
        // Watermarks are always put into the queue and don't count towards the capacity.
        if (active < activeCapacity || streamElement.isWatermark()) {
            CreationOrderEntry<K, OUT> queueEntry = createEntry(key, streamElement);
            queue.add(queueEntry);

            LOG.debug(
                    "Put element into ordered stream element queue. New filling degree "
                            + "({}/{}).",
                    queue.size(),
                    activeCapacity);

            if (streamElement.isRecord()) {
                active++;
            } else {
                watermarksInQueue++;
            }
            return Optional.of(queueEntry);
        } else {
            LOG.debug(
                    "Failed to put element into ordered stream element queue because it "
                            + "was full ({}/{}).",
                    queue.size(),
                    activeCapacity);

            return Optional.empty();
        }
    }

    private CreationOrderEntry<K, OUT> createEntry(K key, StreamElement streamElement) {
        if (streamElement.isRecord()) {
            return new CompletionTimeStreamElementEntry<>(
                    key, (StreamRecord<?>) streamElement, () -> creationCount++, () -> active--);
        }
        if (streamElement.isWatermark()) {
            return new CompletionTimeWatermarkEntry<>(
                    (Watermark) streamElement, () -> creationCount++);
        }
        throw new UnsupportedOperationException("Cannot enqueue <redacted>");
    }

    static class CompletionTimeStreamElementEntry<K, OUT> implements CreationOrderEntry<K, OUT> {

        private final K key;

        private final StreamRecord<?> inputRecord;

        private Collection<OUT> completedElements;

        private final long creationOrder;

        private final Runnable onCompletion;

        CompletionTimeStreamElementEntry(
                K key,
                StreamRecord<?> inputRecord,
                Supplier<Long> creationOrderSupplier,
                Runnable onCompletion) {
            this.key = key;
            this.inputRecord = inputRecord;
            this.creationOrder = creationOrderSupplier.get();
            this.onCompletion = onCompletion;
        }

        @Override
        public boolean isDone() {
            return completedElements != null;
        }

        @Nonnull
        @Override
        public StreamRecord<?> getInputElement() {
            return inputRecord;
        }

        @Override
        public void emitResult(TimestampedCollector<OUT> output) {
            output.setTimestamp(inputRecord);
            for (OUT r : completedElements) {
                output.collect(r);
            }
        }

        @Override
        public void complete(Collection<OUT> result) {
            this.completedElements = Preconditions.checkNotNull(result);
            onCompletion.run();
        }

        @Override
        public K getKey() {
            return key;
        }

        @Override
        public long getCreationOrder() {
            return creationOrder;
        }
    }

    static class CompletionTimeWatermarkEntry<K, OUT> implements CreationOrderEntry<K, OUT> {

        @Nonnull private final Watermark watermark;

        private final long creationOrder;

        public CompletionTimeWatermarkEntry(
                Watermark watermark, Supplier<Long> creationOrderSupplier) {
            this.watermark = Preconditions.checkNotNull(watermark);
            this.creationOrder = creationOrderSupplier.get();
        }

        @Override
        public long getCreationOrder() {
            return creationOrder;
        }

        @Override
        public void emitResult(TimestampedCollector<OUT> output) {
            output.emitWatermark(watermark);
        }

        @Nonnull
        @Override
        public Watermark getInputElement() {
            return watermark;
        }

        @Override
        public K getKey() {
            return null;
        }

        @Override
        public boolean isDone() {
            return true;
        }

        @Override
        public void complete(Collection<OUT> result) {
            throw new IllegalStateException("Cannot complete a watermark.");
        }
    }

    private interface CreationOrderEntry<K, OUT> extends KeyedStreamElementQueueEntry<K, OUT> {
        long getCreationOrder();
    }
}
