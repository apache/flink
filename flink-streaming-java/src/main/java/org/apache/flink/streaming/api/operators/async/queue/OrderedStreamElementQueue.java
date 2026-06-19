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

package org.apache.flink.streaming.api.operators.async.queue;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;

/**
 * Ordered {@link StreamElementQueue} implementation. The ordered stream element queue provides
 * asynchronous results in the order in which the {@link StreamElementQueueEntry} have been added to
 * the queue. Thus, even if the completion order can be arbitrary, the output order strictly follows
 * the insertion order (element cannot overtake each other).
 */
@Internal
public final class OrderedStreamElementQueue<OUT> implements StreamElementQueue<OUT> {

    private static final Logger LOG = LoggerFactory.getLogger(OrderedStreamElementQueue.class);

    /** Capacity of this queue. */
    private final int capacity;

    /** Queue for the inserted StreamElementQueueEntries. */
    private final Queue<StreamElementQueueEntry<OUT>> queue;

    public OrderedStreamElementQueue(int capacity) {
        Preconditions.checkArgument(capacity > 0, "The capacity must be larger than 0.");

        this.capacity = capacity;
        this.queue = new ArrayDeque<>(capacity);
    }

    @Override
    public boolean hasCompletedElements() {
        return !queue.isEmpty() && queue.peek().isDone();
    }

    @Override
    public void emitCompletedElement(TimestampedCollector<OUT> output) {
        if (hasCompletedElements()) {
            final StreamElementQueueEntry<OUT> head = queue.poll();
            head.emitResult(output);
        }
    }

    @Override
    public List<StreamElement> values() {
        List<StreamElement> list = new ArrayList<>(this.queue.size());
        for (StreamElementQueueEntry e : queue) {
            list.add(e.getInputElement());
        }
        return list;
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
    public Optional<ResultFuture<OUT>> tryPut(StreamElement streamElement) {
        if (queue.size() < capacity) {
            StreamElementQueueEntry<OUT> queueEntry = createEntry(streamElement);

            queue.add(queueEntry);

            LOG.debug(
                    "Put element into ordered stream element queue. New filling degree "
                            + "({}/{}).",
                    queue.size(),
                    capacity);

            return Optional.of(queueEntry);
        } else {
            LOG.debug(
                    "Failed to put element into ordered stream element queue because it "
                            + "was full ({}/{}).",
                    queue.size(),
                    capacity);

            return Optional.empty();
        }
    }

    private StreamElementQueueEntry<OUT> createEntry(StreamElement streamElement) {
        if (streamElement.isRecord()) {
            return new StreamRecordQueueEntry<>((StreamRecord<?>) streamElement);
        }
        if (streamElement.isWatermark()) {
            return new WatermarkQueueEntry<>((Watermark) streamElement);
        }
        throw new UnsupportedOperationException("Cannot enqueue " + streamElement);
    }
}
