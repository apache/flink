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
import java.util.Collection;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;

/**
 * Unordered implementation of the {@link StreamElementQueue}. The unordered stream element queue provides
 * asynchronous results as soon as they are completed. Additionally, it maintains the watermark-stream record order.
 *
 * <p>Elements can be logically grouped into different segments separated by watermarks. A segment needs to be
 * completely emitted before entries from a following segment are emitted. Thus, no stream record can be overtaken
 * by a watermark and no watermark can overtake a stream record.
 * However, stream records falling in the same segment between two watermarks can overtake each other (their emission
 * order is not guaranteed).
 */
@Internal
public final class UnorderedStreamElementQueue<OUT> implements StreamElementQueue<OUT> {

	private static final Logger LOG = LoggerFactory.getLogger(UnorderedStreamElementQueue.class);

	/** Capacity of this queue. */
	private final int capacity;

	/** Queue of queue entries segmented by watermarks. */
	private final Deque<Segment<OUT>> segments;

	private int numberOfEntries;

	public UnorderedStreamElementQueue(int capacity) {
		Preconditions.checkArgument(capacity > 0, "The capacity must be larger than 0.");

		this.capacity = capacity;
		// most likely scenario are 4 segments <elements, watermark, elements, watermark>
		this.segments = new ArrayDeque<>(4);
		this.numberOfEntries = 0;
	}

	@Override
	public Optional<ResultFuture<OUT>> tryPut(StreamElement streamElement) {
		if (size() < capacity) {
			StreamElementQueueEntry<OUT> queueEntry;
			if (streamElement.isRecord()) {
				queueEntry = addRecord((StreamRecord<?>) streamElement);
			} else if (streamElement.isWatermark()) {
				queueEntry = addWatermark((Watermark) streamElement);
			} else {
				throw new UnsupportedOperationException("Cannot enqueue " + streamElement);
			}

			numberOfEntries++;

			LOG.debug("Put element into unordered stream element queue. New filling degree " +
					"({}/{}).", size(), capacity);

			return Optional.of(queueEntry);
		} else {
			LOG.debug("Failed to put element into unordered stream element queue because it " +
					"was full ({}/{}).", size(), capacity);

			return Optional.empty();
		}
	}

	private StreamElementQueueEntry<OUT> addRecord(StreamRecord<?> record) {
		// ensure that there is at least one segment
		Segment<OUT> lastSegment;
		if (segments.isEmpty()) {
			lastSegment = addSegment(capacity);
		} else {
			lastSegment = segments.getLast();
		}

		// entry is bound to segment to notify it easily upon completion
		StreamElementQueueEntry<OUT> queueEntry = new SegmentedStreamRecordQueueEntry<>(record, lastSegment);
		lastSegment.add(queueEntry);
		return queueEntry;
	}

	private Segment<OUT> addSegment(int capacity) {
		Segment newSegment = new Segment(capacity);
		segments.addLast(newSegment);
		return newSegment;
	}

	private StreamElementQueueEntry<OUT> addWatermark(Watermark watermark) {
		Segment<OUT> watermarkSegment;
		if (!segments.isEmpty() && segments.getLast().isEmpty()) {
			// reuse already existing segment if possible (completely drained) or the new segment added at the end of
			// this method for two succeeding watermarks
			watermarkSegment = segments.getLast();
		} else {
			watermarkSegment = addSegment(1);
		}

		StreamElementQueueEntry<OUT> watermarkEntry = new WatermarkQueueEntry<>(watermark);
		watermarkSegment.add(watermarkEntry);

		// add a new segment for actual elements
		addSegment(capacity);
		return watermarkEntry;
	}

	@Override
	public boolean hasCompletedElements() {
		return !this.segments.isEmpty() && this.segments.getFirst().hasCompleted();
	}

	@Override
	public void emitCompletedElement(TimestampedCollector<OUT> output) {
		if (segments.isEmpty()) {
			return;
		}
		final Segment currentSegment = segments.getFirst();
		numberOfEntries -= currentSegment.emitCompleted(output);

		// remove any segment if there are further segments, if not leave it as an optimization even if empty
		if (segments.size() > 1 && currentSegment.isEmpty()) {
			segments.pop();
		}
	}

	@Override
	public List<StreamElement> values() {
		List<StreamElement> list = new ArrayList<>();
		for (Segment s : segments) {
			s.addPendingElements(list);
		}
		return list;
	}

	@Override
	public boolean isEmpty() {
		return numberOfEntries == 0;
	}

	@Override
	public int size() {
		return numberOfEntries;
	}

	/**
	 * An entry that notifies the respective segment upon completion.
	 */
	static class SegmentedStreamRecordQueueEntry<OUT> extends StreamRecordQueueEntry<OUT> {
		private final Segment<OUT> segment;

		SegmentedStreamRecordQueueEntry(StreamRecord<?> inputRecord, Segment<OUT> segment) {
			super(inputRecord);
			this.segment = segment;
		}

		@Override
		public void complete(Collection<OUT> result) {
			super.complete(result);
			segment.completed(this);
		}
	}

	/**
	 * A segment is a collection of queue entries that can be completed in arbitrary order.
	 *
	 * <p>All elements from one segment must be emitted before any element of the next segment is emitted.
	 */
	static class Segment<OUT> {
		/** Unfinished input elements. */
		private final Set<StreamElementQueueEntry<OUT>> incompleteElements;

		/** Undrained finished elements. */
		private final Queue<StreamElementQueueEntry<OUT>> completedElements;

		Segment(int initialCapacity) {
			incompleteElements = new HashSet<>(initialCapacity);
			completedElements = new ArrayDeque<>(initialCapacity);
		}

		/**
		 * Signals that an entry finished computation.
		 */
		void completed(StreamElementQueueEntry<OUT> elementQueueEntry) {
			// adding only to completed queue if not completed before
			// there may be a real result coming after a timeout result, which is updated in the queue entry but
			// the entry is not re-added to the complete queue
			if (incompleteElements.remove(elementQueueEntry)) {
				completedElements.add(elementQueueEntry);
			}
		}

		/**
		 * True if there are no incomplete elements and all complete elements have been consumed.
		 */
		boolean isEmpty() {
			return incompleteElements.isEmpty() && completedElements.isEmpty();
		}

		/**
		 * True if there is at least one completed elements, such that {@link #emitCompleted(TimestampedCollector)}
		 * will actually output an element.
		 */
		boolean hasCompleted() {
			return !completedElements.isEmpty();
		}

		/**
		 * Adds the segmentd input elements for checkpointing including completed but not yet emitted elements.
		 */
		void addPendingElements(List<StreamElement> results) {
			for (StreamElementQueueEntry<OUT> element : completedElements) {
				results.add(element.getInputElement());
			}
			for (StreamElementQueueEntry<OUT> element : incompleteElements) {
				results.add(element.getInputElement());
			}
		}

		/**
		 * Pops one completed elements into the given output. Because an input element may produce an arbitrary
		 * number of output elements, there is no correlation between the size of the collection and the popped
		 * elements.
		 *
		 * @return the number of popped input elements.
		 */
		int emitCompleted(TimestampedCollector<OUT> output) {
			final StreamElementQueueEntry<OUT> completedEntry = completedElements.poll();
			if (completedEntry == null) {
				return 0;
			}
			completedEntry.emitResult(output);
			return 1;
		}

		/**
		 * Adds the given entry to this segment. If the element is completed (watermark), it is directly moved into the
		 * completed queue.
		 */
		void add(StreamElementQueueEntry<OUT> queueEntry) {
			if (queueEntry.isDone()) {
				completedElements.add(queueEntry);
			} else {
				incompleteElements.add(queueEntry);
			}
		}
	}
}
