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

package org.apache.flink.streaming.api.functions.async.buffer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.functions.async.collector.AsyncCollector;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.async.AsyncWaitOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * AsyncCollectorBuffer will hold all {@link AsyncCollector} in its internal buffer,
 * and emit results from {@link AsyncCollector} to the next operators following it by
 * calling {@link Output#collect(Object)}
 */
@Internal
public class AsyncCollectorBuffer<IN, OUT> {

	/**
	 * The logger.
	 */
	private static final Logger LOG = LoggerFactory.getLogger(AsyncCollectorBuffer.class);

	/**
	 * Max number of {@link AsyncCollector} in the buffer.
	 */
	private final int bufferSize;

	private final AsyncDataStream.OutputMode mode;

	private final AsyncWaitOperator<IN, OUT> operator;

	/**
	 * Keep all {@link StreamElementEntry}
	 */
	private final Set<StreamElementEntry<OUT>> queue = new LinkedHashSet<>();

	/**
	 * Keep all {@link StreamElementEntry} to their corresponding {@link Watermark} or {@link LatencyMarker}
	 * If the inputs are: SR1, SR2, WM1, SR3, SR4. Then SR1 and SR2 belong to WM1, and
	 * SR3 and SR4 will be kept in {@link #lonelyEntries}
	 */
	private final Map<StreamElementEntry<OUT>, StreamElement> entriesToMarkers = new HashMap<>();

	private final List<StreamElementEntry<OUT>> lonelyEntries = new LinkedList<>();

	/**
	 * Keep finished AsyncCollector belonging to the oldest Watermark or LatencyMarker in UNORDERED mode.
	 */
	private final Map<StreamElement, Set<StreamElementEntry<OUT>>> markerToFinishedEntries = new LinkedHashMap<>();
	private Set<StreamElementEntry<OUT>>lonelyFinishedEntries = new HashSet<>();

	/**
	 * For the AsyncWaitOperator chained with StreamSource, the checkpoint thread may get the
	 * {@link org.apache.flink.streaming.runtime.tasks.StreamTask#lock} while {@link AsyncCollectorBuffer#queue}
	 * is full since main thread waits on this lock. The StreamElement in
	 * {@link AsyncWaitOperator#processElement(StreamRecord)} should be treated as a part of all StreamElements
	 * in its queue. It will be kept in the operator state while snapshotting.
	 */
	private StreamElement extraStreamElement;

	/**
	 * {@link TimestampedCollector} and {@link Output} to collect results and watermarks.
	 */
	private final Output<StreamRecord<OUT>> output;
	private final TimestampedCollector<OUT> timestampedCollector;

	/**
	 * Checkpoint lock from {@link org.apache.flink.streaming.runtime.tasks.StreamTask#lock}
	 */
	private final Object lock;

	private final Emitter emitter;
	private final Thread emitThread;

	/**
	 * Exception from async operation or internal error
	 */
	private Exception error;

	/**
	 * Flag telling Emitter thread to work or not.
	 */
	private volatile boolean workwork = false;

	public AsyncCollectorBuffer(
			int bufferSize,
			AsyncDataStream.OutputMode mode,
			Output<StreamRecord<OUT>> output,
			TimestampedCollector<OUT> collector,
			Object lock,
			AsyncWaitOperator operator) {
		Preconditions.checkArgument(bufferSize > 0, "Future buffer size should be greater than 0.");

		this.bufferSize = bufferSize;

		this.mode = Preconditions.checkNotNull(mode, "Processing mode should not be NULL.");
		this.output = Preconditions.checkNotNull(output, "Output should not be NULL.");
		this.timestampedCollector = Preconditions.checkNotNull(collector, "TimestampedCollector should not be NULL.");
		this.operator = Preconditions.checkNotNull(operator, "Reference to AsyncWaitOperator should not be NULL.");
		this.lock = Preconditions.checkNotNull(lock, "Checkpoint lock should not be NULL.");

		this.emitter = new Emitter();
		this.emitThread = new Thread(emitter);
		this.emitThread.setDaemon(true);
	}

	/**
	 * Add an {@link StreamRecord} into the buffer. A new {@link AsyncCollector} will be created and returned
	 * corresponding to the input StreamRecord.
	 * <p>
	 * If buffer is full, caller will wait until a new space is available.
	 *
	 * @param record StreamRecord
	 * @return An AsyncCollector
	 * @throws Exception Exception from AsyncCollector.
	 */
	public AsyncCollector<OUT> addStreamRecord(StreamRecord<IN> record) throws Exception {
		assert(Thread.holdsLock(lock));

		while (queue.size() >= bufferSize) {
			// hold the input StreamRecord until it is placed in the buffer
			extraStreamElement = record;

			lock.wait();
		}

		if (error != null) {
			throw error;
		}

		StreamElementEntry<OUT> entry = new StreamRecordEntry<>(record, this);

		queue.add(entry);

		if (mode == AsyncDataStream.OutputMode.UNORDERED) {
			lonelyEntries.add(entry);
		}

		extraStreamElement = null;

		return (AsyncCollector<OUT>)entry;
	}

	/**
	 * Add a {@link Watermark} into buffer.
	 * <p>
	 * If queue is full, caller will wait here.
	 *
	 * @param watermark Watermark
	 * @throws Exception Exception from AsyncCollector.
	 */
	public void addWatermark(Watermark watermark) throws Exception {
		processMark(new WatermarkEntry<OUT>(watermark));
	}

	/**
	 * Add a {@link LatencyMarker} into buffer.
	 * <p>
	 * If queue is full, caller will wait here.
	 *
	 * @param latencyMarker LatencyMarker
	 * @throws Exception Exception from AsyncCollector.
	 */
	public void addLatencyMarker(LatencyMarker latencyMarker) throws Exception {
		processMark(new LatencyMarkerEntry<OUT>(latencyMarker));
	}

	/**
	 * Notify the emitter thread and main thread that an AsyncCollector has completed.
	 *
	 * @param entry Completed AsyncCollector
	 */
	public void markCollectorCompleted(StreamElementEntry<OUT> entry) {
		synchronized (lock) {
			entry.markDone();

			if (mode == AsyncDataStream.OutputMode.UNORDERED) {
				StreamElement marker = entriesToMarkers.get(entry);

				if (marker != null) {
					markerToFinishedEntries.get(marker).add(entry);
				}
				else {
					lonelyFinishedEntries.add(entry);
				}
			}

			// if workwork is true, it is not necessary to check it again
			if (!workwork && shouldNotifyEmitterThread(entry)) {
				workwork = true;

				lock.notifyAll();
			}
		}
	}

	/**
	 * Caller will wait here if buffer is not empty, meaning that not all async i/o tasks have returned yet.
	 *
	 * @throws Exception IOException from AsyncCollector.
	 */
	public void waitEmpty() throws Exception {
		assert(Thread.holdsLock(lock));

		while (queue.size() != 0) {
			if (error != null) {
				throw error;
			}

			lock.wait();
		}
	}

	public void startEmitterThread() {
		emitThread.start();
	}

	public void stopEmitterThread() {
		emitter.stop();

		emitThread.interrupt();

		while (emitThread.isAlive()) {
			// temporarily release the lock first, since caller of this method may also hold the lock.
			if (Thread.holdsLock(lock)) {
				try {
					lock.wait(1000);
				}
				catch (InterruptedException e) {
					// do nothing
				}
			}

			try {
				emitThread.join(10000);
			} catch (InterruptedException e) {
				// do nothing
			}

			// get the stack trace
			StringBuilder sb = new StringBuilder();
			StackTraceElement[] stack = emitThread.getStackTrace();

			for (StackTraceElement e : stack) {
				sb.append(e).append('\n');
			}

			LOG.warn("Emitter thread blocks due to {}", sb.toString());

			emitThread.interrupt();
		}
	}

	/**
	 * Get all StreamElements in the AsyncCollector queue.
	 * <p>
	 * Emitter Thread can not output records and will wait for a while due to checkpoiting procedure
	 * holding the checkpoint lock.
	 *
	 * @return An {@link Iterator} to the StreamElements in the buffer, including the extra one.
	 */
	public Iterator<StreamElement> getStreamElementsInBuffer() {
		final Iterator<StreamElementEntry<OUT>> iterator = queue.iterator();
		final StreamElement extra = extraStreamElement;

		return new Iterator<StreamElement>() {
			boolean shouldSendExtraElement = (extra != null);

			@Override
			public boolean hasNext() {
				return iterator.hasNext() || shouldSendExtraElement;
			}

			@Override
			public StreamElement next() {
				if (!hasNext()) {
					throw new NoSuchElementException();
				}

				if (iterator.hasNext()) {
					return iterator.next().getStreamElement();
				}
				else {
					shouldSendExtraElement = false;

					return extra;
				}
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException("remove");
			}
		};
	}

	private void processMark(StreamElementEntry<OUT> entry) throws Exception {
		assert(Thread.holdsLock(lock));

		StreamElement mark = entry.getStreamElement();

		while (queue.size() >= bufferSize) {
			// hold the input StreamRecord until it is placed in the buffer
			extraStreamElement = mark;

			lock.wait();
		}

		if (error != null) {
			throw error;
		}

		queue.add(entry);

		if (mode == AsyncDataStream.OutputMode.UNORDERED) {
			// update AsyncCollector to Watermark / LatencyMarker map
			for (StreamElementEntry<OUT> e : lonelyEntries) {
				entriesToMarkers.put(e, mark);
			}

			lonelyEntries.clear();

			// update Watermark / LatencyMarker to finished AsyncCollector map
			markerToFinishedEntries.put(mark, lonelyFinishedEntries);

			lonelyFinishedEntries = new HashSet<>();
		}

		extraStreamElement = null;

		// notify Emitter thread if the head of buffer is Watermark or LatencyMarker
		// this is for the case when LatencyMarkers keep coming but there is no StreamRecords.
		StreamElementEntry<OUT> element = queue.iterator().next();

		if (element.isLatencyMarker() || element.isWatermark()) {
			workwork = true;

			lock.notifyAll();
		}
	}

	private boolean shouldNotifyEmitterThread(StreamElementEntry<OUT> entry) {

		switch (mode) {

			case ORDERED:
				Iterator<StreamElementEntry<OUT>> queueIterator = queue.iterator();

				// get to work as long as the first AsyncCollect is done.
				return queueIterator.hasNext() && (queueIterator.next().isDone());

			case UNORDERED:
				Iterator<Map.Entry<StreamElement, Set<StreamElementEntry<OUT>>>> iteratorMarker =
						markerToFinishedEntries.entrySet().iterator();

				// get to work only the finished AsyncCollector belongs to the oldest Watermark or LatencyMarker
				// or no Watermark / LatencyMarker is in the buffer yet.
				return iteratorMarker.hasNext() ? iteratorMarker.next().getValue().contains(entry)
						: lonelyFinishedEntries.contains(entry);

			default:
				// this case should never happen
				return false;
		}
	}

	@VisibleForTesting
	public Set<StreamElementEntry<OUT>> getQueue() {
		return queue;
	}

	@VisibleForTesting
	public void setExtraStreamElement(StreamElement element) {
		extraStreamElement = element;
	}

	/**
	 * A working thread to output results from {@link AsyncCollector} to the next operator.
	 */
	private class Emitter implements Runnable {
		private volatile boolean running = true;

		private void output(StreamElementEntry<OUT> entry) throws Exception {

			StreamElement element = entry.getStreamElement();

			if (element == null) {
				throw new Exception("StreamElement in the buffer entry should not be null");
			}

			if (entry.isStreamRecord()) {
				List<OUT> result = entry.getResult();

				if (result == null) {
					throw new Exception("Result for stream record " + element + " is null");
				}

				// update the timestamp for the collector
				timestampedCollector.setTimestamp(element.asRecord());

				for (OUT val : result) {
					timestampedCollector.collect(val);
				}
			}
			else if (entry.isWatermark()) {
				output.emitWatermark(element.asWatermark());
			}
			else if (entry.isLatencyMarker()) {
				operator.sendLatencyMarker(element.asLatencyMarker());
			}
			else {
				throw new IOException("Unknown input record: " + element);
			}
		}

		/**
		 * Emit results from the finished head collector and its following finished ones.
		 *
		 * <p>NOTE: Since {@link #output(StreamElementEntry)} may be blocked if operator chain chained with
		 * another {@link AsyncWaitOperator} and its buffer is full, we can not use an {@link Iterator} to
		 * go through {@link #queue} because ConcurrentModificationException may be thrown while we remove
		 * element in the queue by calling {@link Iterator#remove()}.
		 *
		 * <p>Example: Assume operator chain like this: async-wait-operator1(awo1) -> async-wait-operator2(awo2).
		 * The buffer for awo1 is full so the main thread is blocked there.
		 * The {@link Emitter} thread, named emitter1, in awo1 is outputting
		 * data to awo2. Assume that 2 elements have been emitted and the buffer in awo1 has two vacancies. While
		 * outputting the third one, the buffer in awo2 is full, so emitter1 will wait for a moment. If we use
		 * {@link Iterator}, it is just before calling {@link Iterator#remove()}. Once the {@link #lock} is released
		 * and luckily enough, the main thread get the lock. It will modify {@link #queue}, causing
		 * ConcurrentModificationException once emitter1 runs to {@link Iterator#remove()}.
		 *
		 */
		private void orderedProcess() throws Exception {
			StreamElementEntry<OUT> entry;

			while (queue.size() > 0 && (entry = queue.iterator().next()).isDone()) {
				output(entry);

				queue.remove(entry);
			}
		}

		/**
		 * Emit results for each finished collector. Try to emit results prior to the oldest watermark
		 * in the buffer.
		 * <p>
		 * For example, assume the sequence of input StreamElements is:
		 * Entry(ac1, record1) -> Entry(ac2, record2) -> Entry(ac3, watermark1) -> Entry(ac4, record3).
		 * and both of ac2 and ac3 have finished. For unordered-mode, ac1 and ac2 are prior to watermark1,
		 * so ac2 will be emitted. Since ac1 is not ready yet, ac3 have to wait until ac1 is done.
		 */
		private void unorderedProcess() throws Exception {
			// try to emit finished AsyncCollectors in markerToFinishedEntries
			if (markerToFinishedEntries.size() != 0) {
				while (markerToFinishedEntries.size() != 0) {
					Map.Entry<StreamElement, Set<StreamElementEntry<OUT>>> finishedStreamElementEntry =
							markerToFinishedEntries.entrySet().iterator().next();

					Set<StreamElementEntry<OUT>> finishedElementSet = finishedStreamElementEntry.getValue();

					// While outputting results to the next operator, output may release lock if the following operator
					// in the chain is another AsyncWaitOperator. During this period, there may be some
					// finished StreamElementEntry coming into the finishedElementSet, and we should
					// output all finished elements after re-acquiring the lock.
					while (finishedElementSet.size() != 0) {
						StreamElementEntry<OUT> finishedEntry = finishedElementSet.iterator().next();

						output(finishedEntry);

						queue.remove(finishedEntry);

						entriesToMarkers.remove(finishedEntry);

						finishedElementSet.remove(finishedEntry);
					}

					finishedStreamElementEntry.getValue().clear();


					// if all StreamElements belonging to current Watermark / LatencyMarker have been emitted,
					// emit current Watermark / LatencyMarker

					if (queue.size() == 0) {
						if (markerToFinishedEntries.size() != 0 || entriesToMarkers.size() != 0
								|| lonelyEntries.size() != 0 || lonelyFinishedEntries.size() != 0) {
							throw new IOException("Inner data info is not consistent.");
						}
					}
					else {
						// check the head AsyncCollector whether it is a Watermark or LatencyMarker.
						StreamElementEntry<OUT> queueEntry = queue.iterator().next();

						if (!queueEntry.isStreamRecord()) {
							if (finishedStreamElementEntry.getKey() != queueEntry.getStreamElement()) {
								throw new IOException("Watermark / LatencyMarker from finished collector map "
									+ "and input buffer are not the same.");
							}

							output(queueEntry);

							queue.remove(queueEntry);

							// remove useless data in markerToFinishedEntries
							markerToFinishedEntries.remove(finishedStreamElementEntry.getKey());
						}
						else {
							break;
						}
					}
				}
			}

			if (markerToFinishedEntries.size() == 0) {
				// health check
				if (entriesToMarkers.size() != 0) {
					throw new IOException("Entries to marker map should be zero");
				}

				// no Watermark or LatencyMarker in the buffer yet, emit results in lonelyFinishedEntries
				while (lonelyFinishedEntries.size() != 0) {
					StreamElementEntry<OUT> entry = lonelyFinishedEntries.iterator().next();

					output(entry);

					queue.remove(entry);

					lonelyEntries.remove(entry);

					lonelyFinishedEntries.remove(entry);
				}
			}
		}

		private void processFinishedAsyncCollector() throws Exception {
			if (mode == AsyncDataStream.OutputMode.ORDERED) {
				orderedProcess();
			} else {
				unorderedProcess();
			}
		}

		private void clearAndNotify() {
			// clear all data
			queue.clear();
			entriesToMarkers.clear();
			markerToFinishedEntries.clear();
			lonelyEntries.clear();

			running = false;

			lock.notifyAll();
		}

		@Override
		public void run() {
			while (running) {
				synchronized (lock) {

					try {
						while (!workwork) {
							lock.wait();
						}

						processFinishedAsyncCollector();

						lock.notifyAll();

						workwork = false;
					}
					catch (InterruptedException e) {
						// The source of InterruptedException is from:
						//   1. lock.wait() statement in Emit
						//   2. collector waiting for vacant buffer
						// The action for this exception should try to clear all held data and
						// exit Emit thread.

						clearAndNotify();
					}
					catch (Exception e) {
						// For exceptions, not InterruptedException, it should be propagated
						// to main thread.
						error = e;

						clearAndNotify();
					}
				}
			}
		}

		public void stop() {
			running = false;
		}
	}
}
