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
import java.util.*;

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
	 * Keep all {@link AsyncCollector} and their input {@link StreamElement}
	 */
	private final Map<StreamElementEntry<IN, OUT>, StreamElement> queue = new LinkedHashMap<>();

	/**
	 * Keep all {@link AsyncCollector} to their corresponding {@link Watermark} or {@link LatencyMarker}
	 */
	private final Map<StreamElementEntry<IN, OUT>, StreamElement> collectorToMarkers = new HashMap<>();
	private final List<StreamElementEntry<IN, OUT>> lonelyCollectors = new LinkedList<>();

	/**
	 * Keep finished AsyncCollector belonging to the oldest Watermark or LatencyMarker in UNORDERED mode.
	 */
	private final Map<StreamElement, Set<StreamElementEntry<IN, OUT>>> markerToFinishedCollectors = new LinkedHashMap<>();
	private Set<StreamElementEntry<IN, OUT>> lonelyFinishedCollectors = new HashSet<>();

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
	private boolean workwork;

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
		while (queue.size() >= bufferSize) {
			// hold the input StreamRecord until it is placed in the buffer
			extraStreamElement = record;

			assert(Thread.holdsLock(lock));

			lock.wait();
		}

		if (error != null) {
			throw error;
		}

		AsyncCollector<IN, OUT> collector = new AsyncCollector(this);

		queue.put(collector, record);

		lonelyCollectors.add(collector);

		extraStreamElement = null;

		return collector;
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
		processMark(watermark);
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
		processMark(latencyMarker);
	}

	/**
	 * Notify the emitter thread and main thread that an AsyncCollector has completed.
	 *
	 * @param collector Completed AsyncCollector
	 */
	public void markCollectorCompleted(AsyncCollector<IN, OUT> collector) {
		synchronized (lock) {
			collector.markDone();

			if (shouldNotifyEmitterThread(collector)) {
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
		while (queue.size() != 0) {
			if (error != null) {
				throw error;
			}

			assert(Thread.holdsLock(lock));

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
	 * @return A List containing StreamElements.
	 */
	public List<StreamElement> getStreamElementsInBuffer() {
		List<StreamElement> ret = new ArrayList<>(queue.size());
		for (Map.Entry<AsyncCollector<IN, OUT>, StreamElement> entry : queue.entrySet()) {
			ret.add(entry.getValue());
		}

		// add the lonely input outside of queue into return set.
		if (extraStreamElement != null) {
			ret.add(extraStreamElement);
		}

		return ret;
	}

	private void processMark(StreamElement mark) throws Exception {
		assert(Thread.holdsLock(lock));

		while (queue.size() >= bufferSize) {
			// hold the input StreamRecord until it is placed in the buffer
			extraStreamElement = mark;

			lock.wait();
		}

		if (error != null) {
			throw error;
		}

		queue.put(new AsyncCollector<>(this, true), mark);

		if (mode == AsyncDataStream.OutputMode.UNORDERED) {
			// update AsyncCollector to Watermark / LatencyMarker map
			for (AsyncCollector<IN, OUT> collector : lonelyCollectors) {
				collectorToMarkers.put(collector, mark);
			}

			lonelyCollectors.clear();

			// update Watermark / LatencyMarker to finished AsyncCollector map
			markerToFinishedCollectors.put(mark, lonelyFinishedCollectors);

			lonelyFinishedCollectors = new HashSet<>();
		}

		extraStreamElement = null;

		// notify Emitter thread if the head of buffer is Watermark or LatencyMarker
		// this is for the case when LatencyMarkers keep coming but there is no StreamRecords.
		StreamElement element = queue.entrySet().iterator().next().getValue();

		if (element.isLatencyMarker() || element.isWatermark()) {
			workwork = true;

			lock.notifyAll();
		}
	}

	private boolean shouldNotifyEmitterThread(AsyncCollector<IN, OUT> collector) {
		switch (mode) {

			case ORDERED:
				Iterator<Map.Entry<AsyncCollector<IN, OUT>, StreamElement>> iteratorQueue =
						queue.entrySet().iterator();

				// get to work as long as the first AsyncCollect is done.
				return iteratorQueue.hasNext() && (iteratorQueue.next().getKey() == collector);

			case UNORDERED:
				StreamElement marker = collectorToMarkers.get(collector);

				if (marker != null) {
					markerToFinishedCollectors.get(marker).add(collector);
				}
				else {
					lonelyFinishedCollectors.add(collector);
				}

				Iterator<Map.Entry<StreamElement, Set<AsyncCollector<IN, OUT>>>> iteratorMarker =
						markerToFinishedCollectors.entrySet().iterator();

				// get to work only the finished AsyncCollector belongs to the oldest Watermark or LatencyMarker
				// or no Watermark / LatencyMarker is in the buffer yet.
				return iteratorMarker.hasNext() ? iteratorMarker.next().getValue().contains(collector) : lonelyFinishedCollectors.contains(collector);

			default:
				// this case should never happen
				return false;
		}
	}

	@VisibleForTesting
	public Map<AsyncCollector<IN, OUT>, StreamElement> getQueue() {
		return queue;
	}

	/**
	 * A working thread to output results from {@link AsyncCollector} to the next operator.
	 */
	private class Emitter implements Runnable {
		private volatile boolean running = true;

		private void output(AsyncCollector<IN, OUT> collector, StreamElement element) throws Exception {
			List<OUT> result = collector.getResult();

			if (element == null) {
				throw new IOException("No input stream element for current AsyncCollector");
			}

			if (element.isRecord()) {
				if (result == null) {
					throw new Exception("Result for stream record "+element+" is null");
				}

				// update the timestamp for the collector
				timestampedCollector.setTimestamp(element.asRecord());

				for (OUT val : result) {
					timestampedCollector.collect(val);
				}
			}
			else if (element.isWatermark()) {
				output.emitWatermark(element.asWatermark());
			}
			else if (element.isLatencyMarker()) {
				operator.sendLatencyMarker(element.asLatencyMarker());
			}
			else {
				throw new IOException("Unknown input record: "+element);
			}
		}

		/**
		 * Emit results from the finished head collector and its following finished ones.
		 */
		private void orderedProcess() throws Exception {
			Iterator<Map.Entry<AsyncCollector<IN, OUT>, StreamElement>> iterator = queue.entrySet().iterator();

			while (iterator.hasNext()) {
				Map.Entry<AsyncCollector<IN, OUT>, StreamElement> entry = iterator.next();

				AsyncCollector<IN, OUT> collector = entry.getKey();

				if (!collector.isDone()) {
					break;
				}

				output(collector, entry.getValue());

				iterator.remove();
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

			// try to emit finished AsyncCollectors in markerToFinishedCollectors
			if (markerToFinishedCollectors.size() != 0) {

				Iterator<Map.Entry<StreamElement, Set<AsyncCollector<IN, OUT>>>> iterator =
					markerToFinishedCollectors.entrySet().iterator();

				while (iterator.hasNext()) {
					Map.Entry<StreamElement, Set<AsyncCollector<IN, OUT>>> entry = iterator.next();

					for (AsyncCollector<IN, OUT> collector : entry.getValue()) {
						output(collector, queue.remove(collector));

						collectorToMarkers.remove(collector);
					}

					entry.getValue().clear();

					// if all StreamElements belonging to current Watermark / LatencyMarker have been emitted,
					// emit current Watermark / LatencyMarker
					Iterator<Map.Entry<AsyncCollector<IN, OUT>, StreamElement>> iteratorQueue =
							queue.entrySet().iterator();

					if (!iteratorQueue.hasNext()) {
						if (iterator.hasNext() || lonelyCollectors.size() != 0) {
							throw new IOException("Inner data info is not consistent.");
						}
					}
					else {
						// check the head AsyncCollector whether it is a Watermark or LatencyMarker.
						Map.Entry<AsyncCollector<IN, OUT>, StreamElement> queueEntry = iteratorQueue.next();

						if (!queueEntry.getValue().isRecord()) {
							if (entry.getKey() != queueEntry.getValue()) {
								throw new IOException("Watermark / LatencyMarker are not the same.");
							}

							output(queueEntry.getKey(), queueEntry.getValue());

							iteratorQueue.remove();

							// remove useless data in markerToFinishedCollectors
							iterator.remove();
						}
						else {
							break;
						}
					}
				}
			}

			if (markerToFinishedCollectors.size() == 0) {
				// no Watermark or LatencyMarker in the buffer yet, emit results in lonelyFinishedCollectors
				for (AsyncCollector<IN, OUT> collector : lonelyCollectors) {
					output(collector, queue.remove(collector));

					collectorToMarkers.remove(collector);
				}

				lonelyCollectors.clear();
			}
		}

		private void processFinishedAsyncCollector() throws Exception {
			if (mode == AsyncDataStream.OutputMode.ORDERED) {
				orderedProcess();
			} else {
				unorderedProcess();
			}
		}

		@Override
		public void run() {
			while (running) {
				synchronized (lock) {
					try {
						while (!workwork) {
							lock.wait();
						}
					}
					catch(InterruptedException e) {
						break;
					}

					try {
						processFinishedAsyncCollector();

						lock.notifyAll();

						workwork = false;
					}
					catch (Exception e) {
						// clear all data
						queue.clear();
						collectorToMarkers.clear();
						markerToFinishedCollectors.clear();
						lonelyCollectors.clear();

						error = e;

						lock.notifyAll();

						break;
					}
				}
			}
		}

		public void stop() {
			running = false;
		}
	}
}
