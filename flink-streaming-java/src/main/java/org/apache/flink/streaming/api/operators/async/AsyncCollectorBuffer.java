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

package org.apache.flink.streaming.api.operators.async;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * AsyncCollectorBuffer will hold all {@link AsyncCollector} in its internal buffer,
 * and emit results from {@link AsyncCollector} to the next operators following it by
 * calling {@link Output#collect(Object)}
 */
@Internal
public class AsyncCollectorBuffer<IN, OUT> {

	/**
	 * Max number of {@link AsyncCollector} in the buffer.
	 */
	private final int bufferSize;

	private final AsyncDataStream.OutputMode mode;

	private final AsyncWaitOperator<IN, OUT> operator;

	/**
	 * Keep all {@code AsyncCollector} and their input {@link StreamElement}
	 */
	private final Map<AsyncCollector<IN, OUT>, StreamElement> queue = new LinkedHashMap<>();
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

	private IOException error;

	public AsyncCollectorBuffer(
			int bufferSize,
			AsyncDataStream.OutputMode mode,
			Output<StreamRecord<OUT>> output,
			TimestampedCollector<OUT> collector,
			Object lock,
			AsyncWaitOperator operator) {
		Preconditions.checkArgument(bufferSize > 0, "Future buffer size should be greater than 0.");
		Preconditions.checkNotNull(output, "Output should not be NULL.");
		Preconditions.checkNotNull(collector, "TimestampedCollector should not be NULL.");
		Preconditions.checkNotNull(lock, "Checkpoint lock should not be NULL.");
		Preconditions.checkNotNull(operator, "Reference to AsyncWaitOperator should not be NULL.");

		this.bufferSize = bufferSize;
		this.mode = mode;
		this.output = output;
		this.timestampedCollector = collector;
		this.operator = operator;
		this.lock = lock;

		this.emitter = new Emitter();
		this.emitThread = new Thread(emitter);
	}

	/**
	 * Add an {@link StreamRecord} into the buffer. A new {@link AsyncCollector} will be created and returned
	 * corresponding to the input StreamRecord.
	 * <p>
	 * If buffer is full, caller will wait until a new space is available.
	 *
	 * @param record StreamRecord
	 * @return An AsyncCollector
	 * @throws Exception InterruptedException or IOException from AsyncCollector.
	 */
	public AsyncCollector<IN, OUT> addStreamRecord(StreamRecord<IN> record) throws InterruptedException, IOException {
		while (queue.size() >= bufferSize) {
			// hold the input StreamRecord until it is placed in the buffer
			extraStreamElement = record;

			lock.wait();
		}

		if (error != null) {
			throw error;
		}

		AsyncCollector<IN, OUT> collector = new AsyncCollector(this);

		queue.put(collector, record);

		extraStreamElement = null;

		return collector;
	}

	/**
	 * Add a {@link Watermark} into queue. A new AsyncCollector will be created and returned.
	 * <p>
	 * If queue is full, caller will wait here.
	 *
	 * @param watermark Watermark
	 * @return AsyncCollector
	 * @throws Exception InterruptedException or IOException from AsyncCollector.
	 */
	public AsyncCollector<IN, OUT> addWatermark(Watermark watermark) throws InterruptedException, IOException {
		return processMark(watermark);
	}

	/**
	 * Add a {@link LatencyMarker} into queue. A new AsyncCollector will be created and returned.
	 * <p>
	 * If queue is full, caller will wait here.
	 *
	 * @param latencyMarker LatencyMarker
	 * @return AsyncCollector
	 * @throws Exception InterruptedException or IOException from AsyncCollector.
	 */
	public AsyncCollector<IN, OUT> addLatencyMarker(LatencyMarker latencyMarker) throws InterruptedException, IOException {
		return processMark(latencyMarker);
	}

	private AsyncCollector<IN, OUT> processMark(StreamElement mark) throws InterruptedException, IOException {
		while (queue.size() >= bufferSize) {
			// hold the input StreamRecord until it is placed in the buffer
			extraStreamElement = mark;

			lock.wait();
		}

		if (error != null) {
			throw error;
		}

		AsyncCollector<IN, OUT> collector = new AsyncCollector(this, true);

		queue.put(collector, mark);

		extraStreamElement = null;

		return collector;
	}

	/**
	 * Notify the emitter thread and main thread that an AsyncCollector has completed.
	 *
	 * @param collector Completed AsyncCollector
	 */
	void markCollectorCompleted(AsyncCollector<IN, OUT> collector) {
		synchronized (lock) {
			collector.markDone();

			// notify main thread to keep working
			lock.notifyAll();
		}
	}

	/**
	 * Caller will wait here if buffer is not empty, meaning that not all async i/o tasks have returned yet.
	 *
	 * @throws Exception InterruptedException or IOException from AsyncCollector.
	 */
	void waitEmpty() throws InterruptedException, IOException {
		while (queue.size() != 0) {
			if (error != null) {
				throw error;
			}

			lock.wait();
		}
	}

	public void startEmitterThread() {
		this.emitThread.start();
	}

	public void stopEmitterThread() {
		emitter.stop();

		emitThread.interrupt();
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

	@VisibleForTesting
	public Map<AsyncCollector<IN, OUT>, StreamElement> getQueue() {
		return this.queue;
	}

	/**
	 * A working thread to output results from {@link AsyncCollector} to the next operator.
	 */
	private class Emitter implements Runnable {
		private volatile boolean running = true;

		private void output(AsyncCollector collector, StreamElement element) throws Exception {
			List<OUT> result = collector.getResult();

			// update timestamp for output stream records based on the input stream record.
			if (element == null) {
				throw new Exception("No input stream element for current AsyncCollector");
			}

			if (element.isRecord()) {
				if (result == null) {
					throw new Exception("Result for stream record "+element+" is null");
				}

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
		private void orderedProcess() throws IOException {
			Iterator<Map.Entry<AsyncCollector<IN, OUT>, StreamElement>> iterator = queue.entrySet().iterator();

			while (iterator.hasNext()) {
				try {
					Map.Entry<AsyncCollector<IN, OUT>, StreamElement> entry = iterator.next();

					AsyncCollector<IN, OUT> collector = entry.getKey();

					if (!collector.isDone()) {
						break;
					}

					output(collector, entry.getValue());

					iterator.remove();

					lock.notifyAll();
				}
				catch (Exception e) {
					throw new IOException(e);
				}
			}
		}

		/**
		 * Emit results for each finished collector. Try to emit results prior to the oldest watermark
		 * in the queue.
		 * <p>
		 * For example, assume the queue layout is:
		 * Entry(ac1, record1) -> Entry(ac2, record2) -> Entry(ac3, watermark1) -> Entry(ac4, record3).
		 * and both of ac2 and ac3 have finished. For unordered-mode, ac1 and ac2 are prior to watermark1,
		 * so ac2 will be emitted. Since ac1 is not ready yet, ac3 have to wait until ac1 is done.
		 */
		private void unorderedProcess() throws IOException {
			Iterator<Map.Entry<AsyncCollector<IN, OUT>, StreamElement>> iterator = queue.entrySet().iterator();

			boolean hasUnfinishedCollector = false;

			while (iterator.hasNext()) {
				Map.Entry<AsyncCollector<IN, OUT>, StreamElement> entry = iterator.next();

				StreamElement element = entry.getValue();
				AsyncCollector<IN, OUT> collector = entry.getKey();

				if (element.isWatermark() && hasUnfinishedCollector) {
					break;
				}

				if (collector.isDone() == false) {
					hasUnfinishedCollector = true;

					continue;
				}

				try {
					output(collector, element);

					iterator.remove();

					lock.notifyAll();
				}
				catch (Exception e) {
					throw new IOException(e);
				}
			}
		}

		/**
		 * If
		 *   In ordered mode, there are some finished async collectors, and one of them is the first element in
		 *   the queue.
		 * or
		 *   In unordered mode, there are some finished async collectors prior to the oldest water mark.
		 * or
		 *   The first element in the queue is Watermark or LatencyMarker.
		 * Then, the emitter thread should keep waiting, rather than waiting on the condition.
		 *
		 * Otherwise, the thread should stop for a while until being signalled.
		 */
		private boolean nothingToDo() {
			if (queue.size() == 0) {
				return true;
			}

			// get the first AsyncCollector and StreamElement in the queue.
			Map.Entry<AsyncCollector<IN, OUT>, StreamElement> firstEntry = queue.entrySet().iterator().next();
			StreamElement element = firstEntry.getValue();
			AsyncCollector<IN, OUT> collector = firstEntry.getKey();

			// check head element of the queue, it is OK to process Watermark or LatencyMarker
			if (element.isWatermark() || element.isLatencyMarker()) {
				return false;
			}

			if (mode == AsyncDataStream.OutputMode.ORDERED) {
				// for ORDERED mode, make sure the first collector in the queue has been done.
				return !collector.isDone();
			}
			else {
				Iterator<Map.Entry<AsyncCollector<IN, OUT>, StreamElement>> iterator = queue.entrySet().iterator();

				boolean noFinishedCollector = true;

				while (iterator.hasNext()) {
					Map.Entry<AsyncCollector<IN, OUT>, StreamElement> entry = iterator.next();

					if (entry.getKey().isDone()) {
						noFinishedCollector = false;
						break;
					}

					if (entry.getValue().isWatermark()) {
						break;
					}
				}

				return noFinishedCollector;
			}
		}

		private void processFinishedAsyncCollector() throws IOException {
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
						while (nothingToDo()) {
							lock.wait();
						}
					}
					catch(InterruptedException e) {
						break;
					}

					try {
						processFinishedAsyncCollector();
					}
					catch (IOException e) {
						queue.clear();
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

