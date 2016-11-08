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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	private static final Logger LOG = LoggerFactory.getLogger(AsyncCollectorBuffer.class);

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
	 * in its queue.
	 */
	private StreamElement extraStreamElement;

	/**
	 * {@link TimestampedCollector} and {@link Output} to collect results and watermarks.
	 */
	final private Output<StreamRecord<OUT>> output;
	final private TimestampedCollector<OUT> timestampedCollector;

	/**
	 * Checkpoint lock from {@link org.apache.flink.streaming.runtime.tasks.StreamTask#lock}
	 */
	private final Object lock;

	public AsyncCollectorBuffer(
			int bufferSize,
			AsyncDataStream.OutputMode mode,
			Output<StreamRecord<OUT>> output,
			TimestampedCollector<OUT> collector,
			Object lock,
			AsyncWaitOperator operator) {
		Preconditions.checkArgument(bufferSize > 0, "Future buffer size should be greater than 0.");

		this.bufferSize = bufferSize;
		this.mode = mode;
		this.output = output;
		this.timestampedCollector = collector;
		this.operator = operator;
		this.lock = lock;
	}

	/**
	 * Add an {@link StreamRecord} into the buffer. A new {@link AsyncCollector} will be created and returned
	 * corresponding to the input StreamRecord.
	 * <p>
	 * If buffer is full, caller will wait until new space is available.
	 *
	 * @param record StreamRecord
	 * @return An AsyncCollector
	 * @throws Exception InterruptedException or exceptions from AsyncCollector.
	 */
	public AsyncCollector<IN, OUT> addStreamRecord(StreamRecord<IN> record) throws InterruptedException, IOException {
		while (queue.size() >= bufferSize) {
			if (nothingToDo()) {
				extraStreamElement = record;

				lock.wait();
			}
			else {
				processFinishedAsyncCollector();
			}
		}

		AsyncCollector<IN, OUT> collector = new AsyncCollector(this);

		queue.put(collector, record);

		extraStreamElement = null;

		return collector;
	}

	/**
	 * Add a {@link Watermark} into queue. A new AsyncCollector will be created and returned.
	 * <p>
	 * If queue is full, caller will be blocked here.
	 *
	 * @param watermark Watermark
	 * @return AsyncCollector
	 * @throws Exception Exceptions from async operation.
	 */
	public AsyncCollector<IN, OUT> addWatermark(Watermark watermark) throws Exception {
		return processMark(watermark);
	}

	/**
	 * Add a {@link LatencyMarker} into queue. A new AsyncCollector will be created and returned.
	 * <p>
	 * If queue is full, caller will be blocked here.
	 *
	 * @param latencyMarker LatencyMarker
	 * @return AsyncCollector
	 * @throws Exception Exceptions from async operation.
	 */
	public AsyncCollector<IN, OUT> addLatencyMarker(LatencyMarker latencyMarker) throws Exception {
		return processMark(latencyMarker);
	}

	private AsyncCollector<IN, OUT> processMark(StreamElement mark) throws InterruptedException, IOException {
		while (queue.size() >= bufferSize) {
			if (nothingToDo()) {
				extraStreamElement = mark;

				lock.wait();
			}
			else {
				processFinishedAsyncCollector();
			}
		}

		AsyncCollector<IN, OUT> collector = new AsyncCollector(this, true);

		queue.put(collector, mark);

		extraStreamElement = null;

		return collector;
	}

	/**
	 * Notify the Emitter Thread that an AsyncCollector has completed.
	 *
	 * @param collector Completed AsyncCollector
	 */
	void markCollectorCompleted(AsyncCollector<IN, OUT> collector) {
		synchronized (lock) {
			// notify main thread to keep working
			lock.notifyAll();
		}
	}

	/**
	 * Caller will wait here if buffer is not empty, meaning that not all async i/o tasks have returned yet.
	 *
	 * @throws Exception InterruptedException or Exceptions from AsyncCollector.
	 */
	void waitEmpty() throws InterruptedException, IOException {
		while (queue.size() != 0) {
			if (nothingToDo()) {
				lock.wait();
			}
			else {
				processFinishedAsyncCollector();
			}
		}
	}

	/**
	 * Get all StreamElements in the AsyncCollector queue.
	 * <p>
	 * Emitter Thread can not output records and will wait for a while due to isCheckpointing flag
	 * until checkpointing is done.
	 *
	 * @return A List containing StreamElements.
	 */
	public List<StreamElement> getStreamElementsInBuffer() {
		List<StreamElement> ret = new ArrayList<>(queue.size());
		for (Map.Entry<AsyncCollector<IN, OUT>, StreamElement> entry : queue.entrySet()) {
			ret.add(entry.getValue());
		}

		if (extraStreamElement != null) {
			ret.add(extraStreamElement);
		}

		return ret;
	}

	@VisibleForTesting
	public Map<AsyncCollector<IN, OUT>, StreamElement> getQueue() {
		return this.queue;
	}

	private void output(AsyncCollector collector, StreamElement element) throws Exception {
		List<OUT> result = collector.getResult();

		// update timestamp for output stream records based on the input stream record.
		if (element == null) {
			throw new Exception("No input stream record or watermark for current AsyncCollector: "+collector);
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
			}
			catch (Exception e) {
				throw new IOException(e);
			}
		}
	}

	/**
	 * Emit results for each finished collector. Try to emit results belonging to the current watermark
	 * in the queue.
	 * <p>
	 * For example, the queue layout is:
	 * Entry(ac1, record1) -> Entry(ac2, record2) -> Entry(ac3, watermark1) -> Entry(ac4, record3).
	 * and both of ac2 and ac3 have finished. For unordered-mode, ac1 and ac2 belong to watermark1,
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
	 *   In unordered mode, there are some finished async collectors.
	 * or
	 *   The first element in the queue is Watermark.
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

		// for unordered mode, we have to check it in the queue.
		return false;
	}

	public void processFinishedAsyncCollector() throws IOException {
		if (mode == AsyncDataStream.OutputMode.ORDERED) {
			orderedProcess();
		} else {
			unorderedProcess();
		}
	}
}

