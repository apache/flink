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
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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

	/**
	 * {@link AsyncCollector} queue.
	 */
	private final SimpleLinkedList<AsyncCollector<IN, OUT>> queue = new SimpleLinkedList<>();
	/**
	 * A hash map keeping {@link AsyncCollector} and their corresponding {@link StreamElement}
	 */
	private final Map<AsyncCollector<IN, OUT>, StreamElement> collectorToStreamElement = new HashMap<>();
	/**
	 * A hash map keeping {@link AsyncCollector} and their node references in the #queue.
	 */
	private final Map<AsyncCollector<IN, OUT>, SimpleLinkedList.Node> collectorToQueue = new HashMap<>();

	private final LinkedList<AsyncCollector> finishedCollectors = new LinkedList<>();

	/**
	 * {@link TimestampedCollector} and {@link Output} to collect results and watermarks.
	 */
	private TimestampedCollector<OUT> timestampedCollector;
	private Output<StreamRecord<OUT>> output;

	/**
	 * Locks and conditions to synchronize with main thread and emitter thread.
	 */
	private final Lock lock;
	private final Condition notFull;
	private final Condition taskDone;
	private final Condition isEmpty;

	/**
	 * Error from user codes.
	 */
	private volatile Exception error;

	private final Emitter emitter;
	private final Thread emitThread;

	private boolean isCheckpointing;

	public AsyncCollectorBuffer(int maxSize, AsyncDataStream.OutputMode mode) {
		Preconditions.checkArgument(maxSize > 0, "Future buffer size should be greater than 0.");

		this.bufferSize = maxSize;
		this.mode = mode;

		this.lock = new ReentrantLock(true);
		this.notFull = this.lock.newCondition();
		this.taskDone = this.lock.newCondition();
		this.isEmpty = this.lock.newCondition();

		this.emitter = new Emitter();
		this.emitThread = new Thread(emitter);
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
	public AsyncCollector<IN, OUT> add(StreamRecord<IN> record) throws Exception {
		try {
			lock.lock();

			notifyCheckpointDone();

			while (queue.size() >= bufferSize) {
				notFull.await();
			}

			// propagate error to the main thread
			if (error != null) {
				throw error;
			}

			AsyncCollector<IN, OUT> collector = new AsyncCollector(this);

			collectorToQueue.put(collector, queue.add(collector));
			collectorToStreamElement.put(collector, record);

			return collector;
		}
		finally {
			lock.unlock();
		}
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
	public AsyncCollector<IN, OUT> add(Watermark watermark) throws Exception {
		try {
			lock.lock();

			notifyCheckpointDone();

			while (queue.size() >= bufferSize)
				notFull.await();

			if (error != null) {
				throw error;
			}

			AsyncCollector<IN, OUT> collector = new AsyncCollector(this, true);

			collectorToQueue.put(collector, queue.add(collector));
			collectorToStreamElement.put(collector, watermark);

			// signal emitter thread that current collector is ready
			mark(collector);

			return collector;
		}
		finally {
			lock.unlock();
		}
	}

	/**
	 * Notify the Emitter Thread that an AsyncCollector has completed.
	 *
	 * @param collector Completed AsyncCollector
	 */
	void mark(AsyncCollector<IN, OUT> collector) {
		try {
			lock.lock();

			if (mode == AsyncDataStream.OutputMode.UNORDERED) {
				finishedCollectors.add(collector);
			}

			taskDone.signal();
		}
		finally {
			lock.unlock();
		}
	}

	/**
	 * Caller will wait here if buffer is not empty, meaning that not all async i/o tasks have returned yet.
	 *
	 * @throws Exception InterruptedException or Exceptions from AsyncCollector.
	 */
	void waitEmpty() throws Exception {
		try {
			lock.lock();

			notifyCheckpointDone();

			while (queue.size() != 0)
				isEmpty.await();

			if (error != null) {
				throw error;
			}
		}
		finally {
			lock.unlock();
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
	 * Emitter Thread can not output records and will wait for a while due to isCheckpointing flag
	 * until doing checkpoint has done.
	 *
	 * @return A List containing StreamElements.
	 */
	public List<StreamElement> getStreamElementsInBuffer() {
		try {
			lock.lock();

			// stop emitter thread
			isCheckpointing = true;

			List<StreamElement> ret = new ArrayList<>();
			for (int i = 0; i < queue.size(); ++i) {
				AsyncCollector<IN, OUT> collector = queue.get(i);
				ret.add(collectorToStreamElement.get(collector));
			}

			return ret;
		}
		finally {
			lock.unlock();
		}
	}

	public void setOutput(TimestampedCollector<OUT> collector, Output<StreamRecord<OUT>> output) {
		this.timestampedCollector = collector;
		this.output = output;
	}

	public void notifyCheckpointDone() {
		this.isCheckpointing = false;
		this.taskDone.signalAll();
	}

	/**
	 * A working thread to output results from {@link AsyncCollector} to the next operator.
	 */
	private class Emitter implements Runnable {
		private volatile boolean running = true;

		private void output(AsyncCollector collector) throws IOException {
			List<OUT> result = collector.getResult();

			// update timestamp for output stream records based on the input stream record.
			StreamElement element = collectorToStreamElement.get(collector);
			if (element == null) {
				throw new RuntimeException("No input stream record or watermark for current AsyncCollector: "+collector);
			}

			if (element.isRecord()) {
				if (result == null) {
					throw new RuntimeException("Result for stream record "+element+" is null");
				}

				timestampedCollector.setTimestamp(element.asRecord());
				for (OUT val : result) {
					timestampedCollector.collect(val);
				}
			}
			else {
				output.emitWatermark(element.asWatermark());
			}
		}

		private void clearInfoInMaps(AsyncCollector collector) {
			collectorToStreamElement.remove(collector);
			collectorToQueue.remove(collector);
		}

		/**
		 * Emit results from the finished head collector and its following finished ones.
		 */
		private void orderedProcess() {
			while (queue.size() > 0) {
				try {
					AsyncCollector collector = queue.get(0);
					if (!collector.isDone()) {
						break;
					}

					output(collector);

					queue.remove(0);
					clearInfoInMaps(collector);

					notFull.signal();
				}
				catch (IOException e) {
					error = e;
					break;
				}
			}
		}

		/**
		 * Emit results for each finished collector.
		 */
		private void unorderedProcess() {
			AsyncCollector collector = finishedCollectors.pollFirst();
			while (collector != null) {
				try {
					output(collector);

					queue.remove(collectorToQueue.get(collector));
					clearInfoInMaps(collector);

					notFull.signal();

					collector = finishedCollectors.pollFirst();
				}
				catch (IOException e) {
					error = e;
					break;
				}
			}
		}

		/**
		 * If some bad things happened(like exceptions from async i/o), the operator tries to fail
		 * itself at:
		 *   {@link AsyncWaitOperator#processElement}, triggered by calling {@link AsyncCollectorBuffer#add}.
		 *   {@link AsyncWaitOperator#snapshotState}
		 *   {@link AsyncWaitOperator#close} while calling {@link AsyncCollectorBuffer#waitEmpty}
		 *
		 * It is necessary for Emitter Thread to notify methods blocking on notFull/isEmpty.
		 */
		private void processError() {
			queue.clear();
			finishedCollectors.clear();
			collectorToQueue.clear();
			collectorToStreamElement.clear();

			notFull.signalAll();
			isEmpty.signalAll();
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
				isEmpty.signalAll();
				return true;
			}

			// while doing checkpoints, emitter thread should not try to output records.
			if (isCheckpointing) {
				return true;
			}

			// check head element of the queue
			if (collectorToStreamElement.get(queue.get(0)).isWatermark()) {
				return false;
			}

			if (mode == AsyncDataStream.OutputMode.UNORDERED) {
				// no finished async collector...
				if (finishedCollectors.size() == 0) {
					return true;
				}
				else {
					return false;
				}
			}
			else {
				// for ORDERED mode, make sure the first collector in the queue has been done.
				AsyncCollector collector = queue.get(0);
				if (collector.isDone() == false) {
					return true;
				}
				else {
					return false;
				}
			}
		}

		@Override
		public void run() {
			while (running) {
				try {
					lock.lock();

					if (error != null) {
						// stop processing finished async collectors, and try to wake up blocked main
						// thread or checkpoint thread repeatedly.
						processError();
						Thread.sleep(1000);
					}
					else {
						while (nothingToDo())
							taskDone.await();

						if (mode == AsyncDataStream.OutputMode.ORDERED) {
							orderedProcess();
						}
						else {
							unorderedProcess();
						}
					}
				}
				catch (InterruptedException e) {
					LOG.info("Emitter Thread has been interrupted");
					running = false;
				}
				finally {
					lock.unlock();
				}
			}
		}

		public void stop() {
			running = false;
		}
	}
}
