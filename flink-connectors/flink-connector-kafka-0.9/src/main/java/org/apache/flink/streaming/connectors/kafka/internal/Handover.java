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

package org.apache.flink.streaming.connectors.kafka.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.ExceptionUtils;

import org.apache.kafka.clients.consumer.ConsumerRecords;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import java.io.Closeable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The Handover is a utility to hand over data (a buffer of records) and exception from a
 * <i>producer</i> thread to a <i>consumer</i> thread. It effectively behaves like a
 * "size one blocking queue", with some extras around exception reporting, closing, and
 * waking up thread without {@link Thread#interrupt() interrupting} threads.
 *
 * <p>This class is used in the Flink Kafka Consumer to hand over data and exceptions between
 * the thread that runs the KafkaConsumer class and the main thread.
 *
 * <p>The Handover has the notion of "waking up" the producer thread with a {@link WakeupException}
 * rather than a thread interrupt.
 *
 * <p>The Handover can also be "closed", signalling from one thread to the other that it
 * the thread has terminated.
 */
@ThreadSafe
@Internal
public final class Handover implements Closeable {

	private final Object lock = new Object();

	private ConsumerRecords<byte[], byte[]> next;
	private Throwable error;
	private boolean wakeupProducer;

	/**
	 * Polls the next element from the Handover, possibly blocking until the next element is
	 * available. This method behaves similar to polling from a blocking queue.
	 *
	 * <p>If an exception was handed in by the producer ({@link #reportError(Throwable)}), then
	 * that exception is thrown rather than an element being returned.
	 *
	 * @return The next element (buffer of records, never null).
	 *
	 * @throws ClosedException Thrown if the Handover was {@link #close() closed}.
	 * @throws Exception Rethrows exceptions from the {@link #reportError(Throwable)} method.
	 */
	@Nonnull
	public ConsumerRecords<byte[], byte[]> pollNext() throws Exception {
		synchronized (lock) {
			while (next == null && error == null) {
				lock.wait();
			}

			ConsumerRecords<byte[], byte[]> n = next;
			if (n != null) {
				next = null;
				lock.notifyAll();
				return n;
			}
			else {
				ExceptionUtils.rethrowException(error, error.getMessage());

				// this statement cannot be reached since the above method always throws an exception
				// this is only here to silence the compiler and any warnings
				return ConsumerRecords.empty();
			}
		}
	}

	/**
	 * Hands over an element from the producer. If the Handover already has an element that was
	 * not yet picked up by the consumer thread, this call blocks until the consumer picks up that
	 * previous element.
	 *
	 * <p>This behavior is similar to a "size one" blocking queue.
	 *
	 * @param element The next element to hand over.
	 *
	 * @throws InterruptedException
	 *                 Thrown, if the thread is interrupted while blocking for the Handover to be empty.
	 * @throws WakeupException
	 *                 Thrown, if the {@link #wakeupProducer()} method is called while blocking for
	 *                 the Handover to be empty.
	 * @throws ClosedException
	 *                 Thrown if the Handover was closed or concurrently being closed.
	 */
	public void produce(final ConsumerRecords<byte[], byte[]> element)
			throws InterruptedException, WakeupException, ClosedException {

		checkNotNull(element);

		synchronized (lock) {
			while (next != null && !wakeupProducer) {
				lock.wait();
			}

			wakeupProducer = false;

			// if there is still an element, we must have been woken up
			if (next != null) {
				throw new WakeupException();
			}
			// if there is no error, then this is open and can accept this element
			else if (error == null) {
				next = element;
				lock.notifyAll();
			}
			// an error marks this as closed for the producer
			else {
				throw new ClosedException();
			}
		}
	}

	/**
	 * Reports an exception. The consumer will throw the given exception immediately, if
	 * it is currently blocked in the {@link #pollNext()} method, or the next time it
	 * calls that method.
	 *
	 * <p>After this method has been called, no call to either {@link #produce(ConsumerRecords)}
	 * or {@link #pollNext()} will ever return regularly any more, but will always return
	 * exceptionally.
	 *
	 * <p>If another exception was already reported, this method does nothing.
	 *
	 * <p>For the producer, the Handover will appear as if it was {@link #close() closed}.
	 *
	 * @param t The exception to report.
	 */
	public void reportError(Throwable t) {
		checkNotNull(t);

		synchronized (lock) {
			// do not override the initial exception
			if (error == null) {
				error = t;
			}
			next = null;
			lock.notifyAll();
		}
	}

	/**
	 * Closes the handover. Both the {@link #produce(ConsumerRecords)} method and the
	 * {@link #pollNext()} will throw a {@link ClosedException} on any currently blocking and
	 * future invocations.
	 *
	 * <p>If an exception was previously reported via the {@link #reportError(Throwable)} method,
	 * that exception will not be overridden. The consumer thread will throw that exception upon
	 * calling {@link #pollNext()}, rather than the {@code ClosedException}.
	 */
	@Override
	public void close() {
		synchronized (lock) {
			next = null;
			wakeupProducer = false;

			if (error == null) {
				error = new ClosedException();
			}
			lock.notifyAll();
		}
	}

	/**
	 * Wakes the producer thread up. If the producer thread is currently blocked in
	 * the {@link #produce(ConsumerRecords)} method, it will exit the method throwing
	 * a {@link WakeupException}.
	 */
	public void wakeupProducer() {
		synchronized (lock) {
			wakeupProducer = true;
			lock.notifyAll();
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * An exception thrown by the Handover in the {@link #pollNext()} or
	 * {@link #produce(ConsumerRecords)} method, after the Handover was closed via
	 * {@link #close()}.
	 */
	public static final class ClosedException extends Exception {
		private static final long serialVersionUID = 1L;
	}

	/**
	 * A special exception thrown bv the Handover in the {@link #produce(ConsumerRecords)}
	 * method when the producer is woken up from a blocking call via {@link #wakeupProducer()}.
	 */
	public static final class WakeupException extends Exception {
		private static final long serialVersionUID = 1L;
	}
}
