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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.ExceptionUtils;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import javax.annotation.Nonnull;
import java.io.Closeable;

import static org.apache.flink.util.Preconditions.checkNotNull;

public final class Handover implements Closeable {

	private final Object lock = new Object();

	private ConsumerRecords<byte[], byte[]> next;
	private Throwable error;
	private boolean wakeup;

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

	public void produce(final ConsumerRecords<byte[], byte[]> element)
			throws InterruptedException, WakeupException, ClosedException {

		checkNotNull(element);

		synchronized (lock) {
			while (next != null && !wakeup) {
				lock.wait();
			}

			wakeup = false;

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

	@Override
	public void close() {
		synchronized (lock) {
			next = null;
			wakeup = false;

			if (error == null) {
				error = new ClosedException();
			}
			lock.notifyAll();
		}
	}

	public void wakeupProducer() {
		synchronized (lock) {
			wakeup = true;
			lock.notifyAll();
		}
	}

	@VisibleForTesting
	Object getLock() {
		return lock;
	}

	// ------------------------------------------------------------------------

	public static final class ClosedException extends IllegalStateException {
		private static final long serialVersionUID = 1L;
	}

	public static final class WakeupException extends Exception {
		private static final long serialVersionUID = 1L;
	}
}
