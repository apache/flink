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

package org.apache.flink.core.testutils;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Latch for synchronizing parts of code in tests. Once the latch has fired once calls to
 * {@link #await()} will return immediately in the future.
 *
 * <p>A part of the code that should only run after other code calls {@link #await()}. The call
 * will only return once the other part is finished and calls {@link #trigger()}.
 */
public final class OneShotLatch {

	private final Object lock = new Object();

	private volatile boolean triggered;

	/**
	 * Fires the latch. Code that is blocked on {@link #await()} will now return.
	 */
	public void trigger() {
		synchronized (lock) {
			triggered = true;
			lock.notifyAll();
		}
	}

	/**
	 * Waits until {@link OneShotLatch#trigger()} is called. Once {@code trigger()} has been called this
	 * call will always return immediately.
	 *
	 * @throws InterruptedException Thrown if the thread is interrupted while waiting.
	 */
	public void await() throws InterruptedException {
		synchronized (lock) {
			while (!triggered) {
				lock.wait();
			}
		}
	}

	/**
	 * Waits until {@link OneShotLatch#trigger()} is called. Once {@code #trigger()} has been called this
	 * call will always return immediately.
	 *
	 * <p>If the latch is not triggered within the given timeout, a {@code TimeoutException}
	 * will be thrown after the timeout.
	 *
	 * <p>A timeout value of zero means infinite timeout and make this equivalent to {@link #await()}.
	 *
	 * @param timeout   The value of the timeout, a value of zero indicating infinite timeout.
	 * @param timeUnit  The unit of the timeout
	 *
	 * @throws InterruptedException Thrown if the thread is interrupted while waiting.
	 * @throws TimeoutException Thrown, if the latch is not triggered within the timeout time.
	 */
	public void await(long timeout, TimeUnit timeUnit) throws InterruptedException, TimeoutException {
		if (timeout < 0) {
			throw new IllegalArgumentException("time may not be negative");
		}
		if (timeUnit == null) {
			throw new NullPointerException("timeUnit");
		}

		if (timeout == 0) {
			await();
		} else {
			final long deadline = System.nanoTime() + timeUnit.toNanos(timeout);
			long millisToWait;

			synchronized (lock) {
				while (!triggered && (millisToWait = (deadline - System.nanoTime()) / 1_000_000) > 0) {
					lock.wait(millisToWait);
				}

				if (!triggered) {
					throw new TimeoutException();
				}
			}
		}
	}

	/**
	 * Checks if the latch was triggered.
	 *
	 * @return True, if the latch was triggered, false if not.
	 */
	public boolean isTriggered() {
		return triggered;
	}

	/**
	 * Resets the latch so that {@link #isTriggered()} returns false.
	 */
	public void reset() {
		synchronized (lock) {
			triggered = false;
		}
	}

	@Override
	public String toString() {
		return "Latch " + (triggered ? "TRIGGERED" : "PENDING");
	}
}
