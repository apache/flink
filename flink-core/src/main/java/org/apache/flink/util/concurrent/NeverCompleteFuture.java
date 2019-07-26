/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.util.concurrent;

import org.apache.flink.annotation.Internal;

import javax.annotation.Nonnull;

import java.util.concurrent.CancellationException;
import java.util.concurrent.Delayed;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A future that never completes.
 */
@Internal
public final class NeverCompleteFuture implements ScheduledFuture<Object> {

	private final Object lock = new Object();

	private final long delayMillis;

	private volatile boolean canceled;

	public NeverCompleteFuture(long delayMillis) {
		this.delayMillis = delayMillis;
	}

	@Override
	public long getDelay(@Nonnull TimeUnit unit) {
		return unit.convert(delayMillis, TimeUnit.MILLISECONDS);
	}

	@Override
	public int compareTo(@Nonnull Delayed o) {
		long otherMillis = o.getDelay(TimeUnit.MILLISECONDS);
		return Long.compare(this.delayMillis, otherMillis);
	}

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		synchronized (lock) {
			canceled = true;
			lock.notifyAll();
		}
		return true;
	}

	@Override
	public boolean isCancelled() {
		return canceled;
	}

	@Override
	public boolean isDone() {
		return false;
	}

	@Override
	public Object get() throws InterruptedException {
		synchronized (lock) {
			while (!canceled) {
				lock.wait();
			}
		}
		throw new CancellationException();
	}

	@Override
	public Object get(long timeout, @Nonnull TimeUnit unit) throws InterruptedException, TimeoutException {
		synchronized (lock) {
			while (!canceled) {
				unit.timedWait(lock, timeout);
			}

			if (canceled) {
				throw new CancellationException();
			} else {
				throw new TimeoutException();
			}
		}
	}
}
