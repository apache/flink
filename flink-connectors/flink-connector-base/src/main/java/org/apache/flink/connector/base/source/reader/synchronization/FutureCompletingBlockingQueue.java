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

package org.apache.flink.connector.base.source.reader.synchronization;

import java.util.Collection;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * A BlockingQueue that allows a consuming thread to be notified asynchronously on element
 * availability when the queue is empty.
 *
 * <p>Implementation wise, it is a subclass of {@link LinkedBlockingQueue} that ensures all
 * the methods adding elements into the queue will complete the elements availability future.
 *
 * <p>The overriding methods must first put the elements into the queue then check and complete
 * the future if needed. This is required to ensure the thread waiting for more messages will
 * not lose a notification.
 *
 * @param <T> the type of the elements in the queue.
 */
public class FutureCompletingBlockingQueue<T> extends LinkedBlockingQueue<T> {
	private final FutureNotifier futureNotifier;

	public FutureCompletingBlockingQueue(FutureNotifier futureNotifier) {
		this.futureNotifier = futureNotifier;
	}

	@Override
	public void put(T t) throws InterruptedException {
		super.put(t);
		futureNotifier.notifyComplete();
	}

	@Override
	public boolean offer(T t, long timeout, TimeUnit unit) throws InterruptedException {
		if (super.offer(t, timeout, unit)) {
			futureNotifier.notifyComplete();
			return true;
		} else {
			return false;
		}
	}

	@Override
	public boolean offer(T t) {
		if (super.offer(t)) {
			futureNotifier.notifyComplete();
			return true;
		} else {
			return false;
		}
	}

	@Override
	public boolean add(T t) {
		if (super.add(t)) {
			futureNotifier.notifyComplete();
			return true;
		} else {
			return false;
		}
	}

	@Override
	public boolean addAll(Collection<? extends T> c) {
		if (super.addAll(c)) {
			futureNotifier.notifyComplete();
			return true;
		} else {
			return false;
		}
	}
}
