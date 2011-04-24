/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

/**
 * This file is based on source code from the Hadoop Project (http://hadoop.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. 
 */

package eu.stratosphere.pact.runtime.sort;

import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This class implements a priority-queue, which maintains a partial
 * ordering of its elements such that the+ least element can always be found
 * in constant time. Put()'s and pop()'s require log(size) time.
 * 
 * @author Erik Nijkamp
 */
public class BlockingPartialOrderPriorityQueue<T> extends AbstractQueue<T> implements BlockingQueue<T> {
	// Backing queue.
	private final PartialOrderPriorityQueue<T> queue;

	// Main lock guarding all access
	private final ReentrantLock lock = new ReentrantLock(true);

	// Condition for waiting takes
	private final Condition notEmpty = lock.newCondition();

	public BlockingPartialOrderPriorityQueue(Comparator<T> comparator, int capacity) {
		this.queue = new PartialOrderPriorityQueue<T>(comparator, capacity);
	}

	@Override
	public int size() {
		lock.lock();
		try {
			return queue.size();
		} finally {
			lock.unlock();
		}

	}

	@Override
	public int drainTo(Collection<? super T> c) {
		if (c == null)
			throw new NullPointerException();
		if (c == this)
			throw new IllegalArgumentException();
		lock.lock();
		try {
			int n = 0;
			T t;
			while ((t = queue.poll()) != null) {
				c.add(t);
				++n;
			}
			return n;
		} finally {
			lock.unlock();
		}

	}

	@Override
	public int drainTo(Collection<? super T> c, int maxElements) {
		if (c == null)
			throw new NullPointerException();
		if (c == this)
			throw new IllegalArgumentException();
		if (maxElements <= 0)
			return 0;
		lock.lock();
		try {
			int n = 0;
			T t;
			while (n < maxElements && (t = queue.poll()) != null) {
				c.add(t);
				++n;
			}
			return n;
		} finally {
			lock.unlock();
		}

	}

	@Override
	public boolean offer(T t) {
		lock.lock();
		try {
			boolean ok = queue.offer(t);
			assert ok;
			notEmpty.signal();
			return true;
		} finally {
			lock.unlock();
		}

	}

	@Override
	public boolean offer(T t, long timeout, TimeUnit unit) throws InterruptedException {
		return offer(t);
	}

	@Override
	public void put(T t) throws InterruptedException {
		offer(t);
	}

	@Override
	public int remainingCapacity() {
		lock.lock();
		try {
			return queue.remainingCapacity();
		} finally {
			lock.unlock();
		}
	}

	@Override
	public T take() throws InterruptedException {
		lock.lockInterruptibly();
		try {
			try {
				while (queue.size() == 0)
					notEmpty.await();
			} catch (InterruptedException ie) {
				notEmpty.signal(); // propagate to non-interrupted thread
				throw ie;
			}
			T t = queue.poll();
			assert t != null;
			return t;
		} finally {
			lock.unlock();
		}

	}

	@Override
	public T peek() {
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
			return queue.peek();
		} finally {
			lock.unlock();
		}

	}

	@Override
	public T poll() {
		lock.lock();
		try {
			return queue.poll();
		} finally {
			lock.unlock();
		}

	}

	@Override
	public T poll(long timeout, TimeUnit unit) throws InterruptedException {
		long nanos = unit.toNanos(timeout);
		lock.lockInterruptibly();
		try {
			while (true) {
				T t = queue.poll();
				if (t != null)
					return t;
				if (nanos <= 0)
					return null;
				try {
					nanos = notEmpty.awaitNanos(nanos);
				} catch (InterruptedException ie) {
					notEmpty.signal(); // propagate to non-interrupted thread
					throw ie;
				}
			}
		} finally {
			lock.unlock();
		}
	}

	@Override
	public Object[] toArray() {
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
			return queue.toArray();
		} finally {
			lock.unlock();
		}
	}

	@Override
	public Iterator<T> iterator() {
		lock.lock();
		try {
			return new BlockingIterator(toArray());
		} finally {
			lock.unlock();
		}
	}

	/**
	 * Snapshot iterator that works off copy of underlying queue array.
	 */
	private class BlockingIterator implements Iterator<T> {
		final Object[] array;

		int cursor;

		int lastRet;

		BlockingIterator(Object[] array) {
			lastRet = -1;
			this.array = array;
		}

		public boolean hasNext() {
			return cursor < array.length;
		}

		@SuppressWarnings("unchecked")
		public T next() {
			if (cursor >= array.length)
				throw new NoSuchElementException();
			lastRet = cursor;
			return (T) array[cursor++];
		}

		public void remove() {
			if (lastRet < 0)
				throw new IllegalStateException();
			Object x = array[lastRet];
			lastRet = -1;
			// Traverse underlying queue to find == element,
			// not just a .equals element.
			lock.lock();
			try {
				for (Iterator<T> it = queue.iterator(); it.hasNext();) {
					if (it.next() == x) {
						it.remove();
						return;
					}
				}
			} finally {
				lock.unlock();
			}
		}
	}

}
