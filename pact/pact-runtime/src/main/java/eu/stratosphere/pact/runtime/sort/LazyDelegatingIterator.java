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

package eu.stratosphere.pact.runtime.sort;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * An iterator that delegates requests to another iterator (the target
 * iterator). The target iterator may is initially not set. Any request to this
 * iterator blocks until the target iterator is set.
 */
public class LazyDelegatingIterator<E> implements Iterator<E> {
	/**
	 * The lock used to block requests until the iterator to delegate requests
	 * to is available.
	 */
	private final ReentrantLock lock = new ReentrantLock(true);

	private final Condition isSet = lock.newCondition();

	/**
	 * The iterator to delegate requests to.
	 */
	private Iterator<E> backingIterator = null;

	/**
	 * The exception thrown by the iterator when an error occurs while it is
	 * waiting.
	 */
	private Exception exception;

	/**
	 * Creates a new LazyDelegatingIterator.
	 */
	public LazyDelegatingIterator() {
	}

	@Override
	public boolean hasNext() {
		if (backingIterator != null) {
			return backingIterator.hasNext();
		} else {
			// uninitialized
			lock.lock();
			try {
				while (backingIterator == null && exception == null) {
					try {
						isSet.await();
					} catch (InterruptedException iex) {
					}
				}
			} finally {
				lock.unlock();
			}

			// now we are initialized
			if (exception != null) {
				throw new RuntimeException("An error occured while waiting for the element.", exception);
			} else {
				return backingIterator.hasNext();
			}
		}
	}

	@Override
	public E next() {
		if (!hasNext()) {
			throw new NoSuchElementException();
		} else {
			return backingIterator.next();
		}
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

	/**
	 * Sets the target that this iterator delegates its requests to. If an
	 * exception has been set before, this call is ineffective.
	 * 
	 * @param iterator
	 *        The target iterator.
	 */
	public void setTarget(Iterator<E> iterator) {
		lock.lock();
		try {
			if (backingIterator != null) {
				throw new IllegalStateException("Iterator has already a target.");
			} else if (exception == null) {
				backingIterator = iterator;
				isSet.signalAll();
			}
		} finally {
			lock.unlock();
		}
	}

	/**
	 * Causes the blocking iterator calls to throw a runtime exception
	 * encapsulating the given exception. If any exception has been set before,
	 * this call does nothing.
	 * 
	 * @param ex
	 *        The exception to be thrown.
	 */
	public void setException(Exception ex) {
		lock.lock();
		try {
			if (exception == null) {
				exception = ex;
				isSet.signalAll();
			}
		} finally {
			lock.unlock();
		}
	}
}
