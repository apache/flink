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

package org.apache.flink.runtime.util;

/**
 * Atomic reference counter, which enters a "disposed" state after it reaches a configurable
 * reference count (default 0).
 */
public class AtomicDisposableReferenceCounter {

	private final Object lock = new Object();

	private int referenceCount;

	private boolean isDisposed;

	/** Enter the disposed state when the reference count reaches this number. */
	private final int disposeOnReferenceCount;

	public AtomicDisposableReferenceCounter() {
		this.disposeOnReferenceCount = 0;
	}

	public AtomicDisposableReferenceCounter(int disposeOnReferenceCount) {
		this.disposeOnReferenceCount = disposeOnReferenceCount;
	}

	/**
	 * Increments the reference count and returns whether it was successful.
	 * <p>
	 * If the method returns <code>false</code>, the counter has already been disposed. Otherwise it
	 * returns <code>true</code>.
	 */
	public boolean increment() {
		synchronized (lock) {
			if (isDisposed) {
				return false;
			}

			referenceCount++;
			return true;
		}
	}

	/**
	 * Decrements the reference count and returns whether the reference counter entered the disposed
	 * state.
	 * <p>
	 * If the method returns <code>true</code>, the decrement operation disposed the counter.
	 * Otherwise it returns <code>false</code>.
	 */
	public boolean decrement() {
		synchronized (lock) {
			if (isDisposed) {
				return false;
			}

			referenceCount--;

			if (referenceCount <= disposeOnReferenceCount) {
				isDisposed = true;
			}

			return isDisposed;
		}
	}

	public int get() {
		synchronized (lock) {
			return referenceCount;
		}
	}

	/**
	 * Returns whether the reference count has reached the disposed state.
	 */
	public boolean isDisposed() {
		synchronized (lock) {
			return isDisposed;
		}
	}

	public boolean disposeIfNotUsed() {
		synchronized (lock) {
			if (referenceCount <= disposeOnReferenceCount) {
				isDisposed = true;
			}

			return isDisposed;
		}
	}
}