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
 * Atomic reference counter, which enters a "disposed" state after the reference
 * count reaches 0.
 */
public class AtomicDisposableReferenceCounter {

	private final Object lock = new Object();

	private int referenceCounter;

	private boolean isDisposed;

	/**
	 * Increments the reference count and returns whether it was successful.
	 * <p>
	 * If the method returns <code>false</code>, the counter has already been
	 * disposed. Otherwise it returns <code>true</code>.
	 */
	public boolean incrementReferenceCounter() {
		synchronized (lock) {
			if (isDisposed) {
				return false;
			}

			referenceCounter++;
			return true;
		}
	}

	/**
	 * Decrements the reference count.
	 * <p>
	 * If the method returns <code>true</code>, the decrement operation disposed
	 * the counter. Otherwise it returns <code>false</code>.
	 */
	public boolean decrementReferenceCounter() {
		synchronized (lock) {
			if (isDisposed) {
				return false;
			}

			referenceCounter--;

			if (referenceCounter == 0) {
				isDisposed = true;
			}

			return isDisposed;
		}
	}
}
