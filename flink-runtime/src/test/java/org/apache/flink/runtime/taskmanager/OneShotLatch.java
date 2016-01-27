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

package org.apache.flink.runtime.taskmanager;

/**
 * Latch for synchronizing parts of code in tests. Once the latch has fired once calls to
 * {@link #await()} will return immediately in the future.
 *
 * <p>
 * A part of the code that should only run after other code calls {@link #await()}. The call
 * will only return once the other part is finished and calls {@link #trigger()}.
 */
public final class OneShotLatch {
	
	private final Object lock = new Object();
	
	private boolean triggered;

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
	 * Waits until {@link #trigger())} is called. Once {@code trigger()} has been called this
	 * call will always return immediately.
	 */
	public void await() throws InterruptedException {
		synchronized (lock) {
			while (!triggered) {
				lock.wait();
			}
		}
	}
}
