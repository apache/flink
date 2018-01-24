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

package org.apache.flink.runtime.io.network.partition;

/**
 * Test implementation of {@link BufferAvailabilityListener}.
 */
class AwaitableBufferAvailablityListener implements BufferAvailabilityListener {

	private long numNotifications;

	@Override
	public void notifyDataAvailable() {
		++numNotifications;
	}

	public long getNumNotifications() {
		return numNotifications;
	}

	public void resetNotificationCounters() {
		numNotifications = 0;
	}

	void awaitNotifications(long awaitedNumNotifications, long timeoutMillis) throws InterruptedException {
		long deadline = System.currentTimeMillis() + timeoutMillis;
		while (numNotifications < awaitedNumNotifications && System.currentTimeMillis() < deadline) {
			Thread.sleep(1);
		}
	}
}
