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

package org.apache.flink.runtime.io.network.util;

import org.apache.flink.runtime.util.event.NotificationListener;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A mock notification listener.
 */
public class TestNotificationListener implements NotificationListener {

	final AtomicInteger numberOfNotifications = new AtomicInteger();

	@Override
	public void onNotification() {
		synchronized (numberOfNotifications) {
			numberOfNotifications.incrementAndGet();

			numberOfNotifications.notifyAll();
		}
	}

	/**
	 * Waits on a notification.
	 *
	 * <p> <strong>Important</strong>: It's necessary to get the current number of notifications
	 * <em>before</em> registering the listener. Otherwise the wait call may block indefinitely.
	 *
	 * <pre>
	 * MockNotificationListener listener = new MockNotificationListener();
	 *
	 * int current = listener.getNumberOfNotifications();
	 *
	 * // Register the listener
	 * register(listener);
	 *
	 * listener.waitForNotification(current);
	 * </pre>
	 */
	public void waitForNotification(int current) throws InterruptedException {
		synchronized (numberOfNotifications) {
			while (current == numberOfNotifications.get()) {
				numberOfNotifications.wait();
			}
		}
	}

	public int getNumberOfNotifications() {
		return numberOfNotifications.get();
	}

	public void reset() {
		numberOfNotifications.set(0);
	}
}