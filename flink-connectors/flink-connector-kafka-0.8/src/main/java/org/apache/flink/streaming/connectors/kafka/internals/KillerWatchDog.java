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

package org.apache.flink.streaming.connectors.kafka.internals;

import org.apache.flink.annotation.Internal;

/**
 * A watch dog thread that forcibly kills another thread, if that thread does not
 * finish in time.
 *
 * <p>This uses the discouraged {@link Thread#stop()} method. While this is not
 * advisable, this watch dog is only for extreme cases of thread that simply
 * to not terminate otherwise.
 */
@Internal
class KillerWatchDog extends Thread {

	private final Thread toKill;
	private final long timeout;

	KillerWatchDog(Thread toKill, long timeout) {
		super("KillerWatchDog");
		setDaemon(true);

		this.toKill = toKill;
		this.timeout = timeout;
	}

	@SuppressWarnings("deprecation")
	@Override
	public void run() {
		final long deadline = System.nanoTime() / 1_000_000L + timeout;
		long now;

		while (toKill.isAlive() && (now = (System.nanoTime() / 1_000_000L)) < deadline) {
			try {
				toKill.join(deadline - now);
			}
			catch (InterruptedException e) {
				// ignore here, our job is important!
			}
		}

		// this is harsh, but this watchdog is a last resort
		if (toKill.isAlive()) {
			toKill.stop();
		}
	}
}
