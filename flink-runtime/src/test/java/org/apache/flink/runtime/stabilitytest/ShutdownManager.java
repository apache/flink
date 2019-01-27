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

package org.apache.flink.runtime.stabilitytest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import static org.apache.flink.runtime.stabilitytest.Constants.LOG_PREFIX_DEFAULT;

/**
 * Auxiliary class for stability test, it is responsible for the release of resources at JVM's exit.
 */
public final class ShutdownManager {
	private static final Logger LOG = LoggerFactory.getLogger(ShutdownManager.class);

	private static final ConcurrentHashMap<AutoCloseable, Boolean> closeableMap = new ConcurrentHashMap<AutoCloseable, Boolean>();

	private static AtomicBoolean isShutdowning = new AtomicBoolean(false);
	private static AtomicInteger changingCounter = new AtomicInteger(0);

	/**
	 * Add JVM shutdown hook to clear all resources of stability test.
	 */
	static {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				isShutdowning.set(true);
				while (changingCounter.get() > 0) {}

				closeableMap.forEach(new BiConsumer<AutoCloseable, Boolean>() {
					@Override
					public void accept(AutoCloseable closeable, Boolean aBoolean) {
						try {
							closeable.close();
						} catch (Throwable t) {
							LOG.warn(LOG_PREFIX_DEFAULT + " Close failed and the AutoCloseable is " + closeable.getClass().getCanonicalName(), t);
						}
					}
				});

				closeableMap.clear();
			}
		});
	}

	public static void register(AutoCloseable closeable) {
		// Increment registering counter
		changingCounter.incrementAndGet();

		try {
			if (!isShutdowning.get()) {
				closeableMap.putIfAbsent(closeable, true);
			}
		} finally {
			// Decrement registering counter
			changingCounter.decrementAndGet();
		}
	}
}
