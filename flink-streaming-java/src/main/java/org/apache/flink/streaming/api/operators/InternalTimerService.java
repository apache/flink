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
package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;

/**
 * Interface for working with time and timers.
 *
 * <p>This is the internal version of {@link TimerService} that allows to specify
 * a key and a namespace to which timers should be scoped.
 */
@Internal
public interface InternalTimerService<K, N> {

	/** Returns the current processing time. */
	long currentProcessingTime();

	/** Returns the current event time. */
	long currentEventTime();

	/**
	 * Registers a timer to be fired when processing time passes the given time.
	 *
	 * <p>The timer is scoped to the given key and namespace. This scope will also be active
	 * when you receive the timer notification.
	 */
	void registerProcessingTimeTimer(K key, N namespace, long time);

	/**
	 * Deletes the given timer for the given key and namespace.
	 */
	void deleteProcessingTimeTimer(K key, N namespace, long time);

	/**
	 * Registers a timer to be fired when processing time passes the given time.
	 *
	 * <p>The timer is scoped to the given key and namespace. This scope will also be active
	 * when you receive the timer notification.
	 */
	void registerEventTimeTimer(K key, N namespace, long time);

	/**
	 * Deletes the given timer for the given key and namespace.
	 */
	void deleteEventTimeTimer(K key, N namespace, long time);
}
