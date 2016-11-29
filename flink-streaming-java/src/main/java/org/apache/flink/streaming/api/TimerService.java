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
package org.apache.flink.streaming.api;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Interface for working with time and timers.
 */
@PublicEvolving
public interface TimerService {

	/** Returns the current processing time. */
	long currentProcessingTime();

	/** Returns the current event-time watermark. */
	long currentWatermark();

	/**
	 * Registers a timer to be fired when processing time passes the given time.
	 *
	 * <p>Timers can internally be scoped to keys and/or windows. When you set a timer
	 * in a keyed context, such as in an operation on
	 * {@link org.apache.flink.streaming.api.datastream.KeyedStream} then that context
	 * will also be active when you receive the timer notification.
	 */
	void registerProcessingTimeTimer(long time);

	/**
	 * Registers a timer to be fired when the event time watermark passes the given time.
	 *
	 * <p>Timers can internally be scoped to keys and/or windows. When you set a timer
	 * in a keyed context, such as in an operation on
	 * {@link org.apache.flink.streaming.api.datastream.KeyedStream} then that context
	 * will also be active when you receive the timer notification.
	 */
	void registerEventTimeTimer(long time);
}
