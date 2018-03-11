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
package org.apache.flink.api.common.time;

import org.apache.flink.annotation.Internal;

import java.time.Duration;

/**
 * This class stores a deadline, as obtained via {@link #now()} or from {@link #plus(Duration)}.
 */
@Internal
public class Deadline {
	private final long timeNanos;

	private Deadline(Duration time) {
		this.timeNanos = time.toNanos();
	}

	public Deadline plus(Duration other) {
		return new Deadline(Duration.ofNanos(timeNanos).plus(other));
	}

	/**
	 * Returns the time left between the deadline and now. The result is negative if the deadline
	 * has passed.
	 */
	public Duration timeLeft() {
		return Duration.ofNanos(timeNanos).minus(Duration.ofNanos(System.nanoTime()));
	}

	/**
	 * Returns whether there is any time left between the deadline and now.
	 */
	public boolean hasTimeLeft() {
		return !isOverdue();
	}

	/**
	 * Determines whether the deadline is in the past, i.e. whether the time left is negative.
	 */
	public boolean isOverdue() {
		return timeNanos - System.nanoTime() < 0;
	}

	/**
	 * Constructs a {@link Deadline} that has now as the deadline. Use this and then extend via
	 * {@link #plus(Duration)} to specify a deadline in the future.
	 */
	public static Deadline now() {
		return new Deadline(Duration.ofNanos(System.nanoTime()));
	}
}
