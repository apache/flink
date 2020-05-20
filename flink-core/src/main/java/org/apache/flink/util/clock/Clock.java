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

package org.apache.flink.util.clock;

import org.apache.flink.annotation.PublicEvolving;

/**
 * A clock that gives access to time. This clock returns two flavors of time:
 *
 * <h3>Absolute Time</h3>
 *
 * <p>This refers to real world wall clock time, and it is typically
 * derived from a system clock. It is subject to clock drift and inaccuracy, and can jump if the
 * system clock is adjusted. Absolute time behaves similar to {@link System#currentTimeMillis()}.
 *
 * <h3>Relative Time</h3>
 *
 * <p>This time advances at the same speed as the <i>absolute time</i>,
 * but the timestamps can only be referred to relative to each other. The timestamps have no
 * absolute meaning and cannot be compared across JVM processes. The source for the timestamps is
 * not affected by adjustments to the system clock, so it never jumps. Relative time behaves similar
 * to {@link System#nanoTime()}.
 */
@PublicEvolving
public abstract class Clock {

	/**
	 * Gets the current absolute time, in milliseconds.
	 */
	public abstract long absoluteTimeMillis();

	/**
	 * Gets the current relative time, in milliseconds.
	 */
	public abstract long relativeTimeMillis();

	/**
	 * Gets the current relative time, in nanoseconds.
	 */
	public abstract long relativeTimeNanos();
}
