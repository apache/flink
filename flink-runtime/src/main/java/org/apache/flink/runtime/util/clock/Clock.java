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

package org.apache.flink.runtime.util.clock;

/**
 * A clock that gives access to time. This clock returns two flavors of time:
 * 
 * <p><b>Absolute Time:</b> This refers to real world wall clock time, and it typically
 * derived from a system clock. It is subject to clock drift and inaccuracy, and can jump
 * if the system clock is adjusted.
 * 
 * <p><b>Relative Time:</b> This time advances at the same speed as the <i>absolute time</i>,
 * but the timestamps can only be referred to relative to each other. The timestamps have
 * no absolute meaning and cannot be compared across JVM processes. The source for the
 * timestamps is not affected by adjustments to the system clock, so it never jumps.
 */
public abstract class Clock {

	public abstract long absoluteTimeMillis();

	public abstract long relativeTimeMillis();

	public abstract long relativeTimeNanos();
}
